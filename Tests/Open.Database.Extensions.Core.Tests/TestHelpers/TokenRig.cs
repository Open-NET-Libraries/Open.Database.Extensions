using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>The steps of one call that take a cancellation token.</summary>
public enum TokenStage
{
	/// <summary>Opening the connection.</summary>
	Open,
	/// <summary>Looking up the parameter names of a stored procedure.</summary>
	Lookup,
	/// <summary>Executing the command.</summary>
	Execute,
	/// <summary>Reading the first row (<c>ReadAsync</c>, so the command must use async reads).</summary>
	Read,
}

/// <summary>
/// Records the token each step of a call was given, and can make one step wait for its token to be canceled
/// (what a slow server does), so a test shows which token a slow step can be canceled with.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class TokenRig
{
	public List<CancellationToken> Open { get; } = [];
	public List<CancellationToken> Lookup { get; } = [];
	public List<CancellationToken> Execute { get; } = [];
	public List<CancellationToken> Read { get; } = [];

	/// <summary>The token each handler given to a <c>ReadAsync</c> terminal received.</summary>
	public List<CancellationToken> Handler { get; } = [];

	/// <summary>What was done to the connection and the command, in order.</summary>
	public List<string> Log { get; } = [];

	/// <summary>Turn off to record nothing (the allocation tests must not measure the recording).</summary>
	public bool Recording { get; set; } = true;

	/// <summary>How many rows the reader serves.</summary>
	public int Rows { get; set; } = 1;

	/// <summary>The step that waits, without end, for its token to be canceled; <see langword="null"/> for none.</summary>
	public TokenStage? Stall { get; set; }

	/// <summary>Completes when the stalled step has started waiting.</summary>
	public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>Thrown by the execute, when set.</summary>
	public Exception? ExecuteFailure { get; set; }

	/// <summary>Called with the number of the row about to be read (1 for the first), by both <c>Read</c> and <c>ReadAsync</c>.</summary>
	public Action<int>? BeforeRead { get; set; }

	public void Record(string what)
	{
		if (Recording) Log.Add(what);
	}

	public void Record(List<CancellationToken> tokens, CancellationToken token)
	{
		if (Recording) tokens.Add(token);
	}

	/// <summary>Waits until <paramref name="token"/> is canceled when <paramref name="stage"/> is the stalled one; otherwise completes at once.</summary>
	public Task StallAsync(TokenStage stage, CancellationToken token)
	{
		if (Stall != stage) return Task.CompletedTask;
		Entered.TrySetResult();
		return Task.Delay(Timeout.Infinite, token);
	}
}

[ExcludeFromCodeCoverage]
internal sealed class TokenConnection(TokenRig rig, ConnectionState initial = ConnectionState.Closed) : DbConnection
{
	ConnectionState _state = initial;

	[AllowNull]
	public override string ConnectionString { get; set; } = "";
	public override string Database => "fake";
	public override string DataSource => "fake";
	public override string ServerVersion => "1.0";
	public override ConnectionState State => _state;
	public override void ChangeDatabase(string databaseName) { }

	public override void Open()
	{
		rig.Record("open-sync");
		_state = ConnectionState.Open;
	}

	public override async Task OpenAsync(CancellationToken cancellationToken)
	{
		rig.Record("open");
		rig.Record(rig.Open, cancellationToken);
		cancellationToken.ThrowIfCancellationRequested();
		await rig.StallAsync(TokenStage.Open, cancellationToken).ConfigureAwait(false);
		_state = ConnectionState.Open;
	}

	public override void Close()
	{
		rig.Record("close");
		_state = ConnectionState.Closed;
	}

	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

	protected override DbCommand CreateDbCommand() => new TokenCommand(rig, this);
}

[ExcludeFromCodeCoverage]
internal sealed class TokenCommand(TokenRig rig, DbConnection connection) : DbCommand
{
	[AllowNull]
	public override string CommandText { get; set; } = "";
	public override int CommandTimeout { get; set; }
	public override CommandType CommandType { get; set; }
	public override bool DesignTimeVisible { get; set; }
	public override UpdateRowSource UpdatedRowSource { get; set; }
	protected override DbConnection? DbConnection { get; set; } = connection;
	protected override DbParameterCollection DbParameterCollection { get; } = new ParameterList();
	protected override DbTransaction? DbTransaction { get; set; }

	public override void Cancel() { }
	public override int ExecuteNonQuery() => 1;
	public override object? ExecuteScalar() => null;
	public override void Prepare() { }
	protected override DbParameter CreateDbParameter() => new Parameter();

	// The tests under way are about the asynchronous path: a synchronous execute would be the library taking the wrong one.
	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

	protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
	{
		rig.Record("execute");
		rig.Record(rig.Execute, cancellationToken);
		cancellationToken.ThrowIfCancellationRequested();
		if (rig.ExecuteFailure is { } failure) throw failure;
		return rig.Stall == TokenStage.Execute
			? StallThenReader(cancellationToken)
			: Task.FromResult<DbDataReader>(new TokenReader(rig));
	}

	async Task<DbDataReader> StallThenReader(CancellationToken cancellationToken)
	{
		await rig.StallAsync(TokenStage.Execute, cancellationToken).ConfigureAwait(false);
		return new TokenReader(rig);
	}

	sealed class Parameter : DbParameter
	{
		public override DbType DbType { get; set; }
		public override ParameterDirection Direction { get; set; }
		public override bool IsNullable { get; set; }
		[AllowNull]
		public override string ParameterName { get; set; } = "";
		[AllowNull]
		public override string SourceColumn { get; set; } = "";
		public override object? Value { get; set; }
		public override bool SourceColumnNullMapping { get; set; }
		public override int Size { get; set; }
		public override void ResetDbType() { }
	}

	sealed class ParameterList : DbParameterCollection
	{
		readonly List<DbParameter> _list = [];

		public override int Count => _list.Count;
		public override object SyncRoot => _list;
		public override int Add(object value) { _list.Add((DbParameter)value); return _list.Count - 1; }
		public override void AddRange(Array values) { foreach (object v in values) Add(v); }
		public override void Clear() => _list.Clear();
		public override bool Contains(object value) => _list.Contains((DbParameter)value);
		public override bool Contains(string value) => _list.Exists(p => p.ParameterName == value);
		public override void CopyTo(Array array, int index) => throw new NotSupportedException();
		public override System.Collections.IEnumerator GetEnumerator() => _list.GetEnumerator();
		public override int IndexOf(object value) => _list.IndexOf((DbParameter)value);
		public override int IndexOf(string parameterName) => _list.FindIndex(p => p.ParameterName == parameterName);
		public override void Insert(int index, object value) => _list.Insert(index, (DbParameter)value);
		public override void Remove(object value) => _list.Remove((DbParameter)value);
		public override void RemoveAt(int index) => _list.RemoveAt(index);
		public override void RemoveAt(string parameterName) => _list.RemoveAt(IndexOf(parameterName));
		protected override DbParameter GetParameter(int index) => _list[index];
		protected override DbParameter GetParameter(string parameterName) => _list[IndexOf(parameterName)];
		protected override void SetParameter(int index, DbParameter value) => _list[index] = value;
		protected override void SetParameter(string parameterName, DbParameter value) => _list[IndexOf(parameterName)] = value;
	}
}

[ExcludeFromCodeCoverage]
internal sealed class TokenReader(TokenRig rig) : TypedReader([("Id", typeof(int))], Rows(rig.Rows))
{
	int _reads;

	static object?[][] Rows(int count) => [.. Enumerable.Range(1, count).Select(i => (object?[])[i])];

	public override bool Read()
	{
		rig.BeforeRead?.Invoke(++_reads);
		return base.Read();
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		rig.Record(rig.Read, cancellationToken);
		return rig.Stall == TokenStage.Read ? StallThenRead(cancellationToken) : base.ReadAsync(cancellationToken);
	}

	async Task<bool> StallThenRead(CancellationToken cancellationToken)
	{
		await rig.StallAsync(TokenStage.Read, cancellationToken).ConfigureAwait(false);
		return await base.ReadAsync(cancellationToken).ConfigureAwait(false);
	}
}

/// <summary>A stored-procedure command whose provider "knows" the parameter <c>@id</c>, and records the token its lookup was given.</summary>
[ExcludeFromCodeCoverage]
internal sealed class TokenProcCommand : ExpressiveDbCommand
{
	readonly TokenRig _rig;

	public TokenProcCommand(TokenRig rig, DbConnection db, string name)
		: base(db, CommandType.StoredProcedure, name)
		=> _rig = rig;

	public TokenProcCommand(TokenRig rig, IDbConnectionFactory<DbConnection> factory, string name)
		: base(factory, CommandType.StoredProcedure, name)
		=> _rig = rig;

	protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
	{
		_rig.Record("lookup");
		_rig.Record(_rig.Lookup, cancellationToken);
		cancellationToken.ThrowIfCancellationRequested();
		return _rig.Stall == TokenStage.Lookup ? StallThenNames(cancellationToken) : new(["@id"]);
	}

	async ValueTask<ImmutableArray<string>> StallThenNames(CancellationToken cancellationToken)
	{
		await _rig.StallAsync(TokenStage.Lookup, cancellationToken).ConfigureAwait(false);
		return ["@id"];
	}
}
