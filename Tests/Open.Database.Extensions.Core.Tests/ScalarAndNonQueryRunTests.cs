using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// The run under DbQuery.Scalar<T>() and DbNonQuery, on a fake provider whose
// every open, close and dispose is counted: who the connection belongs to, from
// every source, also when the run or the conversion throws; the text, the kind,
// the timeout and the parameters that reach the command; and cancellation.
[ExcludeFromCodeCoverage]
public static class ScalarAndNonQueryRunTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	readonly record struct Values(int Id, string Name);

	const string Sql = "select @id, @name";

	static readonly DbNonQuery Write = DbNonQuery.Text(Sql);

	static readonly ScalarDbQuery<int> Read = DbQuery.Text(Sql).Scalar<int>();

	// The fake command answers 42: not a date, so the conversion throws.
	static readonly ScalarDbQuery<DateTime> BadRead = DbQuery.Text(Sql).Scalar<DateTime>();

	static BoundCall<TKind> Bind<TKind>(
		DbTally tally,
		LeakSource source,
		TrackedPool pool,
		Func<TrackedFactory, BoundCall<TKind>> factory,
		Func<TrackedPool, BoundCall<TKind>> pooled,
		Func<DbConnection, BoundCall<TKind>> connection,
		Func<DbTransaction, BoundCall<TKind>> transaction)
		=> source switch
		{
			LeakSource.Factory => factory(tally.Factory),
			LeakSource.Pool => pooled(pool),
			LeakSource.BareClosed => connection(tally.NewConnection()),
			LeakSource.BareOpen => connection(tally.NewOpenConnection()),
			_ => transaction(tally.NewOpenConnection().BeginTransaction()),
		};

	static BoundCall<NonQueryKind> Bind(DbTally tally, LeakSource source, TrackedPool pool, DbNonQuery query)
		=> Bind(tally, source, pool, f => query.Using(f), p => query.Using(p), c => query.Using(c), t => query.Using(t));

	static BoundCall<ScalarKind<T>> Bind<T>(DbTally tally, LeakSource source, TrackedPool pool, ScalarDbQuery<T> query)
		=> Bind(tally, source, pool, f => query.Using(f), p => query.Using(p), c => query.Using(c), t => query.Using(t));

	static void Released(DbTally tally, LeakSource source, TrackedPool pool)
	{
		LeakAssert.ConnectionsReleased(tally, source, pool, 1);
		LeakAssert.CommandsAndReadersDisposed(tally);
		Assert.Empty(tally.Readers);
		TrackedCommand command = Assert.Single(tally.Commands);
		if (source == LeakSource.BareOpenInTransaction)
			Assert.Same(Assert.Single(tally.Transactions), command.Transaction);
		else
			Assert.Null(command.Transaction);
	}

	public static TheoryData<LeakSource> Sources
		=> [.. Enum.GetValues<LeakSource>()];

	[Theory]
	[MemberData(nameof(Sources))]
	public static async Task AWrite_ReleasesWhatItTook(LeakSource source)
	{
		var tally = new DbTally();
		TrackedPool pool = tally.Pool;

		int rows = await Bind(tally, source, pool, Write).ExecuteNonQueryAsync(Token);

		Assert.Equal(1, rows);
		Released(tally, source, pool);
	}

	[Theory]
	[MemberData(nameof(Sources))]
	public static async Task AScalar_ReleasesWhatItTook(LeakSource source)
	{
		var tally = new DbTally();
		TrackedPool pool = tally.Pool;

		int value = await Bind(tally, source, pool, Read).ExecuteScalarAsync(Token);

		Assert.Equal(42, value);
		Released(tally, source, pool);
	}

	[Theory]
	[MemberData(nameof(Sources))]
	public static async Task AWriteThatThrows_ReleasesWhatItTook(LeakSource source)
	{
		var failure = new TimeoutException("The provider failed.");
		var tally = new DbTally(new() { ExecuteFailure = failure });
		TrackedPool pool = tally.Pool;
		BoundCall<NonQueryKind> call = Bind(tally, source, pool, Write);

		TimeoutException thrown = await Assert.ThrowsAsync<TimeoutException>(
			async () => await call.ExecuteNonQueryAsync(Token));

		Assert.Same(failure, thrown);
		Released(tally, source, pool);
	}

	[Theory]
	[MemberData(nameof(Sources))]
	public static async Task AScalarThatThrows_ReleasesWhatItTook(LeakSource source)
	{
		var failure = new TimeoutException("The provider failed.");
		var tally = new DbTally(new() { ExecuteFailure = failure });
		TrackedPool pool = tally.Pool;
		BoundCall<ScalarKind<int>> call = Bind(tally, source, pool, Read);

		TimeoutException thrown = await Assert.ThrowsAsync<TimeoutException>(
			async () => await call.ExecuteScalarAsync(Token));

		Assert.Same(failure, thrown);
		Released(tally, source, pool);
	}

	[Theory]
	[MemberData(nameof(Sources))]
	public static async Task AScalarWhoseConversionThrows_ReleasesWhatItTook(LeakSource source)
	{
		var tally = new DbTally();
		TrackedPool pool = tally.Pool;
		BoundCall<ScalarKind<DateTime>> call = Bind(tally, source, pool, BadRead);

		await Assert.ThrowsAsync<ValueMappingException>(
			async () => await call.ExecuteScalarAsync(Token));

		Released(tally, source, pool);
	}

	// ---- what reaches the command ----

	static string[] Sent(TrackedCommand command)
		=> [.. command.Parameters.Cast<DbParameter>().Select(static p => $"{p.ParameterName}={p.Value}")];

	[Fact]
	public static async Task TheTextKindAndParameters_AreThoseOfTheMappedChain()
	{
		var tally = new DbTally();
		TrackedConnection c = tally.NewOpenConnection();
		DbQuery query = DbQuery.Text(Sql);
		const int id = 7;
		string name = "x";

		// The mapped chain of the same definition, as the reference.
		_ = await query.Map<int>().Using(c).AddParam(id).AddParam(name).ToListAsync(Token);
		string[] expected = Sent(tally.Commands[^1]);
		Assert.Equal(["@id=7", "@name=x"], expected);

		DbNonQuery write = DbNonQuery.Text(Sql);
		ScalarDbQuery<int> scalar = query.Scalar<int>();
		Func<ValueTask<int>>[] runs =
		[
			() => write.Using(c).AddParam(id).AddParam(name).ExecuteNonQueryAsync(Token),
			() => write.AddParam(id).AddParam(name).Using(c).ExecuteNonQueryAsync(Token),
			() => write.Using(c).AddParams(new { id, name }).ExecuteNonQueryAsync(Token),
			() => write.AddParams(new { id, name }).Using(c).ExecuteNonQueryAsync(Token),
			() => write.Params<Values>().Using(c).AddParams(new(id, name)).ExecuteNonQueryAsync(Token),
			() => write.Params<Values>().AddParams(new(id, name)).Using(c).ExecuteNonQueryAsync(Token),
			() => scalar.Using(c).AddParam(id).AddParam(name).ExecuteScalarAsync(Token),
			() => scalar.AddParam(id).AddParam(name).Using(c).ExecuteScalarAsync(Token),
			() => scalar.Params<Values>().Using(c).AddParams(new(id, name)).ExecuteScalarAsync(Token),
			() => query.Params<Values>().Scalar<int>().AddParams(new(id, name)).Using(c).ExecuteScalarAsync(Token),
		];

		foreach (Func<ValueTask<int>> run in runs)
		{
			_ = await run();
			TrackedCommand command = tally.Commands[^1];
			Assert.Equal(expected, Sent(command));
			Assert.Equal(Sql, command.CommandText);
			Assert.Equal(CommandType.Text, command.CommandType);
		}

		Assert.Equal(ConnectionState.Open, c.State);
		Assert.Equal(0, c.Opens);
	}

	[Fact]
	public static async Task TheTimeout_ReachesTheCommand()
	{
		var tally = new DbTally();
		TrackedConnection c = tally.NewOpenConnection();
		DbNonQuery write = DbNonQuery.Text(Sql);
		DbQuery query = DbQuery.Text(Sql);

		async Task<int> Timeout(Func<ValueTask<int>> run)
		{
			_ = await run();
			return tally.Commands[^1].CommandTimeout;
		}

		// 0 is what the fake command has until it is set.
		Assert.Equal(0, await Timeout(() => write.Using(c).ExecuteNonQueryAsync(Token)));
		Assert.Equal(15, await Timeout(() => write.SetTimeout(15).Using(c).ExecuteNonQueryAsync(Token)));
		Assert.Equal(16, await Timeout(() => write.SetTimeout(16).AddParam("id", 1).Using(c).ExecuteNonQueryAsync(Token)));
		Assert.Equal(17, await Timeout(() => write.SetTimeout(17).Params<Values>().Using(c).AddParams(new(1, "a")).ExecuteNonQueryAsync(Token)));
		Assert.Equal(30, await Timeout(() => query.SetTimeout(30).Scalar<int>().Using(c).ExecuteScalarAsync(Token)));
		Assert.Equal(31, await Timeout(() => query.SetTimeout(31).Params<Values>().Scalar<int>().Using(c).AddParams(new(1, "a")).ExecuteScalarAsync(Token)));

		// The definition it was set from is not changed.
		Assert.Equal(0, await Timeout(() => write.Using(c).ExecuteNonQueryAsync(Token)));
		Assert.Equal(0, await Timeout(() => query.Scalar<int>().Using(c).ExecuteScalarAsync(Token)));
	}

	[Fact]
	public static async Task ATokenAlreadyCanceled_ThrowsBeforeAConnectionIsTaken()
	{
		var tally = new DbTally();
		var canceled = new CancellationToken(canceled: true);

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Write.Using(tally.Factory).ExecuteNonQueryAsync(canceled));
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Read.Using(tally.Factory).ExecuteScalarAsync(canceled));

		Assert.Empty(tally.Connections);
	}

	// ---- a provider that is truly asynchronous ----

	static readonly TimeSpan Bound5 = TimeSpan.FromSeconds(30);

	// A command that finishes only when the test says so, or when its token is canceled.
	sealed class Gate : DbConnection
	{
		ConnectionState _state = ConnectionState.Closed;

		public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public TaskCompletionSource<object?> Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public int Closes { get; private set; }

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "gate";
		public override string DataSource => "gate";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => _state;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() => _state = ConnectionState.Open;

		public override void Close()
		{
			if (_state == ConnectionState.Closed) return;

			Closes++;
			_state = ConnectionState.Closed;
		}

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
			=> throw new NotSupportedException();

		protected override DbCommand CreateDbCommand() => new Command(this);

		sealed class Command(Gate gate) : DbCommand
		{
			readonly TrackedParameterCollection _parameters = new();

			[AllowNull]
			public override string CommandText { get; set; } = "";
			public override int CommandTimeout { get; set; }
			public override CommandType CommandType { get; set; }
			public override bool DesignTimeVisible { get; set; }
			public override UpdateRowSource UpdatedRowSource { get; set; }
			protected override DbConnection? DbConnection { get; set; } = gate;
			protected override DbParameterCollection DbParameterCollection => _parameters;
			protected override DbTransaction? DbTransaction { get; set; }
			public override void Cancel() { }
			public override void Prepare() { }
			public override int ExecuteNonQuery() => throw new NotSupportedException();
			public override object ExecuteScalar() => throw new NotSupportedException();
			protected override DbParameter CreateDbParameter() => new TrackedParameter();
			protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
				=> throw new NotSupportedException();

			public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
				=> (int)(await Wait(cancellationToken))!;

			public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
				=> Wait(cancellationToken);

			Task<object?> Wait(CancellationToken cancellationToken)
			{
				gate.Started.SetResult();
				return gate.Finish.Task.WaitAsync(cancellationToken);
			}
		}
	}

	static async Task<T> Pending<T>(Gate gate, ValueTask<T> call, object result)
	{
		Task<T> task = call.AsTask();
		await gate.Started.Task.WaitAsync(Bound5, Token);
		Assert.False(task.IsCompleted);
		Assert.Equal(ConnectionState.Open, gate.State);

		gate.Finish.SetResult(result);
		return await task.WaitAsync(Bound5, Token);
	}

	[Fact]
	public static async Task ACallThatSuspends_ReleasesTheConnectionWhenItFinishes()
	{
		await using var writing = new Gate();
		Assert.Equal(3, await Pending(writing, Write.Using(writing).ExecuteNonQueryAsync(Token), 3));
		Assert.Equal(ConnectionState.Closed, writing.State);
		Assert.Equal(1, writing.Closes);

		await using var reading = new Gate();
		Assert.Equal(9, await Pending(reading, Read.Using(reading).ExecuteScalarAsync(Token), 9L));
		Assert.Equal(ConnectionState.Closed, reading.State);
		Assert.Equal(1, reading.Closes);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static async Task ATokenCanceledMidRun_ReachesTheCommand_AndTheConnectionIsReleased(bool scalar)
	{
		await using var gate = new Gate();
		using var source = CancellationTokenSource.CreateLinkedTokenSource(Token);
		Task task = scalar
			? Read.Using(gate).ExecuteScalarAsync(source.Token).AsTask()
			: Write.Using(gate).ExecuteNonQueryAsync(source.Token).AsTask();
		await gate.Started.Task.WaitAsync(Bound5, Token);

		await source.CancelAsync();
		OperationCanceledException canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => task.WaitAsync(Bound5, Token));

		Assert.Equal(source.Token, canceled.CancellationToken);
		Assert.Equal(ConnectionState.Closed, gate.State);
		Assert.Equal(1, gate.Closes);
	}
}
