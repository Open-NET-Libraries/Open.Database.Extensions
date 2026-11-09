using System.Collections;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// What the library itself allocates around a command, measured on a provider that allocates nothing of its own (<see cref="MemConnection"/>),
/// so the bytes are the library's. Like the neighbors in <see cref="AllocationQuickWinTests"/>, each call is compared with the cheapest hand-written equivalent measured in the same run.
/// </summary>
[ExcludeFromCodeCoverage]
public static class CommandAllocationTests
{
	const string Sql = "select 1";

	// What the hand-written version may cost beyond the measured ideal, as in AllocationQuickWinTests.
	const int Slack = 16;

	static CancellationToken Token => TestContext.Current.CancellationToken;

	// ---- a command made from a connection factory: no delegate to the factory's Create method for each command ----

	[Fact]
	public static void FactoryCommand_CarriesOnlyOnePoolHolder_BeyondTheSameCommandOnAConnection()
	{
		var connection = new MemConnection();
		IDbConnectionFactory<DbConnection> factory = new MemFactory(connection);

		Assert.Equal(1, factory.Command(Sql).ExecuteScalar<int>());
		Assert.Equal(1, connection.Command(Sql).ExecuteScalar<int>());

		long viaFactory = AllocationQuickWinTests.BytesPerCall(() => factory.Command(Sql).ExecuteScalar());
		long onConnection = AllocationQuickWinTests.BytesPerCall(() => connection.Command(Sql).ExecuteScalar());

		// The holder is an object with one reference: a header of two words and the field.
		long holder = 3 * IntPtr.Size;
		Assert.True(viaFactory <= onConnection + holder + Slack, $"{viaFactory} B per factory command, {onConnection} B for the same command on a connection, one holder is {holder} B.");
	}

	[Fact]
	public static void FactoryCommand_TakesANewConnectionAndDisposesItForEachExecution()
	{
		var connection = new MemConnection();
		IDbConnectionFactory<DbConnection> factory = new MemFactory(connection);

		factory.Command(Sql).ExecuteScalar();
		factory.Command(Sql).ExecuteScalar();

		Assert.Equal(2, connection.Creates);
		Assert.Equal(2, connection.Disposes);
	}

	// ---- the synchronous transaction helpers: the wrappers around the caller's function allocate no closure and no delegate ----

	static readonly Action<DbTransaction> DoNothing = static _ => { };
	static readonly Func<DbTransaction, int> Seven = static _ => 7;
	static readonly Func<DbTransaction, bool> Yes = static _ => true;

	[Fact]
	public static void ExecuteTransaction_Action_AllocatesNothingBeyondTheHandWrittenTransaction()
	{
		var connection = new MemConnection();
		connection.ExecuteTransaction(DoNothing, cancellationToken: Token);

		long actual = AllocationQuickWinTests.BytesPerCall(() => connection.ExecuteTransaction(DoNothing));
		long byHand = BytesPerHandWrittenTransaction(connection, DoNothing);

		Assert.True(actual <= byHand + Slack, $"{actual} B per call, the hand-written transaction is {byHand} B.");
	}

	[Fact]
	public static void ExecuteTransaction_Func_AllocatesNothingBeyondTheHandWrittenTransaction()
	{
		var connection = new MemConnection();
		Assert.Equal(7, connection.ExecuteTransaction(Seven, cancellationToken: Token));

		long actual = AllocationQuickWinTests.BytesPerCall(() => connection.ExecuteTransaction(Seven));
		long byHand = BytesPerHandWrittenTransaction(connection, DoNothing);

		Assert.True(actual <= byHand + Slack, $"{actual} B per call, the hand-written transaction is {byHand} B.");
	}

	[Fact]
	public static void ExecuteTransactionConditional_Func_AllocatesNothingBeyondTheHandWrittenTransaction()
	{
		var connection = new MemConnection();
		Assert.True(connection.ExecuteTransactionConditional(Yes, cancellationToken: Token));

		long actual = AllocationQuickWinTests.BytesPerCall(() => connection.ExecuteTransactionConditional(Yes));
		long byHand = BytesPerHandWrittenTransaction(connection, DoNothing);

		Assert.True(actual <= byHand + Slack, $"{actual} B per call, the hand-written transaction is {byHand} B.");
	}

	[Fact]
	public static void ExecuteTransaction_Wrappers_CommitOrRollBackAsBefore()
	{
		var connection = new MemConnection();

		connection.ExecuteTransaction(DoNothing, cancellationToken: Token);
		Assert.Equal((1, 0), (connection.Transaction.Commits, connection.Transaction.Rollbacks));

		Assert.Equal(7, connection.ExecuteTransaction(Seven, cancellationToken: Token));
		Assert.Equal((2, 0), (connection.Transaction.Commits, connection.Transaction.Rollbacks));

		Assert.True(connection.ExecuteTransactionConditional(Yes, cancellationToken: Token));
		Assert.Equal((3, 0), (connection.Transaction.Commits, connection.Transaction.Rollbacks));

		Assert.False(connection.ExecuteTransactionConditional(static _ => false, cancellationToken: Token));
		Assert.Equal((3, 1), (connection.Transaction.Commits, connection.Transaction.Rollbacks));

		Assert.Throws<InvalidOperationException>(() => connection.ExecuteTransaction(static _ => throw new InvalidOperationException(), cancellationToken: Token));
		Assert.Equal((3, 2), (connection.Transaction.Commits, connection.Transaction.Rollbacks));
	}

	[Fact]
	public static void ExecuteTransaction_Wrappers_ANullArgument_ThrowsArgumentNull()
	{
		var connection = new MemConnection();
		DbConnection? none = null;

		Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => connection.ExecuteTransaction((Action<DbTransaction>)null!, cancellationToken: Token)).ParamName);
		Assert.Equal("action", Assert.Throws<ArgumentNullException>(() => connection.ExecuteTransaction((Func<DbTransaction, int>)null!, cancellationToken: Token)).ParamName);
		Assert.Equal("conditionalAction", Assert.Throws<ArgumentNullException>(() => connection.ExecuteTransactionConditional((Func<DbTransaction, bool>)null!, cancellationToken: Token)).ParamName);
		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(() => none!.ExecuteTransaction(DoNothing, cancellationToken: Token)).ParamName);
		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(() => none!.ExecuteTransaction(Seven, cancellationToken: Token)).ParamName);
		Assert.Equal("connection", Assert.Throws<ArgumentNullException>(() => none!.ExecuteTransactionConditional(Yes, cancellationToken: Token)).ParamName);
	}

	// What a caller writes without the helper: open if needed, begin, run, commit, dispose, restore.
	static long BytesPerHandWrittenTransaction(DbConnection connection, Action<DbTransaction> action)
		=> AllocationQuickWinTests.BytesPerCall(() =>
		{
			ConnectionState prior = connection.EnsureOpen();
			try
			{
				using DbTransaction transaction = connection.BeginTransaction();
				action(transaction);
				transaction.Commit();
			}
			finally
			{
				if (prior != ConnectionState.Open) connection.Close();
			}
		});

	// ---- AddParams: the list is made at the size of the members, not at four slots of a large struct ----

	sealed record Three(string A, string B, string C);

	[Fact]
	public static void AddParams_ANewList_HasTheSizeOfTheMembers()
	{
		var connection = new MemConnection();
		var source = new Three("a", "b", "c");

		ExpressiveDbCommand typed = connection.Command(Sql).AddParams(source);
		ExpressiveDbCommand runtime = connection.Command(Sql).AddParams<object>(source);

		Assert.Equal(["A", "B", "C"], typed.Params.Select(p => p.Name));
		Assert.Equal(3, typed.Params.Capacity);
		Assert.Equal(["A", "B", "C"], runtime.Params.Select(p => p.Name));
		Assert.Equal(3, runtime.Params.Capacity);
	}

	[Fact]
	public static void AddParams_AllocatesLessThanTheSameParametersAddedOneByOne()
	{
		var connection = new MemConnection();
		var source = new Three("a", "b", "c");
		connection.Command(Sql).AddParams(source);

		long once = AllocationQuickWinTests.BytesPerCall(() => connection.Command(Sql).AddParams(source));
		long oneByOne = AllocationQuickWinTests.BytesPerCall(() => connection.Command(Sql).AddParam("A", "a").AddParam("B", "b").AddParam("C", "c"));

		// The list is the same except for its array: three slots instead of four. AddParams also makes the small block that remembers
		// the names are still to be resolved, which AddParam with a name does not (a slot of a parameter is 48 B on 64 bits, so the array is smaller by one).
		long slot = System.Runtime.CompilerServices.Unsafe.SizeOf<ExpressiveDbCommand.Param>();
		Assert.True(once <= oneByOne - slot + OptionsBlock(connection) + Slack, $"AddParams {once} B, three AddParam calls {oneByOne} B, one slot is {slot} B.");
	}

	// The cost of the block of rarely-set members that a command makes when one of them is set: measured by setting one.
	static long OptionsBlock(MemConnection connection)
	{
		long plain = AllocationQuickWinTests.BytesPerCall(() => connection.Command(Sql));
		long withOne = AllocationQuickWinTests.BytesPerCall(() => connection.Command(Sql).SetTimeout(5));
		return withOne - plain;
	}

	// ---- a provider that allocates nothing of its own ----

	sealed class MemFactory(MemConnection connection) : IDbConnectionFactory<DbConnection>
	{
		public DbConnection Create()
		{
			connection.Creates++;
			return connection;
		}

		IDbConnection IDbConnectionFactory.Create() => Create();
	}

	sealed class MemConnection : DbConnection
	{
		readonly MemCommand _command;

		public MemConnection()
		{
			_command = new(this);
			Transaction = new(this);
		}

		public MemTransaction Transaction { get; }
		public int Creates { get; set; }
		public int Disposes { get; private set; }

		ConnectionState _state = ConnectionState.Open;

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "mem";
		public override string DataSource => "mem";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => _state;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() => _state = ConnectionState.Open;
		public override void Close() => _state = ConnectionState.Closed;
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
		{
			Transaction.Begin();
			return Transaction;
		}

		protected override DbCommand CreateDbCommand()
		{
			_command.Reset();
			return _command;
		}

		// The one instance is handed out again and again.
		protected override void Dispose(bool disposing)
		{
			if (disposing) Disposes++;
		}
	}

	sealed class MemTransaction(MemConnection connection) : DbTransaction
	{
		bool _completed;

		public int Commits { get; private set; }
		public int Rollbacks { get; private set; }

		public void Begin() => _completed = false;

		public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
		protected override DbConnection? DbConnection => connection;

		public override void Commit()
		{
			Commits++;
			_completed = true;
		}

		public override void Rollback()
		{
			Rollbacks++;
			_completed = true;
		}

		// A transaction disposed before it was committed is rolled back, as the mainstream providers do.
		protected override void Dispose(bool disposing)
		{
			if (!disposing || _completed) return;
			Rollbacks++;
			_completed = true;
		}
	}

	static readonly object One = 1;

	sealed class MemCommand(MemConnection connection) : DbCommand
	{
		readonly MemParameters _parameters = new();

		public void Reset() => _parameters.Reset();

		[AllowNull]
		public override string CommandText { get; set; } = "";
		public override int CommandTimeout { get; set; }
		public override CommandType CommandType { get; set; }
		public override bool DesignTimeVisible { get; set; }
		public override UpdateRowSource UpdatedRowSource { get; set; }
		protected override DbConnection? DbConnection { get; set; } = connection;
		protected override DbParameterCollection DbParameterCollection => _parameters;
		protected override DbTransaction? DbTransaction { get; set; }
		public override void Cancel() { }
		public override int ExecuteNonQuery() => 1;
		public override object? ExecuteScalar() => One;
		public override void Prepare() { }
		protected override DbParameter CreateDbParameter() => _parameters.Next();
		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
		protected override void Dispose(bool disposing) { }
	}

	sealed class MemParameter : DbParameter
	{
		public override DbType DbType { get; set; }
		public override ParameterDirection Direction { get; set; }
		public override bool IsNullable { get; set; }

		[AllowNull]
		public override string ParameterName { get; set; } = "";
		public override int Size { get; set; }

		[AllowNull]
		public override string SourceColumn { get; set; } = "";
		public override bool SourceColumnNullMapping { get; set; }
		public override object? Value { get; set; }
		public override void ResetDbType() { }
	}

	// Hands out its few parameters again and again.
	sealed class MemParameters : DbParameterCollection
	{
		readonly MemParameter[] _items = [new(), new(), new(), new(), new(), new(), new(), new()];
		int _count;
		int _next;

		public void Reset() => _count = _next = 0;
		public MemParameter Next() => _items[_next++];

		public override int Count => _count;
		public override object SyncRoot => this;
		public override int Add(object value) => _count++;
		public override void AddRange(Array values) => throw new NotSupportedException();
		public override void Clear() => Reset();
		public override bool Contains(object value) => throw new NotSupportedException();
		public override bool Contains(string value) => throw new NotSupportedException();
		public override void CopyTo(Array array, int index) => throw new NotSupportedException();
		public override IEnumerator GetEnumerator() => throw new NotSupportedException();
		protected override DbParameter GetParameter(int index) => _items[index];
		protected override DbParameter GetParameter(string parameterName) => throw new NotSupportedException();
		public override int IndexOf(object value) => throw new NotSupportedException();
		public override int IndexOf(string parameterName) => throw new NotSupportedException();
		public override void Insert(int index, object value) => throw new NotSupportedException();
		public override void Remove(object value) => throw new NotSupportedException();
		public override void RemoveAt(int index) => throw new NotSupportedException();
		public override void RemoveAt(string parameterName) => throw new NotSupportedException();
		protected override void SetParameter(int index, DbParameter value) => throw new NotSupportedException();
		protected override void SetParameter(string parameterName, DbParameter value) => throw new NotSupportedException();
	}
}
