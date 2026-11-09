using System.Collections;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// What the tracked database does for one scenario: the rows it serves and where it fails.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class DbScript
{
	public ImmutableArray<ColumnDef> Columns { get; init; } = [("id", typeof(int)), ("name", typeof(string))];

	public object?[][] Rows { get; init; } = [[1, "a"]];

	/// <summary>The 1-based number of the <c>Read()</c> call that throws; 0 for none.</summary>
	public int FailOnRead { get; init; }

	/// <summary>Thrown by every execute (reader, scalar, non-query).</summary>
	public Exception? ExecuteFailure { get; init; }

	/// <summary>Called with the 1-based number of each <c>Read()</c> before it is served (to cancel in the middle of a read).</summary>
	public Action<int>? OnRead { get; init; }

	/// <summary>When set, <c>NextResult()</c> throws it: the error a provider raises for a statement that follows the rows.</summary>
	public Exception? NextResultError { get; init; }

	/// <summary>When set, <c>CreateParameter</c> throws (a provider rejecting a parameter).</summary>
	public bool FailCreateParameter { get; init; }
}

/// <summary>
/// A tracking fake database: it hands out real <see cref="DbConnection"/>, <see cref="DbCommand"/>, <see cref="DbDataReader"/> and
/// <see cref="DbTransaction"/> subclasses (not substitutes, so every counter is exact) and records what was created and how often each was
/// opened, closed and disposed. Like a provider it closes the connection when a reader that was asked for
/// <see cref="CommandBehavior.CloseConnection"/> is disposed, and, like a provider, it throws when an open connection is opened.
/// Everything the library does to it is a decision made by the library under test.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class DbTally(DbScript? script = null)
{
	public DbScript Script { get; } = script ?? new();

	public List<TrackedConnection> Connections { get; } = [];
	public List<TrackedCommand> Commands { get; } = [];
	public List<TrackedReader> Readers { get; } = [];
	public List<TrackedTransaction> Transactions { get; } = [];

	/// <summary>A new closed connection, as a factory or the caller would create it.</summary>
	public TrackedConnection NewConnection()
	{
		var connection = new TrackedConnection(this);
		Connections.Add(connection);
		return connection;
	}

	/// <summary>A new connection that the CALLER opened: its counters start from zero so anything it records afterwards is the library's.</summary>
	public TrackedConnection NewOpenConnection()
	{
		TrackedConnection connection = NewConnection();
		connection.Open();
		connection.ResetCounters();
		return connection;
	}

	public TrackedFactory Factory => new(this);

	public TrackedPool Pool => new(this);
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedFactory(DbTally tally) : IDbConnectionFactory<TrackedConnection>
{
	public TrackedConnection Create() => tally.NewConnection();

	IDbConnection IDbConnectionFactory.Create() => Create();
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedPool(DbTally tally) : IDbConnectionPool<TrackedConnection>
{
	public int Takes { get; private set; }
	public int Gives { get; private set; }

	public TrackedConnection Take()
	{
		Takes++;
		return tally.NewConnection();
	}

	IDbConnection IDbConnectionPool.Take() => Take();

	public void Give(IDbConnection connection) => Gives++;
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedConnection(DbTally tally) : DbConnection
{
	ConnectionState _state = ConnectionState.Closed;

	/// <summary>How many times the connection went from closed to open.</summary>
	public int Opens { get; private set; }

	/// <summary>How many times the connection went from open to closed.</summary>
	public int Closes { get; private set; }

	public int Disposes { get; private set; }

	public void ResetCounters()
	{
		Opens = 0;
		Closes = 0;
		Disposes = 0;
	}

	[AllowNull]
	public override string ConnectionString { get; set; } = "";
	public override string Database => "tracked";
	public override string DataSource => "tracked";
	public override string ServerVersion => "1.0";
	public override ConnectionState State => _state;
	public override void ChangeDatabase(string databaseName) { }

	public override void Open()
	{
		if (_state == ConnectionState.Open)
			throw new InvalidOperationException("The connection was not closed. The connection's current state is open.");

		Opens++;
		_state = ConnectionState.Open;
	}

	public override void Close()
	{
		if (_state != ConnectionState.Open) return;

		Closes++;
		_state = ConnectionState.Closed;
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing)
		{
			Disposes++;
			Close();
		}

		base.Dispose(disposing);
	}

	protected override DbCommand CreateDbCommand()
	{
		var command = new TrackedCommand(tally, this);
		tally.Commands.Add(command);
		return command;
	}

	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
	{
		var transaction = new TrackedTransaction(this, isolationLevel);
		tally.Transactions.Add(transaction);
		return transaction;
	}
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedTransaction(TrackedConnection connection, IsolationLevel isolationLevel) : DbTransaction
{
	public int Commits { get; private set; }
	public int Rollbacks { get; private set; }
	public int Disposes { get; private set; }

	public override IsolationLevel IsolationLevel => isolationLevel;
	protected override DbConnection? DbConnection => connection;

	public override void Commit() => Commits++;

	public override void Rollback() => Rollbacks++;

	protected override void Dispose(bool disposing)
	{
		if (disposing) Disposes++;
		base.Dispose(disposing);
	}
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedCommand(DbTally tally, TrackedConnection connection) : DbCommand
{
	readonly TrackedParameterCollection _parameters = new();
	int _timeout;

	public int Disposes { get; private set; }

	[AllowNull]
	public override string CommandText { get; set; } = string.Empty;
	public override int CommandTimeout
	{
		get => _timeout;
		set => _timeout = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value)); // as a provider does
	}
	public override CommandType CommandType { get; set; }
	public override bool DesignTimeVisible { get; set; }
	public override UpdateRowSource UpdatedRowSource { get; set; }
	protected override DbConnection? DbConnection { get; set; } = connection;
	protected override DbParameterCollection DbParameterCollection => _parameters;
	protected override DbTransaction? DbTransaction { get; set; }

	public override void Cancel() { }
	public override void Prepare() { }

	protected override DbParameter CreateDbParameter()
		=> tally.Script.FailCreateParameter
			? throw new InvalidOperationException("The provider rejected the parameter.")
			: new TrackedParameter();

	public override int ExecuteNonQuery()
	{
		ThrowIfExecuteFails();
		return 1;
	}

	public override object? ExecuteScalar()
	{
		ThrowIfExecuteFails();
		return 42;
	}

	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
	{
		ThrowIfExecuteFails();
		var reader = new TrackedReader(tally, connection, behavior);
		tally.Readers.Add(reader);
		return reader;
	}

	void ThrowIfExecuteFails()
	{
		if (tally.Script.ExecuteFailure is { } failure) throw failure;
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing) Disposes++;
		base.Dispose(disposing);
	}
}

/// <summary>A reader over the script's rows that counts reads and disposals and closes its connection when asked, as a provider does.</summary>
[ExcludeFromCodeCoverage]
internal sealed class TrackedReader(DbTally tally, TrackedConnection connection, CommandBehavior behavior)
	: TypedReader(tally.Script.Columns, tally.Script.Rows)
{
	bool _closed;

	public int Reads { get; private set; }
	public int Disposes { get; private set; }

	public override bool IsClosed => _closed;

	public override bool NextResult()
	{
		NextResultError ??= tally.Script.NextResultError;
		return base.NextResult();
	}

	public override bool Read()
	{
		int n = ++Reads;
		tally.Script.OnRead?.Invoke(n);
		if (n == tally.Script.FailOnRead) throw new InvalidOperationException("The provider failed while reading.");
		return base.Read();
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing)
		{
			Disposes++;
			_closed = true;
			if ((behavior & CommandBehavior.CloseConnection) != 0) connection.Close();
		}

		base.Dispose(disposing);
	}
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedParameter : DbParameter
{
	public override DbType DbType { get; set; }
	public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
	public override bool IsNullable { get; set; }
	[AllowNull]
	public override string ParameterName { get; set; } = "";
	public override int Size { get; set; }
	[AllowNull]
	public override string SourceColumn { get; set; } = "";
	public override bool SourceColumnNullMapping { get; set; }
	public override object? Value { get; set; }
	public override void ResetDbType() => DbType = DbType.Object;
}

[ExcludeFromCodeCoverage]
internal sealed class TrackedParameterCollection : DbParameterCollection, IEnumerable<DbParameter>
{
	readonly List<DbParameter> _items = [];

	public override int Count => _items.Count;
	public override object SyncRoot => ((ICollection)_items).SyncRoot;

	public override int Add(object value)
	{
		_items.Add((DbParameter)value);
		return _items.Count - 1;
	}

	public void Add(DbParameter value) => _items.Add(value);

	public override void AddRange(Array values)
	{
		foreach (object value in values) Add(value);
	}

	public override void Clear() => _items.Clear();
	public override bool Contains(object value) => _items.Contains((DbParameter)value);
	public override bool Contains(string value) => IndexOf(value) >= 0;
	public override void CopyTo(Array array, int index) => ((ICollection)_items).CopyTo(array, index);
	public override IEnumerator GetEnumerator() => _items.GetEnumerator();
	IEnumerator<DbParameter> IEnumerable<DbParameter>.GetEnumerator() => _items.GetEnumerator();
	public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);
	public override int IndexOf(string parameterName) => _items.FindIndex(p => p.ParameterName == parameterName);
	public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);
	public override void Remove(object value) => _items.Remove((DbParameter)value);
	public override void RemoveAt(int index) => _items.RemoveAt(index);
	public override void RemoveAt(string parameterName) => _items.RemoveAt(IndexOf(parameterName));
	protected override DbParameter GetParameter(int index) => _items[index];
	protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
	protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
	protected override void SetParameter(string parameterName, DbParameter value) => _items[IndexOf(parameterName)] = value;
}

/// <summary>Where the connection of a scenario comes from, and so who owns it.</summary>
public enum LeakSource
{
	/// <summary>A factory creates a closed connection for the call; the library owns it.</summary>
	Factory,

	/// <summary>A pool hands out a closed connection; the pool owns it.</summary>
	Pool,

	/// <summary>The caller passes a closed connection: the library opens it and must close it again.</summary>
	BareClosed,

	/// <summary>The caller passes an open connection: it must be left as it was.</summary>
	BareOpen,

	/// <summary>The caller passes an open connection and a transaction it began: both must be left as they were.</summary>
	BareOpenInTransaction,
}

/// <summary>The assertions every leak scenario ends with, so that all of them mean the same thing.</summary>
[ExcludeFromCodeCoverage]
internal static class LeakAssert
{
	/// <summary>An <see cref="ExpressiveDbCommand"/> on the kind of connection source.</summary>
	public static ExpressiveDbCommand Command(this DbTally tally, LeakSource source, TrackedPool pool) => source switch
	{
		LeakSource.Factory => tally.Factory.Command("select 1"),
		LeakSource.Pool => pool.Command("select 1"),
		LeakSource.BareClosed => tally.NewConnection().Command("select 1"),
		LeakSource.BareOpen => tally.NewOpenConnection().Command("select 1"),
		_ => ((TrackedTransaction)tally.NewOpenConnection().BeginTransaction()).Command("select 1"),
	};

	/// <summary>Every command and every reader the library created was disposed, exactly once.</summary>
	public static void CommandsAndReadersDisposed(DbTally tally)
	{
		Assert.All(tally.Commands, c => Assert.Equal(1, c.Disposes));
		Assert.All(tally.Readers, r => Assert.Equal(1, r.Disposes));
	}

	/// <summary>The connection is as its owner would want it: see <see cref="LeakSource"/>.</summary>
	/// <param name="expectedConnections">How many connections the call should have asked for (0 when it never got as far as that).</param>
	public static void ConnectionsReleased(DbTally tally, LeakSource source, TrackedPool pool, int expectedConnections)
	{
		switch (source)
		{
			case LeakSource.Factory:
				Assert.Equal(expectedConnections, tally.Connections.Count);
				Assert.All(tally.Connections, c =>
				{
					Assert.Equal(1, c.Disposes);
					Assert.Equal(ConnectionState.Closed, c.State);
					Assert.Equal(c.Opens, c.Closes);
				});
				break;

			case LeakSource.Pool:
				Assert.Equal(expectedConnections, pool.Takes);
				Assert.Equal(pool.Takes, pool.Gives);
				Assert.All(tally.Connections, c => Assert.Equal(0, c.Disposes));
				break;

			case LeakSource.BareClosed:
				TrackedConnection closed = Assert.Single(tally.Connections);
				Assert.Equal(0, closed.Disposes);
				Assert.Equal(ConnectionState.Closed, closed.State);
				Assert.Equal(closed.Opens, closed.Closes);
				break;

			default: // The caller opened it (and, for BareOpenInTransaction, began a transaction on it): both are left exactly as they were.
				TrackedConnection open = Assert.Single(tally.Connections);
				Assert.Equal(ConnectionState.Open, open.State);
				Assert.Equal(0, open.Opens);
				Assert.Equal(0, open.Closes);
				Assert.Equal(0, open.Disposes);
				if (source == LeakSource.BareOpenInTransaction)
				{
					TrackedTransaction tx = Assert.Single(tally.Transactions);
					Assert.Equal(0, tx.Commits);
					Assert.Equal(0, tx.Rollbacks);
					Assert.Equal(0, tx.Disposes);
				}

				break;
		}
	}
}
