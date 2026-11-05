using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A minimal in-memory <see cref="DbConnection"/> that records everything done to it in <see cref="Log"/>,
/// in order, so tests can assert exactly what a library operation did and in what sequence.
/// Like a real provider, <see cref="Open"/> throws if the connection is already open.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class FakeDbConnection : DbConnection
{
	ConnectionState _state = ConnectionState.Closed;

	/// <summary>Ordered log: open, close, begin, commit, rollback, tx-dispose, tx-dispose:rollback.</summary>
	public List<string> Log { get; } = [];

	public int OpenCalls { get; private set; }

	/// <summary>When set, <see cref="BeginDbTransaction"/> throws it.</summary>
	public Exception? BeginException { get; set; }

	/// <summary>When set, <see cref="Close"/> closes the connection and then throws it.</summary>
	public Exception? CloseException { get; set; }

	public FakeDbTransaction? LastTransaction { get; private set; }

	public void SetState(ConnectionState state) => _state = state;

	[AllowNull]
	public override string ConnectionString { get; set; } = "";
	public override string Database => "fake";
	public override string DataSource => "fake";
	public override string ServerVersion => "1.0";
	public override ConnectionState State => _state;
	public override void ChangeDatabase(string databaseName) { }

	public override void Open()
	{
		OpenCalls++;
		Log.Add("open");
		if (_state.HasFlag(ConnectionState.Open))
			throw new InvalidOperationException("The connection was not closed. The connection's current state is open.");

		_state = ConnectionState.Open;
	}

	public override void Close()
	{
		Log.Add("close");
		_state = ConnectionState.Closed;
		if (CloseException is not null)
			throw CloseException;
	}

	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
	{
		Log.Add("begin");
		if (BeginException is not null)
			throw BeginException;

		return LastTransaction = new FakeDbTransaction(this, isolationLevel);
	}

	protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
}

/// <summary>
/// A <see cref="DbTransaction"/> that, like the mainstream providers, rolls back on dispose when it was
/// neither committed nor rolled back, and, like a real provider, throws if completed twice.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class FakeDbTransaction(FakeDbConnection connection, IsolationLevel isolationLevel) : DbTransaction
{
	bool _completed;

	public int CommitCalls { get; private set; }
	public int RollbackCalls { get; private set; }
	public int DisposeCalls { get; private set; }

	public override IsolationLevel IsolationLevel => isolationLevel;
	protected override DbConnection? DbConnection => connection;

	public override void Commit()
	{
		connection.Log.Add("commit");
		CommitCalls++;
		Complete();
	}

	public override void Rollback()
	{
		connection.Log.Add("rollback");
		RollbackCalls++;
		Complete();
	}

	void Complete()
	{
		if (_completed)
			throw new InvalidOperationException("This transaction has completed; it is no longer usable.");

		_completed = true;
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing)
		{
			DisposeCalls++;
			connection.Log.Add(_completed ? "tx-dispose" : "tx-dispose:rollback");
			_completed = true;
		}

		base.Dispose(disposing);
	}
}
