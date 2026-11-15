using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Open.Database.Extensions.Benchmarks;

// BS-SA (IN6 probe): thin wrappers that time a REAL provider the way the synthetic recorder times its own reader.
// The clock starts on entry of ExecuteReader (sync or async) and stops when the reader is closed or disposed (the first of Close, CloseAsync,
// Dispose, DisposeAsync wins, and the time inside the inner close is included). Every call is forwarded to the inner object, one virtual call each
// (the same extra call for all three read paths). The wrapper also counts the async calls: ReadAsync calls, how many returned a Task that was not
// complete (a real wait), and how often the call after an incomplete ReadAsync ran on another thread than the one that made it (the continuation
// resumed elsewhere; the pool may hand it the same thread again, so this is a lower bound).

/// <summary>What the wrappers collected during one operation.</summary>
public sealed class In6Stats
{
	public long OpenTicks;
	// Time spent waiting in incomplete ReadAsync calls: the sum over the operation and the longest single wait (stopwatch ticks).
	public long WaitTicks, MaxWaitTicks;

	/// <summary>BS-RP: when set, every reader that closes also reports its reader-open time here (the real-provider benchmarks read it as the ReaderOpen column).</summary>
	public SyntheticRecorder? Recorder;

	public int Readers, ReadAsyncCalls, ReadAsyncIncomplete, ResumedOnOtherThread, OtherAsyncCalls, OtherAsyncIncomplete;

	public void Reset()
	{
		OpenTicks = 0;
		WaitTicks = 0;
		MaxWaitTicks = 0;
		Readers = ReadAsyncCalls = ReadAsyncIncomplete = ResumedOnOtherThread = OtherAsyncCalls = OtherAsyncIncomplete = 0;
	}

	// Called on the thread that completes a task, so it must be thread safe.
	internal void AddWait(long ticks)
	{
		Interlocked.Add(ref WaitTicks, ticks);
		long seen = Interlocked.Read(ref MaxWaitTicks);
		while (ticks > seen)
		{
			long before = Interlocked.CompareExchange(ref MaxWaitTicks, ticks, seen);
			if (before == seen) break;
			seen = before;
		}
	}
}

/// <summary>Times one incomplete ReadAsync: from the moment the call returned until its task completed (on the completing thread, before any continuation gets a thread).</summary>
sealed class WaitTimer(In6Stats stats, long start)
{
	public void Done() => stats.AddWait(Stopwatch.GetTimestamp() - start);
}

/// <summary>Wraps an open connection (the caller keeps it open and disposes it). Commands and readers made from it are timed.</summary>
public sealed class TimedConnection(DbConnection inner, In6Stats stats) : DbConnection
{
	[AllowNull]
	public override string ConnectionString { get => inner.ConnectionString; set => inner.ConnectionString = value; }
	public override string Database => inner.Database;
	public override string DataSource => inner.DataSource;
	public override string ServerVersion => inner.ServerVersion;
	public override ConnectionState State => inner.State;
	public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
	public override void Close() => inner.Close();
	public override void Open() => inner.Open();
	public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);
	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
	protected override DbCommand CreateDbCommand() => new TimedCommand(inner.CreateCommand(), this, stats);

	// The probe owns the inner connection.
	protected override void Dispose(bool disposing) { }
}

public sealed class TimedCommand(DbCommand inner, TimedConnection connection, In6Stats stats) : DbCommand
{
	[AllowNull]
	public override string CommandText { get => inner.CommandText; set => inner.CommandText = value; }
	public override int CommandTimeout { get => inner.CommandTimeout; set => inner.CommandTimeout = value; }
	public override CommandType CommandType { get => inner.CommandType; set => inner.CommandType = value; }
	public override bool DesignTimeVisible { get => inner.DesignTimeVisible; set => inner.DesignTimeVisible = value; }
	public override UpdateRowSource UpdatedRowSource { get => inner.UpdatedRowSource; set => inner.UpdatedRowSource = value; }
	protected override DbConnection? DbConnection { get => connection; set { } }
	protected override DbParameterCollection DbParameterCollection => inner.Parameters;
	protected override DbTransaction? DbTransaction { get => inner.Transaction; set => inner.Transaction = value; }
	public override void Cancel() => inner.Cancel();
	public override int ExecuteNonQuery() => inner.ExecuteNonQuery();
	public override object? ExecuteScalar() => inner.ExecuteScalar();
	public override void Prepare() => inner.Prepare();
	protected override DbParameter CreateDbParameter() => inner.CreateParameter();

	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
	{
		long start = Stopwatch.GetTimestamp();
		return new TimedReader(inner.ExecuteReader(behavior), stats, start);
	}

	protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
	{
		long start = Stopwatch.GetTimestamp();
		var reader = await inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		return new TimedReader(reader, stats, start);
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing) inner.Dispose();
		base.Dispose(disposing);
	}
}

public sealed class TimedReader(DbDataReader inner, In6Stats stats, long start) : DbDataReader
{
	int _pendingThread;
	bool _closed;

	// The call after an incomplete async call is the continuation's first move: compare its thread with the one that made the async call.
	void Resumed()
	{
		int pending = _pendingThread;
		if (pending == 0) return;
		_pendingThread = 0;
		if (Environment.CurrentManagedThreadId != pending) stats.ResumedOnOtherThread++;
	}

	void Stamp()
	{
		if (_closed) return;
		_closed = true;
		long ticks = Stopwatch.GetTimestamp() - start;
		stats.OpenTicks += ticks;
		stats.Readers++;
		stats.Recorder?.ReaderClosed(ticks, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	// ---- moving and closing ----------------------------------------------------------------
	public override bool Read()
	{
		Resumed();
		return inner.Read();
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		Resumed();
		stats.ReadAsyncCalls++;
		var task = inner.ReadAsync(cancellationToken);
		if (!task.IsCompleted)
		{
			// Only an incomplete read pays for the timing (one small object and a continuation); a completed read costs the flag test above.
			stats.ReadAsyncIncomplete++;
			_pendingThread = Environment.CurrentManagedThreadId;
			task.ContinueWith(static (_, timer) => ((WaitTimer)timer!).Done(), new WaitTimer(stats, Stopwatch.GetTimestamp()), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		return task;
	}

	public override bool NextResult()
	{
		Resumed();
		return inner.NextResult();
	}

	public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
	{
		Resumed();
		stats.OtherAsyncCalls++;
		var task = inner.NextResultAsync(cancellationToken);
		if (!task.IsCompleted)
		{
			stats.OtherAsyncIncomplete++;
			_pendingThread = Environment.CurrentManagedThreadId;
		}

		return task;
	}

	public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
	{
		stats.OtherAsyncCalls++;
		var task = inner.IsDBNullAsync(ordinal, cancellationToken);
		if (!task.IsCompleted) stats.OtherAsyncIncomplete++;
		return task;
	}

	public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
	{
		stats.OtherAsyncCalls++;
		var task = inner.GetFieldValueAsync<T>(ordinal, cancellationToken);
		if (!task.IsCompleted) stats.OtherAsyncIncomplete++;
		return task;
	}

	public override void Close()
	{
		Resumed();
		inner.Close();
		Stamp();
	}

	public override async Task CloseAsync()
	{
		Resumed();
		await inner.CloseAsync().ConfigureAwait(false);
		Stamp();
	}

	public override async ValueTask DisposeAsync()
	{
		Resumed();
		await inner.DisposeAsync().ConfigureAwait(false);
		Stamp();
	}

	protected override void Dispose(bool disposing)
	{
		if (!disposing) return;
		Resumed();
		inner.Dispose();
		Stamp();
	}

	// ---- everything else is forwarded ----------------------------------------------------------------
	public override int Depth => inner.Depth;
	public override int FieldCount => inner.FieldCount;
	public override int VisibleFieldCount => inner.VisibleFieldCount;
	public override bool HasRows => inner.HasRows;
	public override bool IsClosed => inner.IsClosed;
	public override int RecordsAffected => inner.RecordsAffected;
	public override object this[int ordinal] => inner[ordinal];
	public override object this[string name] => inner[name];
	public override string GetName(int ordinal) => inner.GetName(ordinal);
	public override int GetOrdinal(string name) => inner.GetOrdinal(name);
	public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
	public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
	public override Type GetProviderSpecificFieldType(int ordinal) => inner.GetProviderSpecificFieldType(ordinal);
	public override object GetProviderSpecificValue(int ordinal) => inner.GetProviderSpecificValue(ordinal);
	public override int GetProviderSpecificValues(object[] values) => inner.GetProviderSpecificValues(values);
	public override DataTable? GetSchemaTable() => inner.GetSchemaTable();
	public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
	public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
	public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
	public override char GetChar(int ordinal) => inner.GetChar(ordinal);
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
	public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
	public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
	public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
	public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
	public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
	public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
	public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
	public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
	public override string GetString(int ordinal) => inner.GetString(ordinal);
	public override object GetValue(int ordinal) => inner.GetValue(ordinal);
	public override int GetValues(object[] values) => inner.GetValues(values);
	public override T GetFieldValue<T>(int ordinal) => inner.GetFieldValue<T>(ordinal);
	public override Stream GetStream(int ordinal) => inner.GetStream(ordinal);
	public override TextReader GetTextReader(int ordinal) => inner.GetTextReader(ordinal);
	public override IEnumerator GetEnumerator() => inner.GetEnumerator();
}
