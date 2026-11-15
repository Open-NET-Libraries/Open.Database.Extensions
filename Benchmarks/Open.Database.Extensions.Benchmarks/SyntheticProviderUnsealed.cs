using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Benchmarks;

// DS1b-F2u: a copy of the synthetic provider path (connection, command, reader) whose reader class is NOT sealed.
// The code is the SyntheticProvider.cs code (same getters, same Read, same Touch), copied with three names changed
// (the Unsealed prefix) and one thing left out: the async refill branch of ReadAsync (BufferRows is 0 in every F run, the
// timer type it uses takes the sealed reader). The one difference that matters is `sealed`: F2 calls the getters through
// a sealed class (direct calls), F2u through this one (virtual calls on an unsealed class), so F2 minus F2u is what the
// sealed class is worth. The measured variants use UnsealedSyntheticConnection over the same table and options as the
// synthetic connection of the case (the recorder is shared).
//
public sealed class UnsealedSyntheticConnection(SyntheticTable table, SyntheticOptions options) : DbConnection
{
	public SyntheticTable Table { get; } = table;
	public SyntheticOptions Options { get; } = options;

	[AllowNull]
	public override string ConnectionString { get; set; } = "synthetic";
	public override string Database => "synthetic";
	public override string DataSource => "synthetic";
	public override string ServerVersion => "2.0";
	public override ConnectionState State => ConnectionState.Open;
	public override void ChangeDatabase(string databaseName) { }
	public override void Close() { }
	public override void Open() { }
	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
	protected override DbCommand CreateDbCommand() => new UnsealedSyntheticCommand(this);
}

public sealed class UnsealedSyntheticCommand(UnsealedSyntheticConnection connection) : DbCommand
{
	BenchParameterCollection? _parameters;

	[AllowNull]
	public override string CommandText { get; set; } = "";
	public override int CommandTimeout { get; set; } = 30;
	public override CommandType CommandType { get; set; }
	public override bool DesignTimeVisible { get; set; }
	public override UpdateRowSource UpdatedRowSource { get; set; }
	protected override DbConnection? DbConnection { get => connection; set { } }
	protected override DbParameterCollection DbParameterCollection => _parameters ??= [];
	protected override DbTransaction? DbTransaction { get; set; }
	public override void Cancel() { }
	public override int ExecuteNonQuery() => 1;
	public override void Prepare() { }
	protected override DbParameter CreateDbParameter() => new BenchParameter();

	public override object? ExecuteScalar()
	{
		using var reader = ExecuteDbDataReader(CommandBehavior.Default);
		return reader.Read() ? reader.GetValue(0) : null;
	}

	// The reader-open clock starts here, on entry, before anything else happens.
	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
	{
		long start = Stopwatch.GetTimestamp();
		return new UnsealedSyntheticReader(connection.Table, connection.Options, behavior, start);
	}

	protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
	{
		long start = Stopwatch.GetTimestamp();
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<DbDataReader>(cancellationToken);
		return Task.FromResult<DbDataReader>(new UnsealedSyntheticReader(connection.Table, connection.Options, behavior, start));
	}
}

public class UnsealedSyntheticReader : DbDataReader
{
	static readonly Task<bool> TrueTask = Task.FromResult(true), FalseTask = Task.FromResult(false);

	readonly SyntheticTable _table;
	readonly SyntheticRecorder _recorder;
	readonly int _rowCost, _nextResultCost, _extraSets, _bufferRows;
	readonly long _openStart, _refillTicks;
	readonly bool _sequential;

	int _row = -1, _set, _lastOrdinal = -1;
	bool _closed;
	long _drainTicks, _rows, _rowCostCharges, _nextResultCalls, _nextResultCostCharges, _refillsBlocked;

	internal UnsealedSyntheticReader(SyntheticTable table, SyntheticOptions options, CommandBehavior behavior, long openStart)
	{
		_table = table;
		_recorder = options.Recorder;
		_rowCost = options.RowCostIterations;
		_nextResultCost = options.NextResultCostIterations;
		_extraSets = (behavior & CommandBehavior.SingleResult) != 0 ? 0 : options.ExtraResultSets;
		_sequential = (behavior & CommandBehavior.SequentialAccess) != 0;
		_openStart = openStart;
		_bufferRows = Math.Max(0, options.BufferRows);
		_refillTicks = (long)(Math.Max(0, options.RefillCostUs) * Stopwatch.Frequency / 1_000_000d);
	}

	public override int Depth => 0;
	public override int FieldCount => _table.Width;
	public override bool HasRows => _set == 0 && _table.Rows > 0;
	public override bool IsClosed => _closed;
	public override int RecordsAffected => -1;
	public override object this[int ordinal] => GetValue(ordinal);
	public override object this[string name] => GetValue(_table.Ordinal(name));

	// ---- moving ----------------------------------------------------------------
	public override bool Read()
	{
		if (_closed) throw new InvalidOperationException("The reader is closed.");
		if (_set != 0 || _row + 1 >= _table.Rows) return false;
		if (_bufferRows != 0 && AtBoundary())
		{
			// The buffer ran out: the calling thread waits for the refill, holding its thread (and the open reader).
			_refillsBlocked++;
			long end = Stopwatch.GetTimestamp() + _refillTicks;
			while (Stopwatch.GetTimestamp() < end) Thread.SpinWait(4);
		}

		Advance();
		return true;
	}

	// The call that would serve row N, 2N, ... (0-based) while that row exists; the first row never waits.
	bool AtBoundary() => _row >= 0 && (_row + 1) % _bufferRows == 0;

	// Moves to the next row and pays the per-row cost. Callers have checked that the row exists.
	void Advance()
	{
		_row++;
		_lastOrdinal = -1;
		_rows++;
		if (_rowCost > 0)
		{
			Spin.Iterations(_rowCost);
			_rowCostCharges++;
		}
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<bool>(cancellationToken);
		// (the async refill branch of SyntheticReader.ReadAsync is left out of this copy)
		return Read() ? TrueTask : FalseTask;
	}

	// The time inside this call (and its async twin) is the drain; the recorder reports it apart from the rest of reader-open time.
	public override bool NextResult()
	{
		if (_closed) throw new InvalidOperationException("The reader is closed.");
		long t0 = Stopwatch.GetTimestamp();
		_nextResultCalls++;
		if (_nextResultCost > 0)
		{
			Spin.Iterations(_nextResultCost);
			_nextResultCostCharges++;
		}

		bool more = _set < _extraSets;
		if (more) _set++;
		_drainTicks += Stopwatch.GetTimestamp() - t0;
		return more;
	}

	public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
	{
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<bool>(cancellationToken);
		return NextResult() ? TrueTask : FalseTask;
	}

	// Closing and disposing end the reader-open clock; the first one wins.
	public override void Close()
	{
		if (_closed) return;
		_closed = true;
		_recorder.ReaderClosed(Stopwatch.GetTimestamp() - _openStart, _drainTicks, _rows, _rowCostCharges, _nextResultCalls, _nextResultCostCharges, _set, _refillsBlocked, 0);
	}

	// ---- metadata ----------------------------------------------------------------
	public override string GetName(int ordinal) => _table.Names[ordinal];
	public override int GetOrdinal(string name) => _table.Ordinal(name);
	public override Type GetFieldType(int ordinal) => _table.Types[ordinal];
	public override string GetDataTypeName(int ordinal) => _table.Types[ordinal].Name;

	// ---- values: nothing here allocates ----------------------------------------------------------------
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	void Touch(int ordinal)
	{
		if (!_sequential) return;
		if (ordinal < _lastOrdinal) ThrowOutOfOrder(ordinal);
		_lastOrdinal = ordinal;
	}

	[DoesNotReturn]
	void ThrowOutOfOrder(int ordinal)
		=> throw new InvalidOperationException($"With CommandBehavior.SequentialAccess, column {ordinal} cannot be read after column {_lastOrdinal}.");

	public override bool IsDBNull(int ordinal)
	{
		Touch(ordinal);
		return _table.Nulls[ordinal] is { } n && n[_row];
	}

	public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
		=> IsDBNull(ordinal) ? TrueTask : FalseTask;

	public override bool GetBoolean(int ordinal) { Touch(ordinal); return ((bool[])_table.Columns[ordinal])[_row]; }
	public override DateTime GetDateTime(int ordinal) { Touch(ordinal); return ((DateTime[])_table.Columns[ordinal])[_row]; }
	public override decimal GetDecimal(int ordinal) { Touch(ordinal); return ((decimal[])_table.Columns[ordinal])[_row]; }
	public override double GetDouble(int ordinal) { Touch(ordinal); return ((double[])_table.Columns[ordinal])[_row]; }
	public override Guid GetGuid(int ordinal) { Touch(ordinal); return ((Guid[])_table.Columns[ordinal])[_row]; }
	public override int GetInt32(int ordinal) { Touch(ordinal); return ((int[])_table.Columns[ordinal])[_row]; }
	public override long GetInt64(int ordinal) { Touch(ordinal); return ((long[])_table.Columns[ordinal])[_row]; }
	public override string GetString(int ordinal) { Touch(ordinal); return ((string[])_table.Columns[ordinal])[_row]; }

	/// <summary>Copies into the caller's buffer, or returns the length when the buffer is null (the ADO.NET contract). Allocates nothing.</summary>
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		Touch(ordinal);
		byte[] source = ((byte[][])_table.Columns[ordinal])[_row];
		if (buffer is null) return source.Length;
		long available = source.Length - dataOffset;
		if (available <= 0) return 0;
		int count = (int)Math.Min(length, available);
		Array.Copy(source, (int)dataOffset, buffer, bufferOffset, count);
		return count;
	}

	// Boxes a value type, as any object-returning path must; a string or a byte[] is returned as the stored instance.
	public override object GetValue(int ordinal)
	{
		if (IsDBNull(ordinal)) return DBNull.Value;
		return _table.Columns[ordinal] switch
		{
			int[] a => a[_row],
			string[] a => a[_row],
			DateTime[] a => a[_row],
			decimal[] a => a[_row],
			bool[] a => a[_row],
			double[] a => a[_row],
			Guid[] a => a[_row],
			long[] a => a[_row],
			DateTimeOffset[] a => a[_row],
			byte[][] a => a[_row],
			_ => throw new NotSupportedException(),
		};
	}

	public override int GetValues(object[] values)
	{
		int n = Math.Min(values.Length, FieldCount);
		for (int i = 0; i < n; i++) values[i] = GetValue(i);
		return n;
	}

	// The typed read. For every type of the catalog, and its nullable form, nothing is boxed (the JIT drops the casts through object when T is known).
	public override T GetFieldValue<T>(int ordinal)
	{
		if (IsDBNull(ordinal))
		{
			if (default(T) is null) return default!;
			throw new InvalidCastException($"The column is NULL and {typeof(T)} cannot hold that.");
		}

		object column = _table.Columns[ordinal];
		if (column is T[] exact) return exact[_row];

		if (typeof(T) == typeof(int?)) return (T)(object)(int?)((int[])column)[_row];
		if (typeof(T) == typeof(long?)) return (T)(object)(long?)((long[])column)[_row];
		if (typeof(T) == typeof(decimal?)) return (T)(object)(decimal?)((decimal[])column)[_row];
		if (typeof(T) == typeof(double?)) return (T)(object)(double?)((double[])column)[_row];
		if (typeof(T) == typeof(bool?)) return (T)(object)(bool?)((bool[])column)[_row];
		if (typeof(T) == typeof(DateTime?)) return (T)(object)(DateTime?)((DateTime[])column)[_row];
		if (typeof(T) == typeof(Guid?)) return (T)(object)(Guid?)((Guid[])column)[_row];
		return (T)GetValue(ordinal);
	}

	/// <summary>Completes at once; the Task it returns is the one allocation (inherent to the base signature).</summary>
	public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
	{
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<T>(cancellationToken);
		try { return Task.FromResult(GetFieldValue<T>(ordinal)); }
		catch (Exception ex) { return Task.FromException<T>(ex); }
	}

	public override byte GetByte(int ordinal) => throw new NotSupportedException();
	public override char GetChar(int ordinal) => throw new NotSupportedException();
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
	public override short GetInt16(int ordinal) => throw new NotSupportedException();
	public override float GetFloat(int ordinal) => throw new NotSupportedException();
	public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
}
