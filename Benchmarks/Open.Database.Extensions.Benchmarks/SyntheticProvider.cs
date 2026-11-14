using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Benchmarks;

// Synthetic provider v2 (BS1, BS2). Deterministic typed data of any width (1 to 20 columns) and row count, an optional
// cost per row and per NextResult, extra empty result sets for a drain, and a recorder that times the reader itself.
// BenchProvider.cs stays as it was; the parameter types in it are reused.
//
// What the provider does and does not model:
// - The typed getters, Read and ReadAsync allocate nothing (checked by --verify). GetValue and GetValues box value types
//   (inherent to returning object), GetFieldValueAsync allocates its Task (inherent); strings and byte[] are handed out
//   as the stored instance, where a real provider would allocate a copy.
// - Under CommandBehavior.SequentialAccess a column may not be read after a later one in the same row, as in SqlClient.
// - CommandBehavior.SingleResult serves no extra result sets.
// - There is no packet-boundary model. Sync against async on a real wire is a different study.

/// <summary>A fixed busy-wait, in iterations, so a cost has the same shape on every machine run to run.</summary>
public static class Spin
{
	public static void Iterations(int iterations) => Thread.SpinWait(iterations);

	/// <summary>Busy-waits (never sleeps) for the given time; for the self-tests, where the delay must be exact.</summary>
	public static void Microseconds(double microseconds)
	{
		long end = Stopwatch.GetTimestamp() + (long)(microseconds * Stopwatch.Frequency / 1_000_000d);
		while (Stopwatch.GetTimestamp() < end) Thread.SpinWait(4);
	}
}

/// <summary>How a <see cref="SyntheticConnection"/> behaves. Everything defaults to off.</summary>
public sealed class SyntheticOptions
{
	// Cost per row, in Spin iterations. 0 = off, a pure CPU comparison.
	// PLACEHOLDER: this is set later from ONE LocalDB measurement (a separate step); it was not measured in BS-A and must not be guessed.
	public const int DefaultRowCostIterations = 0;

	// Cost per NextResult call, in Spin iterations. 0 = off. PLACEHOLDER: set later from the same LocalDB measurement.
	public const int DefaultNextResultCostIterations = 0;

	/// <summary>A fixed busy-wait per row returned by Read or ReadAsync; 0 = off.</summary>
	public int RowCostIterations { get; init; } = DefaultRowCostIterations;

	/// <summary>A fixed busy-wait per NextResult or NextResultAsync call (the call that returns false pays it too); 0 = off.</summary>
	public int NextResultCostIterations { get; init; } = DefaultNextResultCostIterations;

	/// <summary>How many extra empty result sets follow the rows, so a drain loop has something to drain. Ignored under CommandBehavior.SingleResult.</summary>
	public int ExtraResultSets { get; init; }

	/// <summary>Receives the timestamps. Shared by default so one benchmark case reads one recorder.</summary>
	public SyntheticRecorder Recorder { get; init; } = new();
}

/// <summary>What the recorder holds, in microseconds per reader. Reader-open includes the drain.</summary>
public readonly record struct OpenTimeSummary(long Readers, double MeanOpenUs, double MedianOpenUs, double MeanDrainUs, double MedianDrainUs)
{
	public override string ToString()
		=> $"readers={Readers} reader-open mean={MeanOpenUs:F2}us median={MedianOpenUs:F2}us | drain mean={MeanDrainUs:F2}us median={MedianDrainUs:F2}us";
}

/// <summary>
/// Collects reader-open time (from entry of ExecuteDbDataReader until the reader is closed or disposed) and the time spent
/// inside NextResult/NextResultAsync, per reader. Thread safe, and nothing is allocated per row (a reader adds its numbers once, when it closes).
/// </summary>
public sealed class SyntheticRecorder
{
	const int RingSize = 1024; // The median is over the last RingSize readers, so warm-up does not skew it.

	readonly long[] _open = new long[RingSize], _drain = new long[RingSize];
	long _readers, _openTicks, _drainTicks, _rows, _rowCostCharges, _nextResultCalls, _nextResultCostCharges, _extraSetsServed;

	public long Readers => Interlocked.Read(ref _readers);
	public long Rows => Interlocked.Read(ref _rows);
	/// <summary>How many rows paid the per-row cost (0 when the cost is off).</summary>
	public long RowCostCharges => Interlocked.Read(ref _rowCostCharges);
	/// <summary>How many NextResult/NextResultAsync calls were made on the reader.</summary>
	public long NextResultCalls => Interlocked.Read(ref _nextResultCalls);
	/// <summary>How many NextResult calls paid the cost (0 when the cost is off).</summary>
	public long NextResultCostCharges => Interlocked.Read(ref _nextResultCostCharges);
	public long ExtraSetsServed => Interlocked.Read(ref _extraSetsServed);

	public void Reset()
	{
		Interlocked.Exchange(ref _readers, 0);
		Interlocked.Exchange(ref _openTicks, 0);
		Interlocked.Exchange(ref _drainTicks, 0);
		Interlocked.Exchange(ref _rows, 0);
		Interlocked.Exchange(ref _rowCostCharges, 0);
		Interlocked.Exchange(ref _nextResultCalls, 0);
		Interlocked.Exchange(ref _nextResultCostCharges, 0);
		Interlocked.Exchange(ref _extraSetsServed, 0);
		Array.Clear(_open);
		Array.Clear(_drain);
	}

	internal void ReaderClosed(long openTicks, long drainTicks, long rows, long rowCostCharges, long nextResultCalls, long nextResultCostCharges, long extraSets)
	{
		int slot = (int)((Interlocked.Increment(ref _readers) - 1) & (RingSize - 1));
		_open[slot] = openTicks;
		_drain[slot] = drainTicks;
		Interlocked.Add(ref _openTicks, openTicks);
		Interlocked.Add(ref _drainTicks, drainTicks);
		Interlocked.Add(ref _rows, rows);
		Interlocked.Add(ref _rowCostCharges, rowCostCharges);
		Interlocked.Add(ref _nextResultCalls, nextResultCalls);
		Interlocked.Add(ref _nextResultCostCharges, nextResultCostCharges);
		Interlocked.Add(ref _extraSetsServed, extraSets);
	}

	public OpenTimeSummary Summary()
	{
		long readers = Readers;
		if (readers == 0) return default;
		double toUs = 1_000_000d / Stopwatch.Frequency;
		int n = (int)Math.Min(readers, RingSize);
		return new OpenTimeSummary(
			readers,
			Interlocked.Read(ref _openTicks) * toUs / readers,
			Median(_open, n) * toUs,
			Interlocked.Read(ref _drainTicks) * toUs / readers,
			Median(_drain, n) * toUs);
	}

	// A reader in flight while this runs may be half written; read it after the work has stopped.
	static double Median(long[] ring, int n)
	{
		var copy = new long[n];
		Array.Copy(ring, copy, n);
		Array.Sort(copy);
		return n % 2 == 1 ? copy[n / 2] : (copy[(n / 2) - 1] + copy[n / 2]) / 2d;
	}
}

/// <summary>One column of the catalog.</summary>
public sealed class ColumnSpec(string name, Type type, int nullEvery, int nullOffset, Func<int, Array> fill)
{
	public string Name { get; } = name;
	/// <summary>What the provider reports as the field type: the underlying type for a nullable column, like a real provider.</summary>
	public Type Type { get; } = type;
	/// <summary>0 = never NULL; otherwise row i is NULL when (i + NullOffset) % NullEvery == 0.</summary>
	public int NullEvery { get; } = nullEvery;
	public int NullOffset { get; } = nullOffset;
	public Func<int, Array> Fill { get; } = fill;
	public bool IsNullable => NullEvery > 0;
}

/// <summary>The 20 typed columns. Data comes from the row index alone, so it is the same on every run and every machine.</summary>
public static class SyntheticCatalog
{
	public const int MaxWidth = 20;
	public static readonly int[] Widths = [1, 3, 5, 10, 20];
	public static readonly int[] RowCounts = [1, 100, 1000, 10000];

	static readonly DateTime Start = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
	const string Lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ";

	static ColumnSpec C<T>(string name, Func<int, T> value, int nullEvery = 0, int nullOffset = 0)
		=> new(name, typeof(T), nullEvery, nullOffset, rows =>
		{
			var a = new T[rows];
			for (int i = 0; i < rows; i++) a[i] = value(i);
			return a;
		});

	public static readonly ColumnSpec[] Columns =
	[
		C("Id", static i => i + 1),                                                                   //  1 int (the scalar case)
		C("Created", static i => Start.AddSeconds(i * 37L)),                                           //  2 DateTime
		C("Title", static i => "Title " + i),                                                          //  3 short string
		C("Price", static i => 10m + (i * 0.25m)),                                                     //  4 decimal
		C("Qty", static i => i * 7, nullEvery: 5, nullOffset: 2),                                     //  5 int?  (20% NULL)
		C("IsActive", static i => i % 3 != 0),                                                         //  6 bool
		C("Key", static i => new Guid(i, (short)(i >> 3), 7, 1, 2, 3, 4, 5, 6, 7, (byte)i)),            //  7 Guid
		C("Score", static i => (i * 1.5) + 0.125),                                                     //  8 double
		C("Note", static i => "note " + (i % 97), nullEvery: 4, nullOffset: 1),                       //  9 short string? (25% NULL)
		C("Blob", static i => Blob(i)),                                                                // 10 byte[]
		C("Big", static i => ((long)i << 24) ^ 0x5DEECE66DL),                                          // 11 long
		C("Body", static i => LongText(i)),                                                            // 12 long string
		C("Amount", static i => i * 1.125m, nullEvery: 3, nullOffset: 0),                              // 13 decimal? (33% NULL)
		C("Stamp", static i => Start.AddMinutes(i), nullEvery: 4, nullOffset: 3),                      // 14 DateTime? (25% NULL)
		C("Tag", static i => "t" + (i % 7)),                                                           // 15 short string
		C("Ratio", static i => i / 8.0),                                                               // 16 double
		C("Flag", static i => i % 2 == 0, nullEvery: 2, nullOffset: 1),                                // 17 bool? (50% NULL)
		C("Link", static i => new Guid(7, (short)i, (short)(i >> 8), 9, 8, 7, 6, 5, 4, 3, (byte)(i >> 4)), nullEvery: 5, nullOffset: 4), // 18 Guid? (20% NULL)
		C("Count", static i => i * 1_000_003L),                                                        // 19 long
		C("Summary", static i => LongText(i + 3), nullEvery: 6, nullOffset: 5),                        // 20 long string? (17% NULL)
	];

	static byte[] Blob(int i)
	{
		var b = new byte[16];
		for (int j = 0; j < b.Length; j++) b[j] = unchecked((byte)((i * 31) + j));
		return b;
	}

	static string LongText(int i)
	{
		string s = Lorem + i;
		return s.Length < 160 ? s : s[..160];
	}
}

/// <summary>The data of one result set: the first <see cref="Width"/> columns of the catalog, <see cref="Rows"/> rows.</summary>
public sealed class SyntheticTable
{
	public required int Width { get; init; }
	public required int Rows { get; init; }
	public required string[] Names { get; init; }
	public required Type[] Types { get; init; }
	public required Array[] Columns { get; init; }
	public required bool[]?[] Nulls { get; init; }

	readonly Dictionary<string, int> _ordinals = new(StringComparer.OrdinalIgnoreCase);

	public int Ordinal(string name)
	{
		if (_ordinals.Count == 0)
			for (int i = 0; i < Names.Length; i++) _ordinals[Names[i]] = i;
		return _ordinals.TryGetValue(name, out int o) ? o : throw new IndexOutOfRangeException(name);
	}

	public bool IsNullable(int column) => Nulls[column] is not null;

	public static SyntheticTable Create(int width, int rows)
	{
		if (width is < 1 or > SyntheticCatalog.MaxWidth) throw new ArgumentOutOfRangeException(nameof(width));
		if (rows < 0) throw new ArgumentOutOfRangeException(nameof(rows));

		var names = new string[width];
		var types = new Type[width];
		var columns = new Array[width];
		var nulls = new bool[]?[width];
		for (int c = 0; c < width; c++)
		{
			var spec = SyntheticCatalog.Columns[c];
			names[c] = spec.Name;
			types[c] = spec.Type;
			columns[c] = spec.Fill(rows);
			if (spec.NullEvery > 0)
			{
				var mask = new bool[rows];
				for (int i = 0; i < rows; i++) mask[i] = (i + spec.NullOffset) % spec.NullEvery == 0;
				nulls[c] = mask;
			}
		}

		return new SyntheticTable { Width = width, Rows = rows, Names = names, Types = types, Columns = columns, Nulls = nulls };
	}
}

public sealed class SyntheticConnection(SyntheticTable table, SyntheticOptions options) : DbConnection
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
	protected override DbCommand CreateDbCommand() => new SyntheticCommand(this);
}

public sealed class SyntheticCommand(SyntheticConnection connection) : DbCommand
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
		return new SyntheticReader(connection.Table, connection.Options, behavior, start);
	}

	protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
	{
		long start = Stopwatch.GetTimestamp();
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<DbDataReader>(cancellationToken);
		return Task.FromResult<DbDataReader>(new SyntheticReader(connection.Table, connection.Options, behavior, start));
	}
}

public sealed class SyntheticReader : DbDataReader
{
	static readonly Task<bool> TrueTask = Task.FromResult(true), FalseTask = Task.FromResult(false);

	readonly SyntheticTable _table;
	readonly SyntheticRecorder _recorder;
	readonly int _rowCost, _nextResultCost, _extraSets;
	readonly long _openStart;
	readonly bool _sequential;

	int _row = -1, _set, _lastOrdinal = -1;
	bool _closed;
	long _drainTicks, _rows, _rowCostCharges, _nextResultCalls, _nextResultCostCharges;

	internal SyntheticReader(SyntheticTable table, SyntheticOptions options, CommandBehavior behavior, long openStart)
	{
		_table = table;
		_recorder = options.Recorder;
		_rowCost = options.RowCostIterations;
		_nextResultCost = options.NextResultCostIterations;
		_extraSets = (behavior & CommandBehavior.SingleResult) != 0 ? 0 : options.ExtraResultSets;
		_sequential = (behavior & CommandBehavior.SequentialAccess) != 0;
		_openStart = openStart;
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
		_row++;
		_lastOrdinal = -1;
		_rows++;
		if (_rowCost > 0)
		{
			Spin.Iterations(_rowCost);
			_rowCostCharges++;
		}

		return true;
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<bool>(cancellationToken);
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
		_recorder.ReaderClosed(Stopwatch.GetTimestamp() - _openStart, _drainTicks, _rows, _rowCostCharges, _nextResultCalls, _nextResultCostCharges, _set);
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
