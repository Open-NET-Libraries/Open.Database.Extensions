using System.Collections;
using System.Data;
using System.Data.Common;
using System.Reflection;
using Open.Database.Extensions.Generated;

// The counters are process-wide: one test at a time, so each test's deltas are its own.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>A column of a test table: its name and the CLR type its reader reports.</summary>
internal readonly record struct Col(string Name, Type Type)
{
	public static Col Of<T>(string name) => new(name, typeof(T));
}

/// <summary>
/// An <see cref="IExecuteReader"/> over an in-memory <see cref="DataTable"/>: every execution reads a new <see cref="DataTableReader"/>,
/// a <see cref="DbDataReader"/> that reports each column's declared type, so the typed getters and the NULL tests run as on a provider.
/// </summary>
internal sealed class TableQuery(Col[] columns, params object?[][] rows) : IExecuteReaderSync
{
	public static DataTable Table(Col[] columns, params object?[][] rows)
	{
		var table = new DataTable();
		foreach (Col c in columns) table.Columns.Add(new DataColumn(c.Name, c.Type));
		foreach (object?[] row in rows)
		{
			object[] values = new object[row.Length];
			for (int i = 0; i < row.Length; i++) values[i] = row[i] ?? DBNull.Value;
			table.Rows.Add(values);
		}

		return table;
	}

	/// <summary>Every reader it opens allows each cell once, in ascending column order (as a SequentialAccess reader does).</summary>
	public bool Sequential { get; init; }

	/// <summary>The table, for the <see cref="DataTable"/> receiver.</summary>
	public DataTable Data { get; } = Table(columns, rows);

	/// <summary>A reader over the table, for the <see cref="IDataReader"/> receiver.</summary>
	public DbDataReader OpenReader() => Open();

	public CancellationToken CancellationToken => default;

	DbDataReader Open()
	{
		DataTableReader reader = Data.CreateDataReader();
		return Sequential ? new SequentialReader(reader) : reader;
	}

	public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = Open();
		handler(reader);
	}

	public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = Open();
		return transform(reader);
	}

	public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = Open();
		await handler(reader).ConfigureAwait(false);
	}

	public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = Open();
		return await transform(reader).ConfigureAwait(false);
	}

	public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = Open();
		return transform(reader, state);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = Open();
		return await transform(reader, state).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		await using DbDataReader reader = Open();
		return await transform(reader, state).ConfigureAwait(false);
	}
}

/// <summary>What the generated code did between two points: read from the test-only counters it emits.</summary>
internal readonly record struct Counts(
	long Intercepted,
	long Served,
	long Declined,
	long RowsServed,
	long RowsDeclined,
	long Full,
	long Guarded,
	long Literal,
	long Converting)
{
	public static Counts Now() => new(
		Interlocked.Read(ref OdxGeneratedCounters.Intercepted),
		Interlocked.Read(ref OdxGeneratedCounters.Served),
		Interlocked.Read(ref OdxGeneratedCounters.Declined),
		Interlocked.Read(ref OdxGeneratedCounters.RowsServed),
		Interlocked.Read(ref OdxGeneratedCounters.RowsDeclined),
		Interlocked.Read(ref OdxGeneratedCounters.Modes[1]),
		Interlocked.Read(ref OdxGeneratedCounters.Modes[2]),
		Interlocked.Read(ref OdxGeneratedCounters.Modes[3]),
		Interlocked.Read(ref OdxGeneratedCounters.Modes[4]));

	public Counts Since(Counts start) => new(
		Intercepted - start.Intercepted,
		Served - start.Served,
		Declined - start.Declined,
		RowsServed - start.RowsServed,
		RowsDeclined - start.RowsDeclined,
		Full - start.Full,
		Guarded - start.Guarded,
		Literal - start.Literal,
		Converting - start.Converting);
}

/// <summary>The parity checks: the generated path against the runtime map, row by row and member by member, or exception against exception.</summary>
internal static class Parity
{
	/// <summary>Every row equal, every public property of each equal: bit for bit (decimal scale, DateTime kind, the sign of a zero), byte arrays by content.</summary>
	public static void SameRows<T>(IReadOnlyList<T> runtime, IReadOnlyList<T> generated)
	{
		Assert.Equal(runtime.Count, generated.Count);
		PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
		for (int i = 0; i < runtime.Count; i++)
		{
			foreach (PropertyInfo p in properties)
			{
				if (p.GetIndexParameters().Length != 0 || p.GetMethod is null) continue;

				object? expected = p.GetValue(runtime[i]);
				object? actual = p.GetValue(generated[i]);
				Assert.True(Same(expected, actual), $"row {i}, {p.Name}: runtime {Show(expected)}, generated {Show(actual)}");
			}
		}
	}

	static bool Same(object? a, object? b) => (a, b) switch
	{
		(byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
		(decimal x, decimal y) => decimal.GetBits(x).AsSpan().SequenceEqual(decimal.GetBits(y)),
		(DateTime x, DateTime y) => x.Ticks == y.Ticks && x.Kind == y.Kind,
		(double x, double y) => BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y),
		(float x, float y) => BitConverter.SingleToInt32Bits(x) == BitConverter.SingleToInt32Bits(y),
		_ => Equals(a, b),
	};

	/// <summary>Both threw: the same type and message, the same inner exception (type and message), and for a mapping error the same member, column and type.</summary>
	public static void SameError(Exception? runtime, Exception? generated)
	{
		Assert.NotNull(runtime);
		Assert.NotNull(generated);
		Assert.Equal(runtime.GetType(), generated.GetType());
		Assert.Equal(runtime.Message, generated.Message);
		Assert.Equal(runtime.InnerException?.GetType(), generated.InnerException?.GetType());
		Assert.Equal(runtime.InnerException?.Message, generated.InnerException?.Message);
		if (runtime is ValueMappingException r)
		{
			var g = (ValueMappingException)generated;
			Assert.Equal(r.PropertyName, g.PropertyName);
			Assert.Equal(r.ColumnName, g.ColumnName);
			Assert.Equal(r.TargetType, g.TargetType);
		}
	}

	static string Show(object? value) => value switch
	{
		null => "null",
		byte[] bytes => "[" + string.Join(",", bytes) + "]",
		string s => "\"" + s + "\"",
		decimal d => d.ToString(System.Globalization.CultureInfo.InvariantCulture) + "m",
		DateTime t => t.ToString("O", System.Globalization.CultureInfo.InvariantCulture) + " " + t.Kind,
		IEnumerable e => "[" + string.Join(",", e.Cast<object?>()) + "]",
		_ => value.ToString() ?? "",
	};
}

/// <summary>
/// A reader that allows each cell once, in ascending column order, as a provider's SequentialAccess reader does: reading a column before the last one read throws.
/// IsDBNull and then a getter on the same column is allowed.
/// </summary>
internal sealed class SequentialReader(DbDataReader inner) : DbDataReader
{
	int _last = -1;

	void Touch(int ordinal)
	{
		if (ordinal < _last)
			throw new InvalidOperationException($"Sequential access: column {ordinal} was read after column {_last}.");
		_last = ordinal;
	}

	public override bool Read()
	{
		_last = -1;
		return inner.Read();
	}

	public override bool NextResult()
	{
		_last = -1;
		return inner.NextResult();
	}

	public override int GetValues(object[] values)
	{
		Touch(0);
		int n = inner.GetValues(values);
		_last = inner.FieldCount - 1;
		return n;
	}

	public override bool IsDBNull(int ordinal)
	{
		Touch(ordinal);
		return inner.IsDBNull(ordinal);
	}

	public override object GetValue(int ordinal)
	{
		Touch(ordinal);
		return inner.GetValue(ordinal);
	}

	public override bool GetBoolean(int ordinal)
	{
		Touch(ordinal);
		return inner.GetBoolean(ordinal);
	}

	public override byte GetByte(int ordinal)
	{
		Touch(ordinal);
		return inner.GetByte(ordinal);
	}

	public override char GetChar(int ordinal)
	{
		Touch(ordinal);
		return inner.GetChar(ordinal);
	}

	public override DateTime GetDateTime(int ordinal)
	{
		Touch(ordinal);
		return inner.GetDateTime(ordinal);
	}

	public override decimal GetDecimal(int ordinal)
	{
		Touch(ordinal);
		return inner.GetDecimal(ordinal);
	}

	public override double GetDouble(int ordinal)
	{
		Touch(ordinal);
		return inner.GetDouble(ordinal);
	}

	public override float GetFloat(int ordinal)
	{
		Touch(ordinal);
		return inner.GetFloat(ordinal);
	}

	public override Guid GetGuid(int ordinal)
	{
		Touch(ordinal);
		return inner.GetGuid(ordinal);
	}

	public override short GetInt16(int ordinal)
	{
		Touch(ordinal);
		return inner.GetInt16(ordinal);
	}

	public override int GetInt32(int ordinal)
	{
		Touch(ordinal);
		return inner.GetInt32(ordinal);
	}

	public override long GetInt64(int ordinal)
	{
		Touch(ordinal);
		return inner.GetInt64(ordinal);
	}

	public override string GetString(int ordinal)
	{
		Touch(ordinal);
		return inner.GetString(ordinal);
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		Touch(ordinal);
		return inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		Touch(ordinal);
		return inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	public override object this[int ordinal] => GetValue(ordinal);

	public override object this[string name] => GetValue(inner.GetOrdinal(name));

	public override int Depth => inner.Depth;

	public override int FieldCount => inner.FieldCount;

	public override bool HasRows => inner.HasRows;

	public override bool IsClosed => inner.IsClosed;

	public override int RecordsAffected => inner.RecordsAffected;

	public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);

	public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);

	public override string GetName(int ordinal) => inner.GetName(ordinal);

	public override int GetOrdinal(string name) => inner.GetOrdinal(name);

	public override IEnumerator GetEnumerator() => ((IEnumerable)inner).GetEnumerator();

	protected override void Dispose(bool disposing)
	{
		if (disposing) inner.Dispose();
		base.Dispose(disposing);
	}
}
