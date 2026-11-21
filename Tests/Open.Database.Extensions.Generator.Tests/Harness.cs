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
internal sealed class TableQuery : IExecuteReader
{
	readonly DataTable _table;

	public TableQuery(Col[] columns, params object?[][] rows)
	{
		_table = new DataTable();
		foreach (Col c in columns) _table.Columns.Add(new DataColumn(c.Name, c.Type));
		foreach (object?[] row in rows)
		{
			object[] values = new object[row.Length];
			for (int i = 0; i < row.Length; i++) values[i] = row[i] ?? DBNull.Value;
			_table.Rows.Add(values);
		}
	}

	/// <summary>A reader over the table, for the <see cref="IDataReader"/> receiver.</summary>
	public DataTableReader OpenReader() => _table.CreateDataReader();

	/// <summary>How many readers were opened.</summary>
	public int Executions { get; private set; }

	public CancellationToken CancellationToken => default;

	DataTableReader Open()
	{
		Executions++;
		return _table.CreateDataReader();
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
internal readonly record struct Counts(long Intercepted, long Served, long Declined, long RowsServed, long RowsDeclined)
{
	public static Counts Now() => new(
		Interlocked.Read(ref OdxGeneratedCounters.Intercepted),
		Interlocked.Read(ref OdxGeneratedCounters.Served),
		Interlocked.Read(ref OdxGeneratedCounters.Declined),
		Interlocked.Read(ref OdxGeneratedCounters.RowsServed),
		Interlocked.Read(ref OdxGeneratedCounters.RowsDeclined));

	public Counts Since(Counts start) => new(
		Intercepted - start.Intercepted,
		Served - start.Served,
		Declined - start.Declined,
		RowsServed - start.RowsServed,
		RowsDeclined - start.RowsDeclined);
}

/// <summary>The parity checks: the generated path against the runtime map, row by row and member by member, or exception against exception.</summary>
internal static class Parity
{
	/// <summary>Every row equal, every public property of each equal (byte arrays by content).</summary>
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
				bool equal = expected is byte[] a && actual is byte[] b
					? a.AsSpan().SequenceEqual(b)
					: Equals(expected, actual);
				Assert.True(equal, $"row {i}, {p.Name}: runtime {Show(expected)}, generated {Show(actual)}");
			}
		}
	}

	/// <summary>Both threw: the same type, message, and for a mapping error the same member, column and type.</summary>
	public static void SameError(Exception? runtime, Exception? generated)
	{
		Assert.NotNull(runtime);
		Assert.NotNull(generated);
		Assert.Equal(runtime.GetType(), generated.GetType());
		Assert.Equal(runtime.Message, generated.Message);
		if (runtime is ValueMappingException r)
		{
			var g = (ValueMappingException)generated;
			Assert.Equal(r.PropertyName, g.PropertyName);
			Assert.Equal(r.ColumnName, g.ColumnName);
			Assert.Equal(r.TargetType, g.TargetType);
			Assert.Equal(r.InnerException?.GetType(), g.InnerException?.GetType());
		}
	}

	static string Show(object? value) => value switch
	{
		null => "null",
		byte[] bytes => "[" + string.Join(",", bytes) + "]",
		string s => "\"" + s + "\"",
		IEnumerable e => "[" + string.Join(",", e.Cast<object?>()) + "]",
		_ => value.ToString() ?? "",
	};
}
