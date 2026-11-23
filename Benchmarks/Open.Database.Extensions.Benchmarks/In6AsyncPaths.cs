using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;

namespace Open.Database.Extensions.Benchmarks;

// BS-SA, IN6: the two hand-written "truly async" read paths (ODX does not need to support them).
//   P4  ExecuteReaderAsync (default behavior), ReadAsync, and EVERY cell read with await reader.GetFieldValueAsync<T>(i);
//       IsDBNullAsync only for the columns that can be NULL.
//   P5  the same with CommandBehavior.SequentialAccess, the cells read strictly in ordinal order (P4 reads them in ordinal order too).
// Both build the same objects as the ODX paths (SynRow, In6BigRow, In6LiteRow); the probe checks that on 5 rows before it measures.

/// <summary>One column of a row type, read with the async getters.</summary>
public abstract class AsyncColumn<TRow>
{
	public abstract ValueTask ReadAsync(DbDataReader reader, TRow row);

	/// <summary>The same cell with the SYNC typed getter (P6); the time inside the reader calls is added to getterTicks.</summary>
	public abstract void ReadSync(DbDataReader reader, TRow row, ref long getterTicks);
}

/// <summary>A column read with GetFieldValueAsync&lt;TValue&gt;; when it can be NULL, IsDBNullAsync is awaited first and setNull is called for NULL.</summary>
sealed class AsyncColumn<TRow, TValue>(int ordinal, Action<TRow, TValue> set, Action<TRow>? setNull) : AsyncColumn<TRow>
{
	public override async ValueTask ReadAsync(DbDataReader reader, TRow row)
	{
		if (setNull is not null && await reader.IsDBNullAsync(ordinal).ConfigureAwait(false))
		{
			setNull(row);
			return;
		}

		set(row, await reader.GetFieldValueAsync<TValue>(ordinal).ConfigureAwait(false));
	}

	static readonly Func<DbDataReader, int, TValue> Get = In6Plans.TypedGetter<TValue>();

	public override void ReadSync(DbDataReader reader, TRow row, ref long getterTicks)
	{
		long start = Stopwatch.GetTimestamp();
		if (setNull is not null && reader.IsDBNull(ordinal))
		{
			getterTicks += Stopwatch.GetTimestamp() - start;
			setNull(row);
			return;
		}

		var value = Get(reader, ordinal);
		getterTicks += Stopwatch.GetTimestamp() - start;
		set(row, value);
	}
}

/// <summary>How to read one scenario's rows with the async getters: how to make a row and the columns in ordinal order.</summary>
public sealed class AsyncPlan<TRow>(Func<TRow> create, AsyncColumn<TRow>[] columns)
{
	/// <summary>The hand-written Map(Func&lt;IDataRecord, TRow&gt;) selector for the selector+Read and selector+EnableAsyncRead paths (S2 only), or null.</summary>
	public Func<IDataRecord, TRow>? Selector { get; init; }

	/// <summary>Reads the rows (at most maxRows) with ExecuteReaderAsync, ReadAsync and the async getters; drains the other result sets when it read them all.</summary>
	public async Task<List<TRow>> ReadAllAsync(DbConnection connection, string sql, CommandBehavior behavior, int maxRows = int.MaxValue)
	{
		await using var command = connection.CreateCommand();
		command.CommandText = sql;
		await using var reader = await command.ExecuteReaderAsync(behavior).ConfigureAwait(false);
		var list = new List<TRow>();
		while (list.Count < maxRows && await reader.ReadAsync().ConfigureAwait(false))
		{
			var row = create();
			foreach (var column in columns) await column.ReadAsync(reader, row).ConfigureAwait(false);
			list.Add(row);
		}

		if (maxRows == int.MaxValue)
		{
			while (await reader.NextResultAsync().ConfigureAwait(false)) { }
		}

		return list;
	}

	/// <summary>
	/// P6: the provider's own connection and command (no ODX, no timing wrapper), ExecuteReaderAsync with the default behavior, <c>await reader.ReadAsync()</c> per row, then SYNC typed getters.
	/// Timed inline with Stopwatch, written to stats: the time awaiting every ReadAsync (from before the call to after the await, including a truly async completion),
	/// the time in the getters, and the ReadAsync calls whose task was not complete when checked (nothing else is added to the call). The wait of an incomplete read is that same await time.
	/// </summary>
	public async Task<List<TRow>> ReadAllRawAsync(DbConnection connection, string sql, In6Stats stats, int maxRows = int.MaxValue)
	{
		await using var command = connection.CreateCommand();
		command.CommandText = sql;
		long openStart = Stopwatch.GetTimestamp();
		var reader = await command.ExecuteReaderAsync(CommandBehavior.Default).ConfigureAwait(false);
		var list = new List<TRow>();
		long getterTicks = 0;
		try
		{
			while (list.Count < maxRows)
			{
				long start = Stopwatch.GetTimestamp();
				var task = reader.ReadAsync();
				bool incomplete = !task.IsCompleted;
				bool more = await task.ConfigureAwait(false);
				long waited = Stopwatch.GetTimestamp() - start;
				stats.ReadAsyncCalls++;
				stats.AwaitReadTicks += waited;
				if (waited > stats.MaxAwaitReadTicks) stats.MaxAwaitReadTicks = waited;
				if (incomplete)
				{
					stats.ReadAsyncIncomplete++;
					stats.WaitTicks += waited;
					if (waited > stats.MaxWaitTicks) stats.MaxWaitTicks = waited;
					stats.Positions?.Add(list.Count);
				}

				if (!more) break;
				var row = create();
				foreach (var column in columns) column.ReadSync(reader, row, ref getterTicks);
				list.Add(row);
			}

			if (maxRows == int.MaxValue)
			{
				while (await reader.NextResultAsync().ConfigureAwait(false)) { }
			}
		}
		finally
		{
			await reader.DisposeAsync().ConfigureAwait(false);
		}

		stats.GetterTicks += getterTicks;
		stats.OpenTicks += Stopwatch.GetTimestamp() - openStart;
		stats.Readers++;
		return list;
	}
}

/// <summary>The plans of the probe's scenarios.</summary>
static class In6Plans
{
	/// <summary>The sync typed getter for a column type (P6): GetInt32, GetString and so on; byte[] through GetValue.</summary>
	static object Of<T>(Func<DbDataReader, int, T> getter) => getter;

	internal static Func<DbDataReader, int, TValue> TypedGetter<TValue>()
	{
		object getter = default(TValue) switch
		{
			int => Of<int>(static (r, o) => r.GetInt32(o)),
			long => Of<long>(static (r, o) => r.GetInt64(o)),
			double => Of<double>(static (r, o) => r.GetDouble(o)),
			bool => Of<bool>(static (r, o) => r.GetBoolean(o)),
			decimal => Of<decimal>(static (r, o) => r.GetDecimal(o)),
			DateTime => Of<DateTime>(static (r, o) => r.GetDateTime(o)),
			Guid => Of<Guid>(static (r, o) => r.GetGuid(o)),
			_ when typeof(TValue) == typeof(string) => Of<string>(static (r, o) => r.GetString(o)),
			_ when typeof(TValue) == typeof(byte[]) => Of<byte[]>(static (r, o) => (byte[])r.GetValue(o)),
			_ => throw new NotSupportedException("no typed getter for " + typeof(TValue)),
		};
		return (Func<DbDataReader, int, TValue>)getter;
	}

	static AsyncColumn<TRow> Col<TRow, TValue>(int ordinal, Action<TRow, TValue> set) => new AsyncColumn<TRow, TValue>(ordinal, set, null);
	static AsyncColumn<TRow> Null<TRow, TValue>(int ordinal, Action<TRow, TValue> set, Action<TRow> setNull) => new AsyncColumn<TRow, TValue>(ordinal, set, setNull);

	/// <summary>The catalog row (SynRow) as SQL Server delivers it: the first width columns.</summary>
	public static AsyncPlan<SynRow> SynRowPlan(int width)
		=> new(static () => new SynRow(), [.. SynRowColumns().Take(width)]) { Selector = width == 20 ? SyntheticVariants.Hand(20) : null };

	static AsyncColumn<SynRow>[] SynRowColumns() =>
	[
		Col<SynRow, int>(0, static (r, v) => r.Id = v),
		Col<SynRow, DateTime>(1, static (r, v) => r.Created = v),
		Col<SynRow, string>(2, static (r, v) => r.Title = v),
		Col<SynRow, decimal>(3, static (r, v) => r.Price = v),
		Null<SynRow, int>(4, static (r, v) => r.Qty = v, static r => r.Qty = null),
		Col<SynRow, bool>(5, static (r, v) => r.IsActive = v),
		Col<SynRow, Guid>(6, static (r, v) => r.Key = v),
		Col<SynRow, double>(7, static (r, v) => r.Score = v),
		Null<SynRow, string>(8, static (r, v) => r.Note = v, static r => r.Note = null),
		Col<SynRow, byte[]>(9, static (r, v) => r.Blob = v),
		Col<SynRow, long>(10, static (r, v) => r.Big = v),
		Col<SynRow, string>(11, static (r, v) => r.Body = v),
		Null<SynRow, decimal>(12, static (r, v) => r.Amount = v, static r => r.Amount = null),
		Null<SynRow, DateTime>(13, static (r, v) => r.Stamp = v, static r => r.Stamp = null),
		Col<SynRow, string>(14, static (r, v) => r.Tag = v),
		Col<SynRow, double>(15, static (r, v) => r.Ratio = v),
		Null<SynRow, bool>(16, static (r, v) => r.Flag = v, static r => r.Flag = null),
		Null<SynRow, Guid>(17, static (r, v) => r.Link = v, static r => r.Link = null),
		Col<SynRow, long>(18, static (r, v) => r.Count = v),
		Null<SynRow, string>(19, static (r, v) => r.Summary = v, static r => r.Summary = null),
	];

	/// <summary>Id and the large string.</summary>
	public static AsyncPlan<In6BigRow> BigPlan()
		=> new(static () => new In6BigRow(), [Col<In6BigRow, int>(0, static (r, v) => r.Id = v), Col<In6BigRow, string>(1, static (r, v) => r.Body = v)]);

	/// <summary>The catalog row as SQLite delivers it (the Guid columns are text, so strings): the first width columns.</summary>
	public static AsyncPlan<In6LiteRow> LitePlan(int width)
		=> new(static () => new In6LiteRow(), [.. LiteColumns().Take(width)]);

	static AsyncColumn<In6LiteRow>[] LiteColumns() =>
	[
		Col<In6LiteRow, int>(0, static (r, v) => r.Id = v),
		Col<In6LiteRow, DateTime>(1, static (r, v) => r.Created = v),
		Col<In6LiteRow, string>(2, static (r, v) => r.Title = v),
		Col<In6LiteRow, decimal>(3, static (r, v) => r.Price = v),
		Null<In6LiteRow, int>(4, static (r, v) => r.Qty = v, static r => r.Qty = null),
		Col<In6LiteRow, bool>(5, static (r, v) => r.IsActive = v),
		Col<In6LiteRow, string>(6, static (r, v) => r.Key = v),
		Col<In6LiteRow, double>(7, static (r, v) => r.Score = v),
		Null<In6LiteRow, string>(8, static (r, v) => r.Note = v, static r => r.Note = null),
		Col<In6LiteRow, byte[]>(9, static (r, v) => r.Blob = v),
		Col<In6LiteRow, long>(10, static (r, v) => r.Big = v),
		Col<In6LiteRow, string>(11, static (r, v) => r.Body = v),
		Null<In6LiteRow, decimal>(12, static (r, v) => r.Amount = v, static r => r.Amount = null),
		Null<In6LiteRow, DateTime>(13, static (r, v) => r.Stamp = v, static r => r.Stamp = null),
		Col<In6LiteRow, string>(14, static (r, v) => r.Tag = v),
		Col<In6LiteRow, double>(15, static (r, v) => r.Ratio = v),
		Null<In6LiteRow, bool>(16, static (r, v) => r.Flag = v, static r => r.Flag = null),
		Null<In6LiteRow, string>(17, static (r, v) => r.Link = v, static r => r.Link = null),
		Col<In6LiteRow, long>(18, static (r, v) => r.Count = v),
		Null<In6LiteRow, string>(19, static (r, v) => r.Summary = v, static r => r.Summary = null),
	];

	/// <summary>The first difference between two lists of rows (every public property compared; byte[] by content), or null when they are equal.</summary>
	public static string? FirstDifference<T>(IReadOnlyList<T> expected, IReadOnlyList<T> actual)
	{
		if (expected.Count != actual.Count) return $"{expected.Count} rows against {actual.Count}";
		var properties = typeof(T).GetProperties();
		for (int i = 0; i < expected.Count; i++)
		{
			foreach (var property in properties)
			{
				object? a = property.GetValue(expected[i]), b = property.GetValue(actual[i]);
				bool same = a is byte[] x && b is byte[] y ? x.AsSpan().SequenceEqual(y) : Equals(a, b);
				if (!same) return $"row {i}, {property.Name}: {Show(a)} against {Show(b)}";
			}
		}

		return null;
	}

	static string Show(object? value)
	{
		string text = value switch
		{
			null => "null",
			byte[] bytes => "byte[" + bytes.Length.ToString(CultureInfo.InvariantCulture) + "]",
			_ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
		};
		return text.Length > 40 ? text[..40] + "..." : text;
	}
}
