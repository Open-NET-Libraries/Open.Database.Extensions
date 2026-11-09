namespace Open.Database.Extensions.Core;

/// <summary>Supplies the per-row mapping delegate for the layout of a reader's current result set.</summary>
internal interface IRowSource<T>
{
	/// <summary>The mapper for a <see cref="DbDataReader"/> (virtual calls; the fast path).</summary>
	Func<DbDataReader, T> Mapper(DbDataReader reader);

	/// <summary>The mapper for any other <see cref="IDataRecord"/>.</summary>
	Func<IDataRecord, T> Mapper(IDataRecord reader);
}

/// <summary>A caller-supplied projection (no reflection involved).</summary>
internal sealed class SelectorSource<T>(Func<IDataRecord, T> selector) : IRowSource<T>
{
	// A Func<IDataRecord,T> is a Func<DbDataReader,T> by contravariance: no wrapper delegate needed.
	public Func<DbDataReader, T> Mapper(DbDataReader reader) => selector;
	public Func<IDataRecord, T> Mapper(IDataRecord reader) => selector;
}

/// <summary>
/// What a read loop does with the reader when the rows of the result set are over, so that the same loops serve a reader the terminal owns and one the caller does.
/// </summary>
internal interface IRowsEnd
{
	/// <summary>A read returned <see langword="false"/>: the result set ended by itself.</summary>
	void Natural(IDataReader reader);

	/// <inheritdoc cref="Natural"/>
	ValueTask NaturalAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken);

	/// <summary>The rows that were asked for have been read; the loop stops here without reading the rest.</summary>
	void Limit(IDataReader reader);

	/// <inheritdoc cref="Limit"/>
	ValueTask LimitAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken);
}

/// <summary>
/// The reader belongs to the terminal, which is finished with the command when the rows are. After the last row it calls <c>NextResult</c> once,
/// which is what makes a provider raise an error that follows the rows (a <c>RAISERROR</c> after the <c>SELECT</c>); a terminal that stops at a limit
/// reads one more row first and does that only if there was none, so it stays lazy when there are more rows.
/// </summary>
internal readonly struct OwnedReader : IRowsEnd
{
	public void Natural(IDataReader reader) => reader.NextResult();

	// With UseAsyncRead off only the execution is asynchronous and rows are read synchronously; so is this one call (no task is made for it).
	public ValueTask NaturalAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (useReadAsync) return new ValueTask(reader.NextResultAsync(cancellationToken));
		reader.NextResult();
		return default;
	}

	public void Limit(IDataReader reader)
	{
		if (!reader.Read()) reader.NextResult();
	}

	public ValueTask LimitAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (useReadAsync) return LimitCore(reader, cancellationToken);
		Limit(reader);
		return default;

		static async ValueTask LimitCore(DbDataReader reader, CancellationToken cancellationToken)
		{
			if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
				await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
		}
	}
}

/// <summary>The reader belongs to the caller, who may read the next result set: the loops leave it where they stopped.</summary>
internal readonly struct SharedReader : IRowsEnd
{
	public void Natural(IDataReader reader) { }
	public ValueTask NaturalAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken) => default;
	public void Limit(IDataReader reader) { }
	public ValueTask LimitAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken) => default;
}

/// <summary>Receives each mapped row. Implemented by structs so the read loops specialize and inline <see cref="Add"/>.</summary>
internal interface IRowSink<in T>
{
	void Add(T item);
}

/// <summary>
/// The read loops that turn a positioned reader into results. They call the mapper directly per row:
/// no iterator, no LINQ, no per-row allocation other than the mapped objects themselves.
/// Skipped rows are advanced without being mapped; a take limit stops reading as soon as it is satisfied.
/// What happens to each row is the sink's business, so one loop serves every collection type.
/// </summary>
internal static class RowReader
{
	#region Synchronous

	// Every loop takes the end policy (see IRowsEnd): OwnedReader moves the reader past the rows so that a provider error raised after them is thrown,
	// SharedReader leaves it where the loop stopped. Nothing but the rows of the first result set is ever read.

	public static void Fill<T, TSink, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end, ref TSink sink)
		where TSink : struct, IRowSink<T>
		where TEnd : struct, IRowsEnd
	{
		if (reader is not DbDataReader db)
		{
			foreach (T item in Stream(source, reader, skip, take, end))
				sink.Add(item);
			return;
		}

		if (take == 0) return;
		if (!SkipRows(db, skip) || !db.Read())
		{
			end.Natural(db);
			return;
		}

		Func<DbDataReader, T> map = source.Mapper(db);
		int n = 0;
		do
		{
			sink.Add(map(db));
			if (++n == take)
			{
				end.Limit(db);
				return;
			}
		}
		while (db.Read());

		end.Natural(db);
	}

	// The two hottest terminals keep their own loops: with a tiny per-row projection the extra sink
	// indirection of Fill is measurable (benchmarked), so List and array are written out directly.
	public static List<T> ToList<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end)
		where TEnd : struct, IRowsEnd
	{
		if (reader is not DbDataReader db)
			return [.. Stream(source, reader, skip, take, end)];

		var list = new List<T>();
		if (take == 0) return list;
		if (!SkipRows(db, skip) || !db.Read())
		{
			end.Natural(db);
			return list;
		}

		Func<DbDataReader, T> map = source.Mapper(db);
		int n = 0;
		do
		{
			list.Add(map(db));
			if (++n == take)
			{
				end.Limit(db);
				return list;
			}
		}
		while (db.Read());

		end.Natural(db);
		return list;
	}

	// Buffers mapped rows in a queue so a caller can drain it: each item is released to the GC as it is dequeued.
	public static Queue<T> ToQueue<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end)
		where TEnd : struct, IRowsEnd
	{
		var sink = new QueueSink<T>(new Queue<T>());
		Fill(source, reader, skip, take, end, ref sink);
		return sink.Queue;
	}

	public static T[] ToArray<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end)
		where TEnd : struct, IRowsEnd
	{
		if (reader is not DbDataReader db)
			return Stream(source, reader, skip, take, end).ToArray();

		if (take == 0) return [];
		if (!SkipRows(db, skip) || !db.Read())
		{
			end.Natural(db);
			return [];
		}

		Func<DbDataReader, T> map = source.Mapper(db);
		var buffer = new ArraySink<T>();
		try
		{
			int n = 0;
			do
			{
				buffer.Add(map(db));
				if (++n == take)
				{
					end.Limit(db);
					return buffer.ToArray();
				}
			}
			while (db.Read());

			end.Natural(db);
			return buffer.ToArray();
		}
		finally
		{
			buffer.Dispose();
		}
	}

	public static bool TryFirst<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end, out T? item)
		where TEnd : struct, IRowsEnd
	{
		if (reader is DbDataReader db)
		{
			if (take == 0)
			{
				item = default;
				return false;
			}

			if (!SkipRows(db, skip) || !db.Read())
			{
				end.Natural(db);
				item = default;
				return false;
			}

			// The row is mapped before the peek for another one: a sequential reader can't go back to it.
			item = source.Mapper(db)(db);
			end.Limit(db);
			return true;
		}

		foreach (T i in Stream(source, reader, skip, take, end))
		{
			item = i;
			end.Limit(reader);
			return true;
		}

		item = default;
		return false;
	}

	public static bool TrySingle<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end, out T? item)
		where TEnd : struct, IRowsEnd
	{
		if (reader is DbDataReader db)
		{
			if (take == 0)
			{
				item = default;
				return false;
			}

			if (!SkipRows(db, skip) || !db.Read())
			{
				end.Natural(db);
				item = default;
				return false;
			}

			item = source.Mapper(db)(db);
			if (take == 1)
			{
				end.Limit(db);
				return true;
			}

			// The second row is what tells "single" from "more than one": no row there is the natural end.
			if (db.Read()) throw MoreThanOne();
			end.Natural(db);
			return true;
		}

		item = default;
		bool found = false;
		foreach (T i in Stream(source, reader, skip, take, end))
		{
			if (found) throw MoreThanOne();
			item = i;
			found = true;
		}

		return found;
	}

	/// <summary>A lazy stream. The reader must stay open while it is enumerated. The end policy applies when the enumeration reaches the end.</summary>
	public static IEnumerable<T> Stream<T, TEnd>(IRowSource<T> source, IDataReader reader, int skip, int take, TEnd end)
		where TEnd : struct, IRowsEnd
	{
		if (take == 0) yield break;
		for (; skip > 0; skip--)
		{
			if (reader.Read()) continue;
			end.Natural(reader);
			yield break;
		}

		int n = 0;
		if (reader is DbDataReader db)
		{
			// The virtual-call mapper, not the interface-call one (a second compile per layout otherwise).
			Func<DbDataReader, T>? dbMap = null;
			while (db.Read())
			{
				dbMap ??= source.Mapper(db);
				yield return dbMap(db);
				if (++n != take) continue;
				end.Limit(db);
				yield break;
			}

			end.Natural(db);
			yield break;
		}

		Func<IDataRecord, T>? map = null;
		while (reader.Read())
		{
			map ??= source.Mapper(reader);
			yield return map(reader);
			if (++n != take) continue;
			end.Limit(reader);
			yield break;
		}

		end.Natural(reader);
	}

	static bool SkipRows(DbDataReader reader, int skip)
	{
		for (; skip > 0; skip--)
			if (!reader.Read()) return false;
		return true;
	}

	static InvalidOperationException MoreThanOne() => new("Sequence contains more than one element.");

	#endregion

	#region Asynchronous

	// The sink is passed and returned by value because ref parameters can't cross an await.
	public static async ValueTask<TSink> FillAsync<T, TSink, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, TSink sink, CancellationToken cancellationToken)
		where TSink : struct, IRowSink<T>
		where TEnd : struct, IRowsEnd
	{
		if (take == 0) return sink;
		if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			return sink;
		}

		Func<DbDataReader, T> map = source.Mapper(reader);
		int n = 0;
		do
		{
			sink.Add(map(reader));
			if (++n == take)
			{
				await end.LimitAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
				return sink;
			}
		}
		while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));

		await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
		return sink;
	}

	public static async ValueTask<List<T>> ToListAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
		=> (await FillAsync(source, reader, skip, take, useReadAsync, end, new ListSink<T>([]), cancellationToken).ConfigureAwait(false)).List;

	public static async ValueTask<Queue<T>> ToQueueAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
		=> (await FillAsync(source, reader, skip, take, useReadAsync, end, new QueueSink<T>(new Queue<T>()), cancellationToken).ConfigureAwait(false)).Queue;

	public static async ValueTask<T[]> ToArrayAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
	{
		// The sink is a struct that owns a rented buffer: it stays in this method (FillAsync would work on a copy, which is lost
		// when the read fails) so that the buffer goes back to the pool however the call ends.
		var sink = new ArraySink<T>();
		try
		{
			if (take == 0) return sink.ToArray();
			if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
				|| !await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
			{
				await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
				return sink.ToArray();
			}

			Func<DbDataReader, T> map = source.Mapper(reader);
			int n = 0;
			do
			{
				sink.Add(map(reader));
				if (++n == take)
				{
					await end.LimitAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
					return sink.ToArray();
				}
			}
			while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));

			await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			return sink.ToArray();
		}
		finally
		{
			sink.Dispose();
		}
	}

	public static async ValueTask<(bool Found, T? Item)> TryFirstAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
	{
		if (take == 0) return (false, default);
		if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			return (false, default);
		}

		// The row is mapped before the peek for another one: a sequential reader can't go back to it.
		T item = source.Mapper(reader)(reader);
		await end.LimitAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
		return (true, item);
	}

	public static async ValueTask<(bool Found, T? Item)> TrySingleAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
	{
		if (take == 0) return (false, default);
		if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			return (false, default);
		}

		T item = source.Mapper(reader)(reader);
		if (take == 1)
		{
			await end.LimitAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			return (true, item);
		}

		// The second row is what tells "single" from "more than one": no row there is the natural end.
		if (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)) throw MoreThanOne();
		await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
		return (true, item);
	}

	/// <summary>A lazy async stream. The reader must stay open while it is enumerated. The end policy applies when the enumeration reaches the end.</summary>
	public static async IAsyncEnumerable<T> StreamAsync<T, TEnd>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TEnd end,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
		where TEnd : struct, IRowsEnd
	{
		if (take == 0) yield break;
		if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			yield break;
		}

		Func<DbDataReader, T>? map = null;
		int n = 0;
		while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			map ??= source.Mapper(reader);
			yield return map(reader);
			if (++n != take) continue;
			await end.LimitAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
			yield break;
		}

		await end.NaturalAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false);
	}

	static ValueTask<bool> SkipRowsAsync(DbDataReader reader, int skip, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (skip <= 0) return new ValueTask<bool>(true);
		return Core(reader, skip, useReadAsync, cancellationToken);

		static async ValueTask<bool> Core(DbDataReader reader, int skip, bool useReadAsync, CancellationToken cancellationToken)
		{
			for (; skip > 0; skip--)
			{
				if (!await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
					return false;
			}

			return true;
		}
	}

	// With useReadAsync == false only the execution is async: rows are pulled with Read() (still cancellable between rows).
	static ValueTask<bool> ReadRowAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (useReadAsync) return new ValueTask<bool>(reader.ReadAsync(cancellationToken));
		cancellationToken.ThrowIfCancellationRequested();
		return new ValueTask<bool>(reader.Read());
	}

	#endregion
}

internal readonly struct QueueSink<T>(Queue<T> queue) : IRowSink<T>
{
	public Queue<T> Queue { get; } = queue;
	public void Add(T item) => Queue.Enqueue(item);
}

internal readonly struct ListSink<T>(List<T> list) : IRowSink<T>
{
	public List<T> List { get; } = list;
	public void Add(T item) => List.Add(item);
}

internal readonly struct DictionarySink<T, TKey>(Dictionary<TKey, T> dictionary, Func<T, TKey> keySelector) : IRowSink<T>
	where TKey : notnull
{
	public Dictionary<TKey, T> Dictionary { get; } = dictionary;
	public void Add(T item) => Dictionary.Add(keySelector(item), item);
}

internal readonly struct HashSetSink<T>(HashSet<T> set) : IRowSink<T>
{
	public HashSet<T> Set { get; } = set;
	public void Add(T item) => Set.Add(item);
}

/// <summary>
/// A pooled, growable buffer so an exact-size array can be produced without the garbage a growing list leaves behind.
/// </summary>
internal struct ArraySink<T> : IRowSink<T>
{
	T[]? _items;
	int _count;

	public void Add(T item)
	{
		T[]? items = _items;
		if (items is null)
			_items = items = ArrayPool<T>.Shared.Rent(16);
		else if (_count == items.Length)
			_items = items = Grow(items);

		items[_count++] = item;
	}

	private readonly T[] Grow(T[] items)
	{
		T[] bigger = ArrayPool<T>.Shared.Rent(items.Length * 2);
		Array.Copy(items, bigger, _count);
		Array.Clear(items, 0, _count);
		ArrayPool<T>.Shared.Return(items, clearArray: false);
		return bigger;
	}

	public readonly T[] ToArray()
	{
		if (_count == 0) return [];
		var result = new T[_count];
		Array.Copy(_items!, result, _count);
		return result;
	}

	public void Dispose()
	{
		T[]? items = _items;
		if (items is null) return;
		_items = null;
		Array.Clear(items, 0, _count);
		ArrayPool<T>.Shared.Return(items, clearArray: false);
	}
}
