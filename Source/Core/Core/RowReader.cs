namespace Open.Database.Extensions.Core;

/// <summary>Supplies the per-row mapping delegate for the layout of a reader's current result set.</summary>
internal abstract class RowSource<T>
{
	/// <summary>The mapper for a <see cref="DbDataReader"/> (virtual calls; the fast path).</summary>
	public abstract Func<DbDataReader, T> Mapper(DbDataReader reader);

	/// <summary>The mapper for any other <see cref="IDataRecord"/>.</summary>
	public abstract Func<IDataRecord, T> Mapper(IDataRecord reader);
}

/// <summary>A caller-supplied projection (no reflection involved).</summary>
internal sealed class SelectorSource<T>(Func<IDataRecord, T> selector) : RowSource<T>
{
	// A Func<IDataRecord,T> is a Func<DbDataReader,T> by contravariance: no wrapper delegate needed.
	public override Func<DbDataReader, T> Mapper(DbDataReader reader) => selector;
	public override Func<IDataRecord, T> Mapper(IDataRecord reader) => selector;
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

	public static void Fill<T, TSink>(RowSource<T> source, IDataReader reader, int skip, int take, ref TSink sink)
		where TSink : struct, IRowSink<T>
	{
		if (reader is not DbDataReader db)
		{
			foreach (T item in Stream(source, reader, skip, take))
				sink.Add(item);
			return;
		}

		if (take == 0 || !SkipRows(db, skip) || !db.Read()) return;

		Func<DbDataReader, T> map = source.Mapper(db);
		int n = 0;
		do
		{
			sink.Add(map(db));
			if (++n == take) break;
		}
		while (db.Read());
	}

	// The two hottest terminals keep their own loops: with a tiny per-row projection the extra sink
	// indirection of Fill is measurable (benchmarked), so List and array are written out directly.
	public static List<T> ToList<T>(RowSource<T> source, IDataReader reader, int skip, int take)
	{
		if (reader is not DbDataReader db)
			return [.. Stream(source, reader, skip, take)];

		var list = new List<T>();
		if (take == 0 || !SkipRows(db, skip) || !db.Read()) return list;

		Func<DbDataReader, T> map = source.Mapper(db);
		int n = 0;
		do
		{
			list.Add(map(db));
			if (++n == take) break;
		}
		while (db.Read());
		return list;
	}

	public static T[] ToArray<T>(RowSource<T> source, IDataReader reader, int skip, int take)
	{
		if (reader is not DbDataReader db)
			return Stream(source, reader, skip, take).ToArray();

		if (take == 0 || !SkipRows(db, skip) || !db.Read()) return [];

		Func<DbDataReader, T> map = source.Mapper(db);
		var buffer = new ArraySink<T>();
		try
		{
			int n = 0;
			do
			{
				buffer.Add(map(db));
				if (++n == take) break;
			}
			while (db.Read());
			return buffer.ToArray();
		}
		finally
		{
			buffer.Dispose();
		}
	}

	public static bool TryFirst<T>(RowSource<T> source, IDataReader reader, int skip, int take, out T? item)
	{
		if (reader is DbDataReader db)
		{
			if (take != 0 && SkipRows(db, skip) && db.Read())
			{
				item = source.Mapper(db)(db);
				return true;
			}

			item = default;
			return false;
		}

		foreach (T i in Stream(source, reader, skip, take))
		{
			item = i;
			return true;
		}

		item = default;
		return false;
	}

	public static bool TrySingle<T>(RowSource<T> source, IDataReader reader, int skip, int take, out T? item)
	{
		if (reader is DbDataReader db)
		{
			if (take == 0 || !SkipRows(db, skip) || !db.Read())
			{
				item = default;
				return false;
			}

			item = source.Mapper(db)(db);
			if (take != 1 && db.Read()) throw MoreThanOne();
			return true;
		}

		item = default;
		bool found = false;
		foreach (T i in Stream(source, reader, skip, take))
		{
			if (found) throw MoreThanOne();
			item = i;
			found = true;
		}

		return found;
	}

	/// <summary>A lazy stream. The reader must stay open while it is enumerated.</summary>
	public static IEnumerable<T> Stream<T>(RowSource<T> source, IDataReader reader, int skip, int take)
	{
		if (take == 0) yield break;
		for (; skip > 0; skip--)
			if (!reader.Read()) yield break;

		Func<IDataRecord, T>? map = null;
		int n = 0;
		while (reader.Read())
		{
			map ??= source.Mapper(reader);
			yield return map(reader);
			if (++n == take) yield break;
		}
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
	public static async ValueTask<TSink> FillAsync<T, TSink>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TSink sink, CancellationToken cancellationToken)
		where TSink : struct, IRowSink<T>
	{
		if (take == 0 || !await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
			return sink;

		Func<DbDataReader, T> map = source.Mapper(reader);
		int n = 0;
		do
		{
			sink.Add(map(reader));
			if (++n == take) break;
		}
		while (await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));
		return sink;
	}

	public static async ValueTask<List<T>> ToListAsync<T>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
		=> (await FillAsync(source, reader, skip, take, useReadAsync, new ListSink<T>(new List<T>()), cancellationToken).ConfigureAwait(false)).List;

	public static async ValueTask<T[]> ToArrayAsync<T>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
	{
		var sink = new ArraySink<T>();
		try
		{
			sink = await FillAsync(source, reader, skip, take, useReadAsync, sink, cancellationToken).ConfigureAwait(false);
			return sink.ToArray();
		}
		finally
		{
			sink.Dispose();
		}
	}

	public static async ValueTask<(bool Found, T? Item)> TryFirstAsync<T>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (take == 0
			|| !await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
			return (false, default);

		return (true, source.Mapper(reader)(reader));
	}

	public static async ValueTask<(bool Found, T? Item)> TrySingleAsync<T>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (take == 0
			|| !await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
			return (false, default);

		T item = source.Mapper(reader)(reader);
		if (take != 1 && await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)) throw MoreThanOne();
		return (true, item);
	}

	/// <summary>A lazy async stream. The reader must stay open while it is enumerated.</summary>
	public static async IAsyncEnumerable<T> StreamAsync<T>(
		RowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		if (take == 0) yield break;
		if (!await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)) yield break;

		Func<DbDataReader, T>? map = null;
		int n = 0;
		while (await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			map ??= source.Mapper(reader);
			yield return map(reader);
			if (++n == take) yield break;
		}
	}

	static ValueTask<bool> SkipRowsAsync(DbDataReader reader, int skip, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (skip <= 0) return new ValueTask<bool>(true);
		return Core(reader, skip, useReadAsync, cancellationToken);

		static async ValueTask<bool> Core(DbDataReader reader, int skip, bool useReadAsync, CancellationToken cancellationToken)
		{
			for (; skip > 0; skip--)
				if (!await ReadAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
					return false;
			return true;
		}
	}

	// With useReadAsync == false only the execution is async: rows are pulled with Read() (still cancellable between rows).
	static ValueTask<bool> ReadAsync(DbDataReader reader, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (useReadAsync) return new ValueTask<bool>(reader.ReadAsync(cancellationToken));
		cancellationToken.ThrowIfCancellationRequested();
		return new ValueTask<bool>(reader.Read());
	}

	#endregion
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

	T[] Grow(T[] items)
	{
		T[] bigger = ArrayPool<T>.Shared.Rent(items.Length * 2);
		Array.Copy(items, bigger, _count);
		ArrayPool<T>.Shared.Return(items, clearArray: true);
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
		ArrayPool<T>.Shared.Return(items, clearArray: true);
	}
}
