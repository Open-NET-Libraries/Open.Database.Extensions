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

	public static void Fill<T, TSink>(IRowSource<T> source, IDataReader reader, int skip, int take, ref TSink sink, CancellationToken token = default)
		where TSink : struct, IRowSink<T>
	{
		if (reader is not DbDataReader db)
		{
			foreach (T item in Stream(source, reader, skip, take, false, token))
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
	public static List<T> ToList<T>(IRowSource<T> source, IDataReader reader, int skip, int take, CancellationToken token = default)
	{
		if (reader is not DbDataReader db)
			return [.. Stream(source, reader, skip, take, false, token)];

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

	// Buffers mapped rows in a queue so a caller can drain it: each item is released to the GC as it is dequeued.
	// The rows are collected in a chain of pooled arrays and the queue is made from the exact count in one block copy (no growth copies, no slack).
	public static Queue<T> ToQueue<T>(IRowSource<T> source, IDataReader reader, int skip, int take)
		=> Collect<T, QueueBuild<T>, Queue<T>>(
			source, reader, skip, take, default);

	// The collecting terminals' one sync shape: the rows go into a chain of
	// pooled arrays, then build makes the result from the chain (the copy
	// happens right after the last row), and the chain is disposed however
	// the call ends.
	public static TResult Collect<T, TBuild, TResult>(
		IRowSource<T> source, IDataReader reader, int skip, int take,
		TBuild build, CancellationToken token = default)
		where TBuild : struct, IChainBuild<T, TResult>
	{
		using ChunkChain<T> chain = ChunkChain<T>.Rent();
		var sink = new ChainSink<T>(chain);
		Fill(source, reader, skip, take, ref sink, token);
		return build.Build(chain);
	}

	public static T[] ToArray<T>(IRowSource<T> source, IDataReader reader, int skip, int take, CancellationToken token = default)
	{
		if (reader is not DbDataReader db)
			return Stream(source, reader, skip, take, false, token).ToArray();

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

	public static bool TryFirst<T>(IRowSource<T> source, IDataReader reader, int skip, int take, out T? item, CancellationToken token = default)
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

		foreach (T i in Stream(source, reader, skip, take, false, token))
		{
			item = i;
			return true;
		}

		item = default;
		return false;
	}

	public static bool TrySingle<T>(IRowSource<T> source, IDataReader reader, int skip, int take, out T? item, CancellationToken token = default)
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
		foreach (T i in Stream(source, reader, skip, take, false, token))
		{
			if (found) throw MoreThanOne();
			item = i;
			found = true;
		}

		return found;
	}

	/// <summary>
	/// A lazy stream. The reader must stay open while it is enumerated.
	/// With <paramref name="readThrough"/>, once the rows end (including no rows, a take limit or a skip past the end) the later result sets are read through,
	/// so a failure in one is thrown. An enumeration the caller abandons, or whose consumer throws, never reaches that point.
	/// </summary>
	public static IEnumerable<T> Stream<T>(IRowSource<T> source, IDataReader reader, int skip, int take, bool readThrough = false, CancellationToken token = default)
	{
		bool skipped = take != 0;
		if (skipped)
		{
			for (; skip > 0; skip--)
			{
				token.ThrowIfCancellationRequested();
				if (reader.Read()) continue;
				skipped = false;
				break;
			}
		}

		if (skipped)
		{
			int n = 0;
			if (reader is DbDataReader db)
			{
				// The virtual-call mapper, not the interface-call one (a second compile per layout otherwise).
				Func<DbDataReader, T>? dbMap = null;
				while (db.Read())
				{
					dbMap ??= source.Mapper(db);
					yield return dbMap(db);
					if (++n == take) break;
				}
			}
			else
			{
				Func<IDataRecord, T>? map = null;
				do
				{
					token.ThrowIfCancellationRequested();
					if (!reader.Read()) break;
					map ??= source.Mapper(reader);
					yield return map(reader);
				}
				while (++n != take);
			}
		}

		if (readThrough) ReadThrough(reader, token);
	}

	// Moves past every later result set without reading its rows; the provider raises a failure in one of them here.
	// A canceled token throws before each NextResult;
	// once one returns false the read is done.
	internal static void ReadThrough(
		IDataReader reader,
		CancellationToken token = default)
	{
		do
		{
			token.ThrowIfCancellationRequested();
		}
		while (reader.NextResult());
	}

	static bool SkipRows(DbDataReader reader, int skip)
	{
		for (; skip > 0; skip--)
			if (!reader.Read()) return false;
		return true;
	}

	static InvalidOperationException MoreThanOne() => new("Sequence contains more than one element.");

	// For the throwing terminals (First, Single) when the query returned no row; thrown after the reader is cleaned up.
	internal static InvalidOperationException NoRows(string terminal)
		=> new($"Sequence contains no elements: the query returned no rows, and {terminal} needs one.");

	#endregion

	#region Asynchronous

	// The sink is passed and returned by value because ref parameters can't cross an await.
	public static async ValueTask<TSink> FillAsync<T, TSink>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, TSink sink, CancellationToken cancellationToken)
		where TSink : struct, IRowSink<T>
	{
		if (take == 0
			|| !await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			|| !await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			return sink;
		}

		Func<DbDataReader, T> map = source.Mapper(reader);
		int n = 0;
		do
		{
			sink.Add(map(reader));
			if (++n == take) break;
		}
		while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));
		return sink;
	}

	public static ValueTask<Queue<T>> ToQueueAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
		=> CollectAsync<T, QueueBuild<T>, Queue<T>>(
			source, reader, skip, take, useReadAsync, default,
			cancellationToken);

	// ONE async method for every collecting terminal (ToList, ToArray, ToQueue,
	// ToHashSet, ToDictionary): the read loop and the build share a single
	// state machine, so a read that really suspends boxes one machine, not a
	// wrapper around another. The rows go into a chain of pooled arrays; build
	// (a struct, so what it carries rides in the machine, not in a delegate
	// and a state object) makes the result from the chain right after the
	// last row.
	public static async ValueTask<TResult> CollectAsync<T, TBuild, TResult>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync,
		TBuild build, CancellationToken cancellationToken)
		where TBuild : struct, IChainBuild<T, TResult>
	{
		using ChunkChain<T> chain = ChunkChain<T>.Rent();
		if (take != 0
			&& await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			&& await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			Func<DbDataReader, T> map = source.Mapper(reader);
			int n = 0;
			do
			{
				chain.Add(map(reader));
				if (++n == take) break;
			}
			while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));
		}

		return build.Build(chain);
	}

	public static async ValueTask<(bool Found, T? Item)> TryFirstAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
		=> take != 0
		&& await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
		&& await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)
		? (true, source.Mapper(reader)(reader))
		: (false, default);

	public static async ValueTask<(bool Found, T? Item)> TrySingleAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (take != 0
			&& await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			&& await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			T item = source.Mapper(reader)(reader);
			if (take != 1 && await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)) throw MoreThanOne();
			return (true, item);
		}

		return (false, default);
	}

	/// <summary>
	/// A lazy async stream. The reader must stay open while it is enumerated.
	/// With <paramref name="readThrough"/>, once the rows end the later result sets are read through (see <see cref="Stream{T}"/>).
	/// </summary>
	public static async IAsyncEnumerable<T> StreamAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, bool readThrough,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		if (take != 0 && await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			Func<DbDataReader, T>? map = null;
			int n = 0;
			while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
			{
				map ??= source.Mapper(reader);
				yield return map(reader);
				if (++n == take) break;
			}
		}

		if (readThrough) await ReadThroughAsync(reader, cancellationToken).ConfigureAwait(false);
	}

	internal static ValueTask ReadThroughAsync(IDataReader reader, CancellationToken token)
	{
		if (reader is not DbDataReader db)
		{
			ReadThrough(reader, token);
			return default;
		}

		return Core(db, token);

		static async ValueTask Core(DbDataReader db, CancellationToken token)
		{
			while (await db.NextResultAsync(token).ConfigureAwait(false)) { }
		}
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

internal readonly struct ListSink<T>(List<T> list) : IRowSink<T>
{
	public List<T> List { get; } = list;
	public void Add(T item) => List.Add(item);
}

/// <summary>
/// Makes the result of a collecting terminal from the rows in a chain.
/// Implemented by structs so the collecting methods are specialized per
/// result and carry no delegate or state object.
/// </summary>
internal interface IChainBuild<T, out TResult>
{
	TResult Build(ChunkChain<T> chain);
}

internal readonly struct ListBuild<T> : IChainBuild<T, List<T>>
{
	public List<T> Build(ChunkChain<T> chain) => chain.ToList();
}

internal readonly struct ArrayBuild<T> : IChainBuild<T, T[]>
{
	public T[] Build(ChunkChain<T> chain) => chain.ToArray();
}

/// <summary>The array of ToArray, wrapped (not copied) as an immutable array.</summary>
internal readonly struct ImmutableArrayBuild<T> : IChainBuild<T, ImmutableArray<T>>
{
	public ImmutableArray<T> Build(ChunkChain<T> chain)
	{
		T[] array = chain.ToArray();
#if NET8_0_OR_GREATER
		return System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsImmutableArray(array);
#else
		return ImmutableArray.Create(array);
#endif
	}
}

internal readonly struct QueueBuild<T> : IChainBuild<T, Queue<T>>
{
	public Queue<T> Build(ChunkChain<T> chain) => chain.ToQueue();
}

internal readonly struct HashSetBuild<T>(IEqualityComparer<T>? comparer)
	: IChainBuild<T, HashSet<T>>
{
	public HashSet<T> Build(ChunkChain<T> chain)
		=> chain.ToHashSet(comparer);
}

/// <summary>A ToDictionary that keeps the row.</summary>
internal readonly struct DictionaryBuild<T, TKey>(
	Func<T, TKey> keySelector,
	IEqualityComparer<TKey>? comparer)
	: IChainBuild<T, Dictionary<TKey, T>>
	where TKey : notnull
{
	public Dictionary<TKey, T> Build(ChunkChain<T> chain)
		=> chain.ToDictionary(keySelector, comparer);
}

/// <summary>A ToDictionary with a value selector.</summary>
internal readonly struct DictionaryValueBuild<T, TKey, TValue>(
	Func<T, TKey> keySelector,
	Func<T, TValue> valueSelector,
	IEqualityComparer<TKey>? comparer)
	: IChainBuild<T, Dictionary<TKey, TValue>>
	where TKey : notnull
{
	public Dictionary<TKey, TValue> Build(ChunkChain<T> chain)
		=> chain.ToDictionary(keySelector, valueSelector, comparer);
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
