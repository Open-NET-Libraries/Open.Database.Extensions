namespace Open.Database.Extensions.Core;

/// <summary>Supplies the per-row mapping delegate for the layout of a reader's current result set.</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
internal interface IRowSource<T>
{
	/// <summary>Returns the mapper for a <see cref="DbDataReader"/> (virtual calls; the fast path).</summary>
	/// <param name="reader">The reader positioned on a result set.</param>
	/// <returns>The delegate that maps the current row of <paramref name="reader"/>.</returns>
	Func<DbDataReader, T> Mapper(DbDataReader reader);

	/// <summary>Returns the mapper for any other <see cref="IDataRecord"/>.</summary>
	/// <param name="reader">The record positioned on a result set.</param>
	/// <returns>The delegate that maps the current row of <paramref name="reader"/>.</returns>
	Func<IDataRecord, T> Mapper(IDataRecord reader);
}

/// <summary>Represents a caller-supplied projection (no reflection involved).</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <param name="selector">The projection of a record to a row.</param>
internal sealed class SelectorSource<T>(Func<IDataRecord, T> selector) : IRowSource<T>
{
	/// <summary>
	/// Determines whether <paramref name="obj"/> is a source of an equal projection.
	/// </summary>
	/// <remarks>
	/// The sources are equal when the delegates are: a <see langword="static"/> lambda or a method group is, a capturing lambda is not (each call makes a new one).
	/// This is for a caller that keys on a query (the memoize seam); it never runs on the path that reads rows.
	/// </remarks>
	/// <param name="obj">The object to compare with this source.</param>
	/// <returns>
	/// <see langword="true"/> if <paramref name="obj"/> is a <see cref="SelectorSource{T}"/> whose delegate equals this one; otherwise, <see langword="false"/>.
	/// </returns>
	public override bool Equals(object? obj) => obj is SelectorSource<T> other && selector.Equals(other.Selector);

	/// <inheritdoc />
	public override int GetHashCode() => selector.GetHashCode();

	Func<IDataRecord, T> Selector => selector;

	/// <summary>Returns the projection as the mapper for a <see cref="DbDataReader"/>.</summary>
	/// <remarks>
	/// A <c>Func&lt;IDataRecord, T&gt;</c> is a <c>Func&lt;DbDataReader, T&gt;</c> by contravariance: no wrapper delegate is needed.
	/// </remarks>
	/// <param name="reader">The reader, which is not used.</param>
	/// <returns>The projection.</returns>
	public Func<DbDataReader, T> Mapper(DbDataReader reader) => selector;
	/// <summary>Returns the projection as the mapper for an <see cref="IDataRecord"/>.</summary>
	/// <param name="reader">The record, which is not used.</param>
	/// <returns>The projection.</returns>
	public Func<IDataRecord, T> Mapper(IDataRecord reader) => selector;
}

/// <summary>Receives each mapped row. Implemented by structs so the read loops specialize and inline <see cref="Add"/>.</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
internal interface IRowSink<in T>
{
	/// <summary>Receives one mapped row.</summary>
	/// <param name="item">The mapped row.</param>
	void Add(T item);
}

/// <summary>
/// Provides the read loops that turn a positioned reader into results.
/// </summary>
/// <remarks>
/// <para>
/// The loops call the mapper directly per row:
/// no iterator, no LINQ, no per-row allocation other than the mapped objects themselves.
/// Skipped rows are advanced without being mapped; a take limit stops reading as soon as it is satisfied.
/// What happens to each row is the sink's business, so one loop serves every collection type.
/// </para>
/// <para>
/// Two things hold for every loop here, and they are why the code looks the way it does:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// The command has <em>already</em> run when a loop starts: the caller opened the reader (that is what executes the command, and what
/// a <c>Take(0)</c> is still expected to do). A take of 0 therefore only means "read no rows": the loops return an empty result at once,
/// without skipping or mapping anything, and the caller disposes the reader (and reads through later result sets) as for any other call.
/// </description>
/// </item>
/// <item>
/// <description>
/// A reader that is not a <see cref="DbDataReader"/> (a bare <see cref="IDataReader"/>) takes the <see cref="Stream{T}"/> path:
/// it has no virtual-call fast path, so one lazy loop reads it, and that loop makes the same take-0 decision, so the result is the same.
/// </description>
/// </item>
/// </list>
/// </remarks>
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

/// <summary>
/// Reads the rows into a <see cref="List{T}"/>.
/// </summary>
/// <remarks>
/// The two hottest terminals (this one and <see cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}"/>) keep their own loops:
/// with a tiny per-row projection the extra sink indirection of <c>Fill</c> is measurable (benchmarked),
/// so the list and the array are written out directly.
/// </remarks>
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

/// <summary>
/// Reads the rows into a chain of pooled arrays and makes the result from it: the one sync shape of the collecting terminals.
/// </summary>
/// <remarks>
/// The arrays come from the pool of <paramref name="config"/> (the shared array pool by default).
/// <paramref name="build"/> makes the result from the chain (the copy happens right after the last row),
/// and the chain is disposed however the call ends.
/// <paramref name="config"/> says, among other things, what <see cref="PoolClearing"/> decided for this call.
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <typeparam name="TBuild">The struct that makes the result from the chain.</typeparam>
/// <typeparam name="TResult">The type of the result.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="build">Makes the result from the chain.</param>
/// <param name="config">The pool and the clearing rule of the chain.</param>
/// <param name="token">The token that cancels the read.</param>
/// <returns>The result <paramref name="build"/> made from the rows.</returns>
	public static TResult Collect<T, TBuild, TResult>(
		IRowSource<T> source, IDataReader reader, int skip, int take,
		TBuild build, ChunkConfig<T> config,
		CancellationToken token = default)
		where TBuild : struct, IChainBuild<T, TResult>
	{
		using var chain = ChunkChain<T>.Rent(config);
		var sink = new ChainSink<T>(chain);
		Fill(source, reader, skip, take, ref sink, token);
		return build.Build(chain);
	}

/// <summary>
/// Reads the rows into an array.
/// </summary>
/// <remarks>
/// Like <see cref="Synchronous.SyncMappedQueryExtensions.ToList{T}"/>, it keeps its own loop rather than using <c>Fill</c>.
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="config">The pool and the clearing rule of the buffer.</param>
/// <param name="token">The token that cancels the read.</param>
/// <returns>The rows, in an array that is as long as their count.</returns>
	public static T[] ToArray<T>(
		IRowSource<T> source, IDataReader reader, int skip, int take,
		ChunkConfig<T> config, CancellationToken token = default)
	{
		// A bare IDataReader has no DbDataReader fast path: Stream reads it (and
		// treats take 0 the same way as the line below), then ToArray copies.
		if (reader is not DbDataReader db)
			return Stream(source, reader, skip, take, false, token).ToArray();

		if (take == 0 || !SkipRows(db, skip) || !db.Read()) return [];

		Func<DbDataReader, T> map = source.Mapper(db);
		var buffer = new ArraySink<T>(config);
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
/// Returns a lazy stream of the mapped rows.
/// </summary>
/// <remarks>
/// The reader must stay open while the stream is enumerated.
/// With <paramref name="readThrough"/>, once the rows end (including no rows, a take limit or a skip past the end) the later result sets are read through,
/// so a failure in one is thrown. An enumeration the caller abandons, or whose consumer throws, never reaches that point.
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="readThrough"><see langword="true"/> to read through the later result sets once the rows end; otherwise, <see langword="false"/>. The default is <see langword="false"/>.</param>
/// <param name="token">The token that cancels the read.</param>
/// <returns>A lazy sequence of the mapped rows.</returns>
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

/// <summary>
/// Moves past every later result set without reading its rows.
/// </summary>
/// <remarks>
/// The provider raises a failure in one of them here.
/// A canceled token throws before each <c>NextResult</c>;
/// once one returns <see langword="false"/> the read is done.
/// </remarks>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="token">The token that cancels the read.</param>
/// <exception cref="OperationCanceledException"><paramref name="token"/> is canceled.</exception>
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

/// <summary>
/// Creates the exception the throwing terminals (<c>First</c>, <c>Single</c>) raise when the query returned no row.
/// </summary>
/// <remarks>
/// It is thrown after the reader is cleaned up.
/// </remarks>
/// <param name="terminal">The name of the terminal that needs a row.</param>
/// <returns>The exception to throw.</returns>
	internal static InvalidOperationException NoRows(string terminal)
		=> new($"Sequence contains no elements: the query returned no rows, and {terminal} needs one.");

	#endregion

	#region Asynchronous

/// <summary>
/// Reads the rows into a sink.
/// </summary>
/// <remarks>
/// The sink is passed and returned by value because <see langword="ref"/> parameters cannot cross an <see langword="await"/>.
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <typeparam name="TSink">The struct that receives the rows.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="useReadAsync">See <see cref="ReadRowAsync"/>.</param>
/// <param name="sink">The sink that receives the rows.</param>
/// <param name="cancellationToken">The token that cancels the read.</param>
/// <returns>A task whose result is the sink, holding the rows.</returns>
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

/// <summary>
/// Reads the rows into a chain of pooled arrays and makes the result from it, for every collecting terminal.
/// </summary>
/// <remarks>
/// <para>
/// This is one async method for every collecting terminal (<c>ToList</c>, <c>ToArray</c>,
/// <c>ToHashSet</c>, <c>ToDictionary</c>): the read loop and the build share a single
/// state machine, so a read that really suspends boxes one machine, not a
/// wrapper around another.
/// </para>
/// <para>
/// The rows go into a chain of pooled arrays; <paramref name="build"/>
/// (a struct, so what it carries rides in the machine, not in a delegate
/// and a state object) makes the result from the chain right after the
/// last row.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <typeparam name="TBuild">The struct that makes the result from the chain.</typeparam>
/// <typeparam name="TResult">The type of the result.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="useReadAsync">See <see cref="ReadRowAsync"/>.</param>
/// <param name="build">Makes the result from the chain.</param>
/// <param name="config">The pool and the clearing rule of the chain.</param>
/// <param name="cancellationToken">The token that cancels the read.</param>
/// <returns>A task whose result is what <paramref name="build"/> made from the rows.</returns>
	public static async ValueTask<TResult> CollectAsync<T, TBuild, TResult>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync,
		TBuild build, ChunkConfig<T> config,
		CancellationToken cancellationToken)
		where TBuild : struct, IChainBuild<T, TResult>
	{
		using var chain = ChunkChain<T>.Rent(config);
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
/// Returns a lazy async stream of the mapped rows.
/// </summary>
/// <remarks>
/// The reader must stay open while the stream is enumerated.
/// With <paramref name="readThrough"/>, once the rows end the later result sets are read through (see <see cref="Stream{T}"/>).
/// </remarks>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <param name="source">The mapping of the rows.</param>
/// <param name="reader">The reader positioned on a result set.</param>
/// <param name="skip">The number of rows to skip.</param>
/// <param name="take">The number of rows to read; 0 reads none.</param>
/// <param name="useReadAsync">See <see cref="ReadRowAsync"/>.</param>
/// <param name="readThrough"><see langword="true"/> to read through the later result sets once the rows end; otherwise, <see langword="false"/>.</param>
/// <param name="cancellationToken">The token that cancels the read.</param>
/// <returns>A lazy asynchronous sequence of the mapped rows.</returns>
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

/// <summary>
/// Reads the next row.
/// </summary>
/// <remarks>
/// Without <paramref name="useReadAsync"/> only the execution is async: rows are pulled with <c>Read</c> (still cancelable between rows).
/// </remarks>
/// <param name="reader">The reader.</param>
/// <param name="useReadAsync"><see langword="true"/> to read with <c>ReadAsync</c>; otherwise, <see langword="false"/> to read with <c>Read</c>.</param>
/// <param name="cancellationToken">The token that cancels the read.</param>
/// <returns>A task whose result is <see langword="true"/> if there is a row; otherwise, <see langword="false"/>.</returns>
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
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <typeparam name="TResult">The type of the result.</typeparam>
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
/// <remarks>
/// <para>
/// The arrays come from the pool of the <see cref="ChunkConfig{T}"/> (the shared array pool by default) and are as long as the pool makes them:
/// the sink uses all of the array it holds and grows by renting one twice as long.
/// A row is one array store.
/// </para>
/// <para>
/// <see cref="ChunkConfig{T}.ClearOnReturn"/> is what <see cref="PoolClearing"/> decided:
/// the rows that were written are cleared before an array goes back, and nothing else
/// (an array it outgrew is released the same way, since the rows were copied out of it).
/// </para>
/// </remarks>
/// <typeparam name="T">The type of a row.</typeparam>
/// <param name="config">The pool and the clearing rule of the buffer.</param>
internal struct ArraySink<T>(ChunkConfig<T> config) : IRowSink<T>
{
	T[]? _items;
	int _count;

	public void Add(T item)
	{
		T[]? items = _items;
		int count = _count;
		if (items is not null && (uint)count < (uint)items.Length)
		{
			items[count] = item;
			_count = count + 1;
			return;
		}

		AddSlow(item);
	}

/// <summary>
/// Adds a row to a bigger array: rents it, copies the rows and releases the old array.
/// </summary>
/// <remarks>
/// A rent that fails (or is refused) leaves the sink as it was:
/// the array is kept only once the rent has succeeded.
/// </remarks>
/// <param name="item">The row to add.</param>
	void AddSlow(T item)
	{
		T[]? old = _items;
		int rows = _count;
		T[] bigger = config.Rent(old is null ? 16 : old.Length * 2);
		if (old is not null) Array.Copy(old, bigger, rows);

		// The sink holds the new array before the old one is released, so
		// that a release that throws (a pool whose Return throws) leaves
		// one array held once: Dispose releases the new one, and the old
		// one is never released a second time.
		_items = bigger;
		bigger[rows] = item;
		_count = rows + 1;

		// The rows were copied: the old array goes back, its rows cleared
		// first when the rule says so (they are alive in the new one, so the
		// old must not keep them).
		if (old is not null) config.Seal(old, rows).Release();
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
		config.Seal(items, _count).Release();
	}
}
