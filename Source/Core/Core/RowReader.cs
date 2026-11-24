namespace Open.Database.Extensions.Core;

/// <summary>Supplies the per-row mapping delegate for the layout of a reader's current result set.</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
internal interface IRowSource<T>
{
	/// <summary>Returns the mapper for a <see cref="DbDataReader"/>.</summary>
	/// <param name="reader">The reader positioned on a result set.</param>
	/// <returns>The delegate that maps the current row of <paramref name="reader"/>.</returns>
	// The virtual-call mapper: the fast path.
	Func<DbDataReader, T> Mapper(DbDataReader reader);

	/// <summary>Returns the mapper for any other <see cref="IDataRecord"/>.</summary>
	/// <param name="reader">The record positioned on a result set.</param>
	/// <returns>The delegate that maps the current row of <paramref name="reader"/>.</returns>
	Func<IDataRecord, T> Mapper(IDataRecord reader);
}

/// <summary>Represents a caller-supplied projection.</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <param name="selector">The projection of a record to a row.</param>
internal sealed class SelectorSource<T>(Func<IDataRecord, T> selector) : IRowSource<T>
{
	/// <summary>
	/// Determines whether <paramref name="obj"/> is a source of an equal projection.
	/// </summary>
	/// <param name="obj">The object to compare with this source.</param>
	/// <returns>
	/// <see langword="true"/> if <paramref name="obj"/> is a <see cref="SelectorSource{T}"/> whose delegate equals this one; otherwise, <see langword="false"/>.
	/// </returns>
	// Equal when the delegates are: a static lambda or a method group is, a
	// capturing lambda is not (each call makes a new one). Used by the memoize
	// seam only, never on the path that reads rows.
	public override bool Equals(object? obj) => obj is SelectorSource<T> other && selector.Equals(other.Selector);

	/// <inheritdoc />
	public override int GetHashCode() => selector.GetHashCode();

	Func<IDataRecord, T> Selector => selector;

	/// <summary>Returns the projection as the mapper for a <see cref="DbDataReader"/>.</summary>
	/// <param name="reader">The reader, which is not used.</param>
	/// <returns>The projection.</returns>
	// A Func<IDataRecord, T> is a Func<DbDataReader, T> by contravariance: no
	// wrapper delegate is needed.
	public Func<DbDataReader, T> Mapper(DbDataReader reader) => selector;
	/// <summary>Returns the projection as the mapper for an <see cref="IDataRecord"/>.</summary>
	/// <param name="reader">The record, which is not used.</param>
	/// <returns>The projection.</returns>
	public Func<IDataRecord, T> Mapper(IDataRecord reader) => selector;
}

/// <summary>Receives each mapped row.</summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
// Implemented by structs, so the read loops specialize and inline Add.
internal interface IRowSink<in T>
{
	/// <summary>Receives one mapped row.</summary>
	/// <param name="item">The mapped row.</param>
	void Add(T item);

	/// <summary>Gets the number of rows received.</summary>
	// Read by the read loop's handler to name the failing row, so the loop
	// keeps no counter of its own that the handler reads.
	int Count { get; }
}

/// <summary>
/// Provides the read loops that turn a positioned reader into results.
/// </summary>
// The loops call the mapper directly per row: no iterator, no LINQ, no per-row
// allocation other than the mapped objects themselves. Skipped rows are advanced
// without being mapped; a take limit stops reading as soon as it is satisfied.
// What happens to each row is the sink's business, so one loop serves every
// collection type.
// The command has already run when a loop starts: the caller opened the reader
// (that is what executes the command, and what a Take(0) is still expected to do).
// A take of 0 therefore only means "read no rows": the loops return an empty
// result at once, without skipping or mapping anything, and the caller disposes
// the reader (and reads through later result sets) as for any other call.
// A reader that is not a DbDataReader (a bare IDataReader) takes the Stream path:
// it has no virtual-call fast path, so one lazy loop reads it, and that loop makes
// the same take-0 decision, so the result is the same.
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
		try
		{
			do
			{
				sink.Add(map(db));
				if (++n == take) break;
			}
			while (db.Read());
		}
		catch (RowMappingFailure failure)
		{
			// The handler reads the sink's count, not n: a local the handler
			// reads is stored to the frame on every update, once per row.
			throw failure.At(skip + sink.Count);
		}
	}

	/// <summary>Reads the rows into a <see cref="List{T}"/>.</summary>
	/// <typeparam name="T">The type of a mapped row.</typeparam>
	/// <param name="source">The mapping of the rows.</param>
	/// <param name="reader">The reader positioned on a result set.</param>
	/// <param name="skip">The number of rows to skip.</param>
	/// <param name="take">The number of rows to read; 0 reads none.</param>
	/// <param name="token">The token that cancels the read; only a reader that is not a <see cref="DbDataReader"/> observes it.</param>
	/// <returns>The rows, in a new list.</returns>
	// The two hottest terminals (this one and ToArray) keep their own loops:
	// the sink indirection of Fill is measurable with a tiny projection
	// (benchmarked), so the list and the array are written out directly.
	public static List<T> ToList<T>(IRowSource<T> source, IDataReader reader, int skip, int take, CancellationToken token = default)
	{
		if (reader is not DbDataReader db)
			return [.. Stream(source, reader, skip, take, false, token)];

		var list = new List<T>();
		if (take == 0 || !SkipRows(db, skip) || !db.Read()) return list;

		Func<DbDataReader, T> map = source.Mapper(db);
		int n = 0;
		try
		{
			do
			{
				list.Add(map(db));
				if (++n == take) break;
			}
			while (db.Read());
		}
		catch (RowMappingFailure failure)
		{
			// The list's count, not n (see Fill).
			throw failure.At(skip + list.Count);
		}

		return list;
	}

	/// <summary>
	/// Reads the rows into a chain of pooled arrays and makes the result from it right after the last row.
	/// The chain is disposed however the call ends.
	/// </summary>
	/// <typeparam name="T">The type of a mapped row.</typeparam>
	/// <typeparam name="TBuild">The struct that makes the result from the chain.</typeparam>
	/// <typeparam name="TResult">The type of the result.</typeparam>
	/// <param name="source">The mapping of the rows.</param>
	/// <param name="reader">The reader positioned on a result set.</param>
	/// <param name="skip">The number of rows to skip.</param>
	/// <param name="take">The number of rows to read; 0 reads none.</param>
	/// <param name="build">Makes the result from the chain.</param>
	/// <param name="config">The pool and the clearing rule of the chain.</param>
	/// <param name="token">The token that cancels the read; only a reader that is not a <see cref="DbDataReader"/> observes it.</param>
	/// <returns>The result <paramref name="build"/> made from the rows.</returns>
	// The one sync shape of the collecting terminals. The arrays come from the
	// pool of config (the shared array pool by default); config says what
	// PoolClearing decided for this call.
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
	/// <typeparam name="T">The type of a mapped row.</typeparam>
	/// <param name="source">The mapping of the rows.</param>
	/// <param name="reader">The reader positioned on a result set.</param>
	/// <param name="skip">The number of rows to skip.</param>
	/// <param name="take">The number of rows to read; 0 reads none.</param>
	/// <param name="config">The pool and the clearing rule of the buffer.</param>
	/// <param name="token">The token that cancels the read; only a reader that is not a <see cref="DbDataReader"/> observes it.</param>
	/// <returns>The rows, in an array that is as long as their count.</returns>
	// Keeps its own loop, like ToList, rather than using Fill.
	public static T[] ToArray<T>(
		IRowSource<T> source, IDataReader reader, int skip, int take,
		ChunkConfig<T> config, CancellationToken token = default)
	{
		// A bare IDataReader has no DbDataReader fast path: Fill reads it through Stream into the same sink.
		if (reader is not DbDataReader db)
		{
			// The sink is a mutable struct, so it is disposed by hand (a using variable would hand Fill a copy).
			var sink = new ArraySink<T>(config);
			try
			{
				Fill(source, reader, skip, take, ref sink, token);
				return sink.ToArray();
			}
			finally
			{
				sink.Dispose();
			}
		}

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
		catch (RowMappingFailure failure)
		{
			// The buffer's count, not n (see Fill).
			throw failure.At(skip + buffer.Count);
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
				item = MapAt(source.Mapper(db), db, skip);
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

			item = MapAt(source.Mapper(db), db, skip);
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
	/// The reader must stay open while the stream is enumerated, and an enumeration holds the connection for each row: for a buffered result use <see cref="ToList{T}"/>.
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
		int first = skip; // The row number of the first row mapped: skip counts down below.
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
					// A try cannot hold a yield: it holds the map call only. The
					// handler reads the iterator's fields; the row does nothing
					// for it.
					T item;
					try
					{
						item = dbMap(db);
					}
					catch (RowMappingFailure failure)
					{
						throw failure.At(first + n);
					}

					yield return item;
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
					T item;
					try
					{
						item = map(reader);
					}
					catch (RowMappingFailure failure)
					{
						throw failure.At(first + n);
					}

					yield return item;
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

	// The loops that fill a collection catch around the loop: one try region for the whole loop. The iterators (Stream, StreamAsync) cannot catch around a yield, so they catch around the map call. The one-row terminals (First, Single) catch here.
	/// <summary>
	/// Maps the current row, and reports a failure of a row reader of the library for the row.
	/// </summary>
	/// <typeparam name="TReader">The type of the reader.</typeparam>
	/// <typeparam name="T">The type of a mapped row.</typeparam>
	/// <param name="map">The mapper.</param>
	/// <param name="reader">The reader positioned on the row.</param>
	/// <param name="row">The 0-based row in the result set.</param>
	/// <returns>The mapped row.</returns>
	static T MapAt<TReader, T>(Func<TReader, T> map, TReader reader, int row)
	{
		try
		{
			return map(reader);
		}
		catch (RowMappingFailure failure)
		{
			throw failure.At(row);
		}
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
	// ref parameters cannot cross an await, so the sink is passed and returned
	// by value.
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
		try
		{
			do
			{
				sink.Add(map(reader));
				if (++n == take) break;
			}
			while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));
		}
		catch (RowMappingFailure failure)
		{
			// The sink's count, as in Fill.
			throw failure.At(skip + sink.Count);
		}

		return sink;
	}

	/// <summary>
	/// Reads the rows into a chain of pooled arrays and makes the result from it right after the last row, for every collecting terminal.
	/// </summary>
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
	// One async method for every collecting terminal (ToList, ToArray,
	// ToHashSet, ToDictionary): the read loop and the build share a single
	// state machine, so a read that really suspends boxes one machine, not a
	// wrapper around another. build is a struct, so what it carries rides in
	// the machine, not in a delegate and a state object.
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
			try
			{
				do
				{
					chain.Add(map(reader));
					if (++n == take) break;
				}
				while (await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false));
			}
			catch (RowMappingFailure failure)
			{
				// The chain's count, as in Fill.
				throw failure.At(skip + chain.Count);
			}
		}

		return build.Build(chain);
	}

	public static async ValueTask<(bool Found, T? Item)> TryFirstAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
		=> take != 0
		&& await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
		&& await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)
		? (true, MapAt(source.Mapper(reader), reader, skip))
		: (false, default);

	public static async ValueTask<(bool Found, T? Item)> TrySingleAsync<T>(
		IRowSource<T> source, DbDataReader reader, int skip, int take, bool useReadAsync, CancellationToken cancellationToken)
	{
		if (take != 0
			&& await SkipRowsAsync(reader, skip, useReadAsync, cancellationToken).ConfigureAwait(false)
			&& await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false))
		{
			T item = MapAt(source.Mapper(reader), reader, skip);
			if (take != 1 && await ReadRowAsync(reader, useReadAsync, cancellationToken).ConfigureAwait(false)) throw MoreThanOne();
			return (true, item);
		}

		return (false, default);
	}

	/// <summary>
	/// Returns a lazy async stream of the mapped rows.
	/// </summary>
	/// <remarks>
	/// The reader must stay open while the stream is enumerated, and an enumeration holds the connection for each row (with asynchronous reads, for as long as the consumer takes): for a buffered result use <see cref="CollectAsync{T, TBuild, TResult}"/>.
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
				T item;
				try
				{
					item = map(reader);
				}
				catch (RowMappingFailure failure)
				{
					// As in Stream: the handler reads the iterator's fields.
					throw failure.At(skip + n);
				}

				yield return item;
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
	public int Count => List.Count;
}

/// <summary>
/// Makes the result of a collecting terminal from the rows in a chain.
/// </summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <typeparam name="TResult">The type of the result.</typeparam>
// Implemented by structs, so each collecting method is specialized per result
// and carries no delegate or state object.
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

/// <summary>The array of ToArray, as an immutable array.</summary>
// Wrapped, not copied, where ImmutableCollectionsMarshal is available.
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
/// A pooled, growable buffer that makes an exact-size array.
/// </summary>
/// <typeparam name="T">The type of a row.</typeparam>
/// <param name="config">The pool and the clearing rule of the buffer.</param>
// The arrays come from the pool of the ChunkConfig (the shared array pool by default)
// and are as long as the pool makes them: the sink uses all of the array it holds and
// grows by renting one twice as long. A row is one array store.
// ChunkConfig.ClearOnReturn is what PoolClearing decided: the rows that were written
// are cleared before an array goes back, and nothing else (an array it outgrew is
// released the same way, since the rows were copied out of it).
internal struct ArraySink<T>(ChunkConfig<T> config) : IRowSink<T>
{
	T[]? _items;
	/// <summary>Gets the number of rows added.</summary>
	public int Count { readonly get; private set; }

	public void Add(T item)
	{
		T[]? items = _items;
		int count = Count;
		if (items is not null && (uint)count < (uint)items.Length)
		{
			items[count] = item;
			Count = count + 1;
			return;
		}

		AddSlow(item);
	}

	/// <summary>
	/// Adds a row to a bigger array: rents it, copies the rows and releases the old array.
	/// </summary>
	/// <param name="item">The row to add.</param>
	// A rent that fails (or is refused) leaves the sink as it was: the array is
	// kept only once the rent has succeeded.
	void AddSlow(T item)
	{
		T[]? old = _items;
		int rows = Count;
		T[] bigger = config.Rent(old is null ? 16 : old.Length * 2);
		if (old is not null) Array.Copy(old, bigger, rows);

		// The sink holds the new array before the old one is released, so
		// that a release that throws (a pool whose Return throws) leaves
		// one array held once: Dispose releases the new one, and the old
		// one is never released a second time.
		_items = bigger;
		bigger[rows] = item;
		Count = rows + 1;

		// The rows were copied: the old array goes back, its rows cleared
		// first when the rule says so (they are alive in the new one, so the
		// old must not keep them).
		if (old is not null) config.Seal(old, rows).Release();
	}

	public readonly T[] ToArray()
	{
		if (Count == 0) return [];

		var result = new T[Count];
		Array.Copy(_items!, result, Count);
		return result;
	}

	public void Dispose()
	{
		T[]? items = _items;
		if (items is null) return;

		_items = null;
		config.Seal(items, Count).Release();
	}
}
