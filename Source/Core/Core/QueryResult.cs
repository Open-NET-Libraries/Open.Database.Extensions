
namespace Open.Database.Extensions.Core;

/// <summary>
/// Represents a container for data reader results that also provides the column names
/// and other helpful data methods.
/// </summary>
/// <typeparam name="TResult">The type of the result property.</typeparam>
public class QueryResult<TResult>
{
	/// <summary>Initializes a new instance of the <see cref="QueryResult{TResult}"/> class.</summary>
	/// <param name="ordinals">The ordinal values requested.</param>
	/// <param name="names">The column names requested.</param>
	/// <param name="result">The result.</param>
	/// <exception cref="ArgumentException"><paramref name="ordinals"/> and <paramref name="names"/> differ in length.</exception>
	public QueryResult(
		ImmutableArray<int> ordinals,
		ImmutableArray<string> names,
		TResult result)
	{
		if (ordinals.Length != names.Length)
			throw new ArgumentException("Mismatched array lengths of ordinals and names.");

		Ordinals = ordinals;
		Names = names;
		Result = result;
		Contract.EndContractBlock();

		ColumnCount = ordinals.Length;
	}

	/// <inheritdoc cref="QueryResult{TResult}.QueryResult(ImmutableArray{int}, ImmutableArray{string}, TResult)" />
	/// <exception cref="ArgumentNullException"><paramref name="ordinals"/> or <paramref name="names"/> is <see langword="null"/>.</exception>
	public QueryResult(
		IEnumerable<int> ordinals,
		IEnumerable<string> names,
		TResult result)
		: this(Immute(ordinals), Immute(names), result)
	{
	}

	static ImmutableArray<T> Immute<T>(IEnumerable<T> source)
		=> source is ImmutableArray<T> o ? o : source?.ToImmutableArray()
		?? throw new ArgumentNullException(nameof(source));

	/// <summary>
	/// The number of columns.
	/// </summary>
	public readonly int ColumnCount;

	/// <summary>
	/// The ordinal values requested.
	/// </summary>
	public readonly ImmutableArray<int> Ordinals;

	/// <summary>
	/// The column names requested.
	/// </summary>
	public readonly ImmutableArray<string> Names;

	/// <summary>
	/// The values requested.
	/// A queue is typically used since values are used first in, first out,
	/// and dequeuing results helps reduce redundant memory usage.
	/// </summary>
	public readonly TResult Result;

	/// <summary>Implicitly returns the result from this instance.</summary>
	/// <param name="result">The source of the result.</param>
	/// <returns>The <see cref="Result"/> of <paramref name="result"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
	public static implicit operator TResult(QueryResult<TResult> result) => (result ?? throw new ArgumentNullException(nameof(result))).Result;
}

/// <summary>
/// Obsolete: nothing in the library returns this type; there is no replacement (rows come back as a <see cref="List{T}"/> from <see cref="MappedQuery{T}.ToList"/>).
/// Represents a container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <remarks>
/// <include file="RowMapping.docs.xml" path="docs/members[@name='obsolete-12']/*" />
/// </remarks>
/// <typeparam name="T">The type of the items in the resultant enumerable.</typeparam>
/// <typeparam name="TResult">The type of the result property.</typeparam>
[Obsolete(QueryResultExtensions._queryResultCollectionObsolete)]
public class QueryResultCollection<T, TResult> : QueryResult<TResult>, IEnumerable<T>
	where TResult : IEnumerable<T>
{
	/// <summary>Initializes a new instance of the <see cref="QueryResultCollection{T, TResult}"/> class.</summary>
	/// <inheritdoc />
	public QueryResultCollection(
		ImmutableArray<int> ordinals,
		ImmutableArray<string> names,
		TResult result)
		: base(ordinals, names, result)
	{
	}

	/// <inheritdoc cref="QueryResultCollection{T, TResult}.QueryResultCollection(ImmutableArray{int}, ImmutableArray{string}, TResult)" />
	public QueryResultCollection(
		IEnumerable<int> ordinals,
		IEnumerable<string> names,
		TResult result)
		: base(ordinals, names, result)
	{
	}

	/// <inheritdoc />
	public IEnumerator<T> GetEnumerator()
		=> Result is null
		? Enumerable.Empty<T>().GetEnumerator() // A null result is empty.
		: Result.GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator()
		=> GetEnumerator();
}

/// <summary>
/// Obsolete: nothing in the library returns this type; there is no replacement (rows come back as a <see cref="List{T}"/> from <see cref="MappedQuery{T}.ToList"/>).
/// Represents a container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <remarks>
/// <include file="RowMapping.docs.xml" path="docs/members[@name='obsolete-12']/*" />
/// </remarks>
/// <typeparam name="T">The type of the items in the resultant enumerable.</typeparam>
[Obsolete(QueryResultExtensions._queryResultCollectionObsolete)]
public class QueryResultCollection<T> : QueryResultCollection<T, IEnumerable<T>>
{
	/// <summary>Initializes a new instance of the <see cref="QueryResultCollection{T}"/> class.</summary>
	/// <inheritdoc />
	public QueryResultCollection(
		ImmutableArray<int> ordinals,
		ImmutableArray<string> names,
		IEnumerable<T> result)
		: base(ordinals, names, result)
	{
	}

	/// <inheritdoc cref="QueryResultCollection{T, TResult}.QueryResultCollection(ImmutableArray{int}, ImmutableArray{string}, TResult)" />
	public QueryResultCollection(
		IEnumerable<int> ordinals,
		IEnumerable<string> names,
		IEnumerable<T> result)
		: base(ordinals, names, result)
	{
	}
}

/// <summary>
/// Represents a container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <typeparam name="T">The type of the items in the resultant enumerable.</typeparam>
public class QueryResultQueue<T> : QueryResult<Queue<T>> // Not exposed as enumerable to avoid confusion with the queue.
{
	/// <summary>Initializes a new instance of the <see cref="QueryResultQueue{T}"/> class.</summary>
	/// <inheritdoc />
	public QueryResultQueue(
		ImmutableArray<int> ordinals,
		ImmutableArray<string> names,
		Queue<T> result)
		: base(ordinals, names, result)
	{
	}

	/// <inheritdoc cref="QueryResultQueue{T}.QueryResultQueue(ImmutableArray{int}, ImmutableArray{string}, Queue{T})" />
	public QueryResultQueue(
		IEnumerable<int> ordinals,
		IEnumerable<string> names,
		Queue<T> result)
		: base(ordinals, names, result)
	{
	}

	/// <inheritdoc cref="QueryResultQueue{T}.QueryResultQueue(ImmutableArray{int}, ImmutableArray{string}, Queue{T})" />
	public QueryResultQueue(
		IEnumerable<int> ordinals,
		IEnumerable<string> names,
		IEnumerable<T> result)
		: base(ordinals, names, new(result))
	{
	}
}

/// <summary>
/// Provides extensions for getting column data from a <see cref="QueryResult{TResult}"/>.
/// </summary>
public static class QueryResultExtensions
{
	internal const string _queryResultCollectionObsolete = "Nothing in the library returns this type, and there is no replacement. Get rows as a List<T> from .Map<T>().ToList(), or keep your own class. Removed in 12.";
	const string AsMappedDictionariesTail = " DequeueAsMappedDictionaries drains the queue as you enumerate it, so it can be read once; this method could be read again. Removed in 12.";
	const string AsMappedDictionariesObsolete = "Nothing in the library returns the QueryResult<IEnumerable<object?[]>> this takes. To read rows as dictionaries use .Retrieve().DequeueAsMappedDictionaries(), or .Map(record => ...) with a selector that builds the dictionary." + AsMappedDictionariesTail;
	const string AsMappedDictionariesAsyncObsolete = "Nothing in the library returns the QueryResult<IEnumerable<object?[]>> this takes. To read rows as dictionaries use (await cmd.RetrieveAsync()).DequeueAsMappedDictionaries(), or .Map(record => ...) with a selector that builds the dictionary." + AsMappedDictionariesTail;

	const string DequeueAsTail = "Map reads in one pass instead of buffering raw rows first, reports a missing required column at the first row (so not for an empty result), and also reads later result sets: to read only the first, as Retrieve did, call .SingleResult() before Map (cmd.SingleResult().Map<T>(); .Map<T>().SingleResult() still works). DequeueAs returned a one-pass enumerable; ToList() returns a List<T>. Removed in 12.";
	const string DequeueAsObsolete = "Use cmd.Map<T>().ToList() instead of cmd.Retrieve().DequeueAs<T>(), or cmd.Map(map).ToList() when you passed a RowMap (DequeueAs accepted a null map; Map(map) throws on null). On an IDbCommand use command.Results<T>() (it takes no CommandBehavior); on an IDataReader use reader.ResultsBuffered<T>() (reader.Results<T>() is lazy). " + DequeueAsTail;
	const string DequeueAsAsyncObsolete = "Use await cmd.Map<T>().ToListAsync() instead of (await cmd.RetrieveAsync()).DequeueAs<T>(), or cmd.Map(map).ToListAsync() when you passed a RowMap (DequeueAs accepted a null map; Map(map) throws on null). On a DbCommand use await command.ResultsAsync<T>(). " + DequeueAsTail;
	/// <summary>
	/// Returns an enumerable that dequeues the results and returns a column
	/// mapped dictionary for each entry.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to <see langword="null"/>.</remarks>
	/// <param name="source">The query result. Typically produced by a <c>Retrieve</c> method.</param>
	/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	public static IEnumerable<Dictionary<string, object?>> DequeueAsMappedDictionaries(this QueryResult<Queue<object[]>> source)
	{
		return source is null
			? throw new ArgumentNullException(nameof(source))
			: DequeueAsMappedDictionariesCore(source);

		static IEnumerable<Dictionary<string, object?>> DequeueAsMappedDictionariesCore(QueryResult<Queue<object[]>> source)
		{
			Queue<object[]> q = source.Result;
			ImmutableArray<string> names = source.Names;
			int count = source.ColumnCount;
			while (q.Count != 0)
			{
				object[] r = q.Dequeue();
				var d = new Dictionary<string, object?>(count);
				for (int i = 0; i < count; i++)
					d.Add(names[i], CoreExtensions.DBNullValueToNull(r[i]));
				yield return d;
			}
		}
	}

	/// <inheritdoc
	/// cref="DequeueAsMappedDictionaries(QueryResult{Queue{object[]}})"/>
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this Task<QueryResult<Queue<object[]>>> source)
		=> (await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false)).DequeueAsMappedDictionaries();

	/// <inheritdoc
	/// cref="DequeueAsMappedDictionaries(QueryResult{Queue{object[]}})"/>
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this ValueTask<QueryResult<Queue<object[]>>> source)
		=> (await source.ConfigureAwait(false)).DequeueAsMappedDictionaries();

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader)"/> then <see cref="MappedQuery{T}.ToList"/> (async: <see cref="MappedQuery{T}.ToListAsync(CancellationToken)"/>).
	/// Returns an enumerable that dequeues the results and attempts to map the
	/// fields to type <typeparamref name="T"/>.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='obsolete-12']/*" />
	/// <para>
	/// Replace the whole chain <c>cmd.Retrieve().DequeueAs&lt;T&gt;()</c>:
	/// on an <c>IExecuteReader</c> use <c>cmd.Map&lt;T&gt;().ToList()</c> (<c>cmd.Map(map).ToList()</c> with a <see cref="RowMap{T}"/>);
	/// on an <c>IDbCommand</c> use <c>Results&lt;T&gt;()</c>;
	/// on an <c>IDataReader</c> use <c>ResultsBuffered&lt;T&gt;()</c>;
	/// on a <c>DbCommand</c> async use <c>ResultsAsync&lt;T&gt;()</c>.
	/// The "Deprecated in 11.0" page has the differences and a before/after example.
	/// </para>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map-declare-once']/*" />
	/// <para><see cref="DBNull"/> values are converted to <see langword="null"/>.</para>
	/// </remarks>
	/// <typeparam name="T">The type of the entity to create.</typeparam>
	/// <param name="source">The query result. Typically produced by a <c>Retrieve</c> method.</param>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>An enumerable that dequeues the results and returns an entity
	/// of type <typeparamref name="T"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(DequeueAsObsolete)]
	public static IEnumerable<T> DequeueAs<T>(
		this QueryResult<Queue<object[]>> source,
		RowMap<T>? map = null)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(source);
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" path="/*[not(self::param)]" />
	/// <param name="source">The task that produces the query result.</param>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	[Obsolete(DequeueAsAsyncObsolete)]
	public static async ValueTask<IEnumerable<T>> DequeueAs<T>(
		this Task<QueryResult<Queue<object[]>>> source,
		RowMap<T>? map = null)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" path="/*[not(self::param) and not(self::exception)]" />
	/// <param name="source">The value task that produces the query result.</param>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	[Obsolete(DequeueAsAsyncObsolete)]
	public static async ValueTask<IEnumerable<T>> DequeueAs<T>(
		this ValueTask<QueryResult<Queue<object[]>>> source,
		RowMap<T>? map = null)
	{
		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" path="/*[not(self::param)]" />
	/// <param name="source">The task that produces the query result.</param>
	[Obsolete(DequeueAsAsyncObsolete)]
	[OverloadResolutionPriority(1)]
	public static ValueTask<IEnumerable<T>> DequeueAs<T>(
		this Task<QueryResult<Queue<object[]>>> source)
		=> DequeueAs<T>(source, null);

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" path="/*[not(self::param) and not(self::exception)]" />
	/// <param name="source">The value task that produces the query result.</param>
	[Obsolete(DequeueAsAsyncObsolete)]
	[OverloadResolutionPriority(1)]
	public static ValueTask<IEnumerable<T>> DequeueAs<T>(
		this ValueTask<QueryResult<Queue<object[]>>> source)
		=> DequeueAs<T>(source, null);

	/// <summary>
	/// Obsolete: nothing produces the source this takes. Use <see cref="DequeueAsMappedDictionaries(QueryResult{Queue{object[]}})"/> on a <c>Retrieve()</c> result, or <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/> with your own selector.
	/// Returns an enumerable of column mapped dictionaries, one for each entry.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='obsolete-12']/*" />
	/// <para><see cref="DBNull"/> values are converted to <see langword="null"/>.</para>
	/// </remarks>
	/// <param name="source">The query result. Typically produced by a <c>Retrieve</c> method.</param>
	/// <returns>An enumerable of column mapped dictionaries, one for each entry.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
	[Obsolete(AsMappedDictionariesObsolete)]
	public static IEnumerable<Dictionary<string, object?>> AsMappedDictionaries(this QueryResult<IEnumerable<object?[]>> source)
	{
		return source is null
			? throw new ArgumentNullException(nameof(source))
			: AsMappedDictionariesCore(source);

		static IEnumerable<Dictionary<string, object?>> AsMappedDictionariesCore(QueryResult<IEnumerable<object?[]>> source)
		{
			IEnumerable<object?[]> q = source.Result;
			ImmutableArray<string> names = source.Names;
			int count = source.ColumnCount;
			foreach (object?[] r in q)
			{
				var d = new Dictionary<string, object?>(count);
				for (int i = 0; i < count; i++)
					d.Add(names[i], CoreExtensions.DBNullValueToNull(r[i]));
				yield return d;
			}
		}
	}

	/// <inheritdoc
	/// cref="AsMappedDictionaries(QueryResult{IEnumerable{object?[]}})"/>
	[Obsolete(AsMappedDictionariesAsyncObsolete)]
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this ValueTask<QueryResult<IEnumerable<object?[]>>> source)
		=> AsMappedDictionaries(await source.ConfigureAwait(false));

	/// <inheritdoc
	/// cref="AsMappedDictionaries(QueryResult{IEnumerable{object?[]}})"/>
	[Obsolete(AsMappedDictionariesAsyncObsolete)]
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this Task<QueryResult<IEnumerable<object?[]>>> source)
		=> AsMappedDictionaries(await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false));
}
