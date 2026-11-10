
namespace Open.Database.Extensions.Core;

/// <summary>
/// A container for data reader results that also provides the column names
/// and other helpful data methods.
/// </summary>
/// <typeparam name="TResult">The type of the result property.</typeparam>
public class QueryResult<TResult>
{
	/// <summary>Constructs a <see cref="QueryResult{TResult}"/>.</summary>
	/// <param name="ordinals">The ordinal values requested</param>
	/// <param name="names">The column names requested.</param>
	/// <param name="result">The result.</param>
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
	/// The values requested.  A Queue is used since values are typically used
	/// first in first out and dequeuing results helps reduced redundant memory
	/// usage.
	/// </summary>
	public readonly TResult Result;

	/// <summary>Implicitly returns the result from this instance.</summary>
	/// <param name="result">The source of the result.</param>
	public static implicit operator TResult(QueryResult<TResult> result) => (result ?? throw new ArgumentNullException(nameof(result))).Result;
}

/// <summary>
/// A container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <typeparam name="T">The type of the items in the resultant enumerable.</typeparam>
/// <typeparam name="TResult">The type of the result property.</typeparam>
public class QueryResultCollection<T, TResult> : QueryResult<TResult>, IEnumerable<T>
	where TResult : IEnumerable<T>
{
	/// <summary>Constructs a <see cref="QueryResultCollection{T,
	/// TResult}"/>.</summary>
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
/// A container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <typeparam name="T">The type of the items in the resultant enumerable.</typeparam>
public class QueryResultCollection<T> : QueryResultCollection<T, IEnumerable<T>>
{
	/// <summary>Constructs a <see cref="QueryResultCollection{T}"/>.</summary>
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
/// A container for data reader results that also provides the column names and
/// other helpful data methods.
/// </summary>
/// <typeparam name="T">The type of the items in the resultant
/// enumerable.</typeparam>
public class QueryResultQueue<T> : QueryResult<Queue<T>> // Not exposed as enumerable to avoid confusion with the queue.
{
	/// <summary>Constructs a <see cref="QueryResult{T}"/>.</summary>
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
/// A set of extensions for getting column data from a QueryResult.
/// </summary>
public static class QueryResultExtensions
{
	/// <summary>
	/// Returns an enumerable that dequeues the results and returns a column
	/// mapped dictionary for each entry.<see cref="DBNull"/> values are converted to null.
	/// </summary>
	/// <param name="source">The query result.  Typically produced by a
	/// .Retrieve method.</param>
	/// <returns>An enumerable that dequeues the results and returns a column
	/// mapped dictionary for each entry</returns>
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
	/// Returns an enumerable that dequeues the results and attempts to map the
	/// fields to type T.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <remarks>DBNull values are converted to null.</remarks>
	/// <param name="source">The query result.  Typically produced by a
	/// .Retrieve method.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>An enumerable that dequeues the results and returns an entity
	/// of type T.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> DequeueAs<T>(
		this QueryResult<Queue<object[]>> source,
		RowMap<T>? map = null)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(source);
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" />
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<IEnumerable<T>> DequeueAs<T>(
		this Task<QueryResult<Queue<object[]>>> source,
		RowMap<T>? map = null)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" />
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<IEnumerable<T>> DequeueAs<T>(
		this ValueTask<QueryResult<Queue<object[]>>> source,
		RowMap<T>? map = null)
	{
		var x = new Transformer<T>(map);
		return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
	}

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" />
	[OverloadResolutionPriority(1)]
	public static ValueTask<IEnumerable<T>> DequeueAs<T>(
		this Task<QueryResult<Queue<object[]>>> source)
		=> DequeueAs<T>(source, null);

	/// <inheritdoc cref="DequeueAs{T}(QueryResult{Queue{object[]}}, RowMap{T}?)" />
	[OverloadResolutionPriority(1)]
	public static ValueTask<IEnumerable<T>> DequeueAs<T>(
		this ValueTask<QueryResult<Queue<object[]>>> source)
		=> DequeueAs<T>(source, null);

	/// <summary>
	/// Returns an enumerable that dequeues the results and returns a column
	/// mapped dictionary for each entry.<see cref="DBNull"/> values are converted to null.
	/// </summary>
	/// <param name="source">The query result.  Typically produced by a
	/// .Retrieve method.</param>
	/// <returns>An enumerable that dequeues the results and returns a column
	/// mapped dictionary for each entry</returns>
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
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this ValueTask<QueryResult<IEnumerable<object?[]>>> source)
		=> AsMappedDictionaries(await source.ConfigureAwait(false));

	/// <inheritdoc
	/// cref="AsMappedDictionaries(QueryResult{IEnumerable{object?[]}})"/>
	public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this Task<QueryResult<IEnumerable<object?[]>>> source)
		=> AsMappedDictionaries(await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false));
}
