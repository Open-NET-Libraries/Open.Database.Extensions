
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
}
