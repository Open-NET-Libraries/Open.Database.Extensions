namespace Open.Database.Extensions;

/// <summary>
/// Terminal operations for <see cref="IMappedQuery{T}"/>: they materialize the mapped rows into a collection or take a single row. <see cref="MappedQuery{T}"/> and its skip and take variants expose the same operations directly.
/// </summary>
/// <remarks>
/// <para>
/// Every terminal executes the command, reads the rows while the connection is open, and releases the connection before the call returns (or, for the asynchronous forms, before the returned task completes). The asynchronous forms accept a <see cref="CancellationToken"/>, which is used together with the cancellation token set on the underlying command: either one cancels the call, and the call covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows.
/// </para>
/// <para>
/// Exceptions from the provider (for example <see cref="DbException"/>) propagate unchanged and the connection is still released.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// List&lt;Customer&gt; all = connection
///     .Command("SELECT Id, Name FROM Customers")
///     .Map&lt;Customer&gt;()
///     .ToList();
///
/// Customer? first = await connection
///     .Command("SELECT Id, Name FROM Customers ORDER BY Id")
///     .Map&lt;Customer&gt;()
///     .FirstOrDefaultAsync(cancellationToken);
/// </code>
/// </example>
/// <seealso cref="IMappedQuery{T}"/>
/// <seealso cref="MappedQuery{T}"/>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Convenience and style.")]
public static class MappedQueryExtensions
{
	const string ReadAsyncTokenFirstObsolete = "Pass the token last: query.ReadAsync(handler, cancellationToken). Removed in 12.";

	/// <summary>
	/// Asynchronously executes the query and passes the mapped rows to <paramref name="handler"/> as they are read. Use this overload when the handler does not need the cancellation token.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that completes after the handler has completed and the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask}, CancellationToken)"/>
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask> handler, CancellationToken cancellationToken = default)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();
		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <summary>
	/// Asynchronously executes the query, passes the mapped rows to <paramref name="handler"/> as they are read, and returns the handler's result. Use this overload when the handler does not need the cancellation token.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows. Enumerate it inside the handler; it is not valid after the handler returns.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces the value returned by <paramref name="handler"/> after the connection has been released.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync{TResult}(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask{TResult}}, CancellationToken)"/>
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask<TResult>> handler, CancellationToken cancellationToken = default)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <summary>
	/// Obsolete: use query.ReadAsync(handler, cancellationToken).
	/// Asynchronously executes the query and passes the mapped rows to <paramref name="handler"/>. The cancellation token comes first so the handler can be written as a trailing lambda.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <inheritdoc cref="ReadAsync{T}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask}, CancellationToken)"/>
	[Obsolete(ReadAsyncTokenFirstObsolete)]
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, ValueTask> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <summary>
	/// Obsolete: use query.ReadAsync(handler, cancellationToken).
	/// Asynchronously executes the query, passes the mapped rows to <paramref name="handler"/>, and returns the handler's result. The cancellation token comes first so the handler can be written as a trailing lambda.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <inheritdoc cref="ReadAsync{T, TResult}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask{TResult}}, CancellationToken)"/>
	[Obsolete(ReadAsyncTokenFirstObsolete)]
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, ValueTask<TResult>> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <summary>
	/// Obsolete: use query.ReadAsync(handler, cancellationToken).
	/// Asynchronously executes the query and passes the mapped rows to <paramref name="handler"/> together with the cancellation token in effect. The cancellation token comes first so the handler can be written as a trailing lambda.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <returns>A task that completes after the handler has completed and the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask}, CancellationToken)"/>
	[Obsolete(ReadAsyncTokenFirstObsolete)]
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <summary>
	/// Obsolete: use query.ReadAsync(handler, cancellationToken).
	/// Asynchronously executes the query, passes the mapped rows to <paramref name="handler"/> together with the cancellation token in effect, and returns the handler's result. The cancellation token comes first so the handler can be written as a trailing lambda.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TResult">The type returned by the handler.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <param name="handler">Receives a forward-only asynchronous sequence of mapped rows and the cancellation token in effect. Enumerate it inside the handler; the sequence and the token are not valid after the handler returns.</param>
	/// <returns>A task that produces the value returned by <paramref name="handler"/> after the connection has been released.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync{TResult}(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask{TResult}}, CancellationToken)"/>
	[Obsolete(ReadAsyncTokenFirstObsolete)]
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <summary>
	/// Reads every mapped row into a new <see cref="List{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <returns>A list of all the rows, in the order the database returned them. The list is empty if there are no rows.</returns>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static List<T> ToList<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.ToList());

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="List{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces a list of all the rows, in the order the database returned them.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<List<T>> ToListAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(
			async (items, token) => await items.ToListAsync(token).ConfigureAwait(false),
			cancellationToken);

	/// <summary>
	/// Reads every mapped row into a new array.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <returns>An array of all the rows, in the order the database returned them. The array is empty if there are no rows.</returns>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static T[] ToArray<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.ToArray());

	/// <summary>
	/// Asynchronously reads every mapped row into a new array.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces an array of all the rows, in the order the database returned them.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T[]> ToArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(
			async (items, token) => await items.ToArrayAsync(token).ConfigureAwait(false),
			cancellationToken);

	/// <summary>
	/// Reads every mapped row into a new <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <returns>An immutable array of all the rows, in the order the database returned them.</returns>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.ToImmutableArray());

	internal static async ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(this IAsyncEnumerable<T> query, CancellationToken cancellationToken)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		var builder = ImmutableArray.CreateBuilder<T>();
		await foreach (var item in query.WithCancellation(cancellationToken).ConfigureAwait(false))
		{
			builder.Add(item);
		}

		builder.Capacity = builder.Count;
		return builder.MoveToImmutable();
	}

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces an immutable array of all the rows, in the order the database returned them.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default) => query.ReadAsync(
			async (items, token) => await items.ToImmutableArrayAsync(token).ConfigureAwait(false),
			cancellationToken);

	/// <summary>
	/// Reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>, keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A dictionary of all the rows. It is empty if there are no rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this IMappedQuery<T> query, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.Read(items => items.ToDictionary(keySelector, comparer));

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>, keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces a dictionary of all the rows.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();
		return query.ReadAsync(
			async (items, token) => await items.ToDictionaryAsync(keySelector, comparer, token).ConfigureAwait(false),
			cancellationToken);
	}

	/// <summary>
	/// Reads every mapped row into a new <see cref="HashSet{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="comparer">The comparer for the rows, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A set of the distinct rows. Rows that compare equal to an earlier row are dropped.</returns>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static HashSet<T> ToHashSet<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer = null)
	{
#if NETSTANDARD2_0
		var result = new HashSet<T>(comparer);
		return query.Read(items =>
		{
			foreach (var item in items)
			{
				result.Add(item);
			}

			return result;
		});
#else
		return query.Read(items => items.ToHashSet(comparer));
#endif
	}

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="HashSet{T}"/>.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="comparer">The comparer for the rows, or <see langword="null"/> to use the default comparer.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces a set of the distinct rows.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// All rows are buffered in memory before the call returns. Use <see cref="IMappedQuery{T}.Read(Action{IEnumerable{T}})"/> to process very large results as a stream.
	/// </remarks>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default)
	{
#if NETSTANDARD2_0
		var result = new HashSet<T>(comparer);
		return query.ReadAsync(
			async (items, token) =>
			{
				await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
				{
					result.Add(item);
				}

				return result;
			},
			cancellationToken);
#else
		return query.ReadAsync(
			(items, token) => items.ToHashSetAsync(comparer, token),
			cancellationToken);
#endif
	}

	/// <summary>
	/// Reads the first mapped row.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <returns>The first row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <remarks>
	/// Rows after the first are not mapped. Add an <c>ORDER BY</c> to the SQL when "first" must be deterministic.
	/// </remarks>
	/// <seealso cref="SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query.Read(items => items.FirstOrDefault());
	}

	/// <summary>
	/// Asynchronously reads the first mapped row.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces the first row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <remarks>
	/// Rows after the first are not mapped. Add an <c>ORDER BY</c> to the SQL when "first" must be deterministic.
	/// </remarks>
	/// <seealso cref="SingleOrDefault{T}(IMappedQuery{T})"/>
	public static ValueTask<T?> FirstOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(
			async (items, token) => await items.FirstOrDefaultAsync(token).ConfigureAwait(false),
			cancellationToken);

	/// <summary>
	/// Reads the only mapped row.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <returns>The single row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <exception cref="InvalidOperationException">The query returns more than one row.</exception>
	/// <seealso cref="FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.SingleOrDefault());

	/// <summary>
	/// Asynchronously reads the only mapped row.
	/// </summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="query">The query to execute.</param>
	/// <param name="cancellationToken">Cancels the call, together with the cancellation token set on the underlying command: either one cancels it, whether it is opening the connection, looking up stored-procedure parameter names, executing or reading.</param>
	/// <returns>A task that produces the single row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <exception cref="InvalidOperationException">The query returns more than one row.</exception>
	/// <seealso cref="FirstOrDefault{T}(IMappedQuery{T})"/>
	public static ValueTask<T?> SingleOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(
			async (items, token) => await items.SingleOrDefaultAsync(token).ConfigureAwait(false),
			cancellationToken);
}
