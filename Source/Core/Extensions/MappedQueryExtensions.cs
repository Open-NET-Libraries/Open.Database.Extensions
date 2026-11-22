namespace Open.Database.Extensions;

/// <summary>
/// Terminal operations for <see cref="IMappedQuery{T}"/>: they
/// materialize the mapped rows into a collection or take a single row.
/// <see cref="MappedQuery{T}"/> and its skip and take variants expose
/// the same operations directly.
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
/// <code language="csharp">
/// List&lt;Post&gt; posts = await connection
///     .Command("SELECT Id, Title FROM Posts")
///     .Map&lt;Post&gt;()
///     .ToListAsync(cancellationToken);
///
/// Post? first = await connection
///     .Command("SELECT Id, Title FROM Posts ORDER BY Id")
///     .Map&lt;Post&gt;()
///     .FirstOrDefaultAsync(cancellationToken);
/// </code>
/// </example>
/// <seealso cref="IMappedQuery{T}"/>
/// <seealso cref="MappedQuery{T}"/>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Convenience and style.")]
public static class MappedQueryExtensions
{

	/// <summary>
	/// Asynchronously executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// Use this overload when the handler does not need the
	/// cancellation token.
	/// </summary>
	/// <inheritdoc
	/// cref="Synchronous.IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-items']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask}, CancellationToken)"/>
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask> handler, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();
		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <summary>
	/// Asynchronously executes the query, passes the mapped rows to
	/// <paramref name="handler"/> as they are read,
	/// and returns the handler's result;
	/// the connection is held while each row is processed.
	/// Use this overload when the handler does not need the
	/// cancellation token.
	/// </summary>
	/// <inheritdoc
	/// cref="Synchronous.IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-result']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-items']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-task-result']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
	/// <seealso cref="IMappedQuery{T}.ReadAsync{TResult}(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask{TResult}}, CancellationToken)"/>
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask<TResult>> handler, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="List{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces a list of all the rows, in the order the database returned them.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<List<T>> ToListAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToListAsync(cancellationToken)
			: query.ReadAsync(
				(items, token) => CollectListAsync(items, token),
				cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads every mapped row into a new array.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces an array of all the rows, in the order the database returned them.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <para>
	/// A result with rows is a new array, exactly as long as the number of rows, and nothing else refers to it: keep it, change it or hand it on.
	/// <see cref="ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/> wraps such an array without copying it on .NET 8 and later (the netstandard builds copy it once).
	/// An empty result is the shared empty array, which has nothing to change.
	/// </para>
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T[]> ToArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToArrayAsync(cancellationToken)
			: query.ReadAsync(
				async (items, token) => (await CollectListAsync(items, token).ConfigureAwait(false)).ToArray(),
				cancellationToken);
	}

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
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces an immutable array of all the rows, in the order the database returned them.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})"/>
	/// <seealso cref="ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToImmutableArrayAsync(cancellationToken)
			: query.ReadAsync(
				async (items, token) => await items.ToImmutableArrayAsync(token).ConfigureAwait(false),
				cancellationToken);
	}

	/// <inheritdoc cref="ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public static ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<T, TKey>(query, keySelector, null, cancellationToken);

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>, keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces a dictionary of all the rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <para>
	/// The built-in queries read every row first and add them after the last one, so a duplicate key (or a <see langword="null"/> key) throws only after the whole read.
	/// </para>
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();
		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToDictionaryAsync(keySelector, comparer, cancellationToken)
			: query.ReadAsync(
				(items, token) => CollectDictionaryAsync(items, keySelector, comparer, token),
				cancellationToken);
	}

	/// <inheritdoc cref="ToDictionaryAsync{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public static ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<T, TKey, TValue>(query, keySelector, valueSelector, null, cancellationToken);

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>,
	/// keyed by <paramref name="keySelector"/>, with the value
	/// <paramref name="valueSelector"/> returns for the row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <typeparam name="TValue">The type of the dictionary value.</typeparam>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="valueSelector">Produces the value for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces a dictionary of all the rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="query"/>, <paramref name="keySelector"/> or <paramref name="valueSelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <remarks>
	/// <para>
	/// Each selector is called once per row, key first. The built-in queries read every row first and build the dictionary right after the last one, so a duplicate key (or a <see langword="null"/> key) throws, and the selectors run, only after the whole read.
	/// A duplicate key throws, as it does for <see cref="ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>.
	/// </para>
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		if (valueSelector is null) throw new ArgumentNullException(nameof(valueSelector));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToDictionaryAsync(keySelector, valueSelector, comparer, cancellationToken)
			: query.ReadAsync(
				(items, token) => CollectDictionaryAsync(items, keySelector, valueSelector, comparer, token),
				cancellationToken);
	}

	/// <inheritdoc cref="ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> ToHashSetAsync<T>(query, null, cancellationToken);

	/// <summary>
	/// Asynchronously reads every mapped row into a new <see cref="HashSet{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <param name="comparer">The comparer for the rows, or <see langword="null"/> to use the default comparer.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces a set of the distinct rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})"/>
	/// <seealso cref="ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToHashSetAsync(comparer, cancellationToken)
			: query.ReadAsync(
				(items, token) => CollectHashSetAsync(items, comparer, token),
				cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads the first mapped row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces the first row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <para>
	/// Rows after the first are not mapped. Add an <c>ORDER BY</c> to the SQL when "first" must be deterministic.
	/// </para>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	/// <seealso cref="SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T?> FirstOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.FirstOrDefaultAsync(cancellationToken);
		}

		return query.ReadAsync(
			(items, token) => FirstOrDefaultOfAsync(items, token),
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads the only mapped row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces the single row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns more than one row.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	/// <seealso cref="FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T?> SingleOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.SingleOrDefaultAsync(cancellationToken);
		}

		return query.ReadAsync(
			(items, token) => SingleOrDefaultOfAsync(items, token),
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads the first mapped row; throws if the query returns no rows.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces the first row.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns no rows.</exception>
	/// <remarks>
	/// <para>
	/// Rows after the first are not mapped. Add an <c>ORDER BY</c> to the SQL when "first" must be deterministic.
	/// </para>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-row']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='no-rows']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through-throwing']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	/// <seealso cref="FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="SingleAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T> FirstAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.FirstAsync(cancellationToken);
		}

		return query.ReadAsync(
			(items, token) => FirstOfAsync(items, token),
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads the only mapped row; throws if the query returns no rows or more than one.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>A task that produces the single row.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns no rows, or more than one row.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-row']/*" />
	/// <para>
	/// The provider is not asked for a single row, because it could then hide the second one.
	/// </para>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='no-rows']/*" />
	/// <para>
	/// The more-than-one error is thrown at the second row, before the later result sets are read, so it is never replaced by a later failure.
	/// </para>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through-throwing']/*" />
	/// </remarks>
	/// <seealso cref="Synchronous.SyncMappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	/// <seealso cref="SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="FirstAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public static ValueTask<T> SingleAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.SingleAsync(cancellationToken);
		}

		return query.ReadAsync(
			(items, token) => SingleOfAsync(items, token),
			cancellationToken);
	}

	/// <summary>
	/// Reads a query that is not one of this library's own, through its
	/// <see cref="IMappedQuery{T}.ReadAsync(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask}, CancellationToken)"/>
	/// handler, into a list.
	/// </summary>
	/// <remarks>
	/// This and the helpers below read a foreign <see cref="IMappedQuery{T}"/>
	/// through its handler.
	/// Each enumerates with <c>WithCancellation(token).ConfigureAwait(false)</c>:
	/// the in-box <c>System.Linq.AsyncEnumerable</c> methods await
	/// <c>MoveNextAsync</c> and <c>DisposeAsync</c> without
	/// <c>ConfigureAwait(false)</c>, so they would resume on the caller's
	/// <see cref="SynchronizationContext"/>.
	/// The messages are those of LINQ.
	/// </remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="items">The rows to read.</param>
	/// <param name="token">The cancellation token for the enumeration.</param>
	/// <returns>A list of all the rows.</returns>
	static async ValueTask<List<T>> CollectListAsync<T>(
		IAsyncEnumerable<T> items,
		CancellationToken token)
	{
		var list = new List<T>();
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			list.Add(item);
		}

		return list;
	}

	static async ValueTask<Dictionary<TKey, T>> CollectDictionaryAsync<T, TKey>(
		IAsyncEnumerable<T> items,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken token)
		where TKey : notnull
	{
		var result = new Dictionary<TKey, T>(comparer);
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			result.Add(keySelector(item), item);
		}

		return result;
	}

	static async ValueTask<Dictionary<TKey, TValue>> CollectDictionaryAsync<T, TKey, TValue>(
		IAsyncEnumerable<T> items,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken token)
		where TKey : notnull
	{
		var result = new Dictionary<TKey, TValue>(comparer);
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			result.Add(keySelector(item), valueSelector(item));
		}

		return result;
	}

	static async ValueTask<HashSet<T>> CollectHashSetAsync<T>(
		IAsyncEnumerable<T> items,
		IEqualityComparer<T>? comparer,
		CancellationToken token)
	{
		var result = new HashSet<T>(comparer);
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			result.Add(item);
		}

		return result;
	}

	static async ValueTask<T?> FirstOrDefaultOfAsync<T>(
		IAsyncEnumerable<T> items,
		CancellationToken token)
	{
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			return item;
		}

		return default;
	}

	static async ValueTask<T> FirstOfAsync<T>(
		IAsyncEnumerable<T> items,
		CancellationToken token)
	{
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			return item;
		}

		throw new InvalidOperationException("Sequence contains no elements");
	}

	static async ValueTask<T?> SingleOrDefaultOfAsync<T>(
		IAsyncEnumerable<T> items,
		CancellationToken token)
	{
		T? found = default;
		bool any = false;
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			if (any) throw new InvalidOperationException("Sequence contains more than one element");
			found = item;
			any = true;
		}

		return found;
	}

	static async ValueTask<T> SingleOfAsync<T>(
		IAsyncEnumerable<T> items,
		CancellationToken token)
	{
		T? found = default;
		bool any = false;
		await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
		{
			if (any) throw new InvalidOperationException("Sequence contains more than one element");
			found = item;
			any = true;
		}

		return any ? found! : throw new InvalidOperationException("Sequence contains no elements");
	}
}
