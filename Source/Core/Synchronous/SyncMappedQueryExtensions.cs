namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking terminals on any <see cref="IMappedQuery{T}"/> that implements <see cref="IMappedQuerySync{T}"/>: they hold the calling thread until every row is read.
/// </summary>
public static class SyncMappedQueryExtensions
{
	/// <summary>
	/// Reads every mapped row into a new <see cref="List{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>A list of all the rows, in the order the database returned them. The list is empty if there are no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	public static List<T> ToList<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		// Our own query collects with its own pool and clear rule; a query that is
		// not ours has none, so it is collected without pooling.
		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToList()
			: query.AsSync().Read(items => items.ToList());
	}

	/// <summary>
	/// Reads every mapped row into a new array.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>An array of all the rows, in the order the database returned them. The array is empty if there are no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="ToList{T}(IMappedQuery{T})"/>
	public static T[] ToArray<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToArray()
			: query.AsSync().Read(items => items.ToArray());
	}

	/// <summary>
	/// Reads every mapped row into a new <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>An immutable array of all the rows, in the order the database returned them.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="ToArray{T}(IMappedQuery{T})"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToImmutableArray()
			: query.AsSync().Read(items => items.ToImmutableArray());
	}

	/// <summary>
	/// Reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>, keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A dictionary of all the rows. It is empty if there are no rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="query"/> or <paramref name="keySelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <para>
	/// The built-in queries read every row first and add them after the last one, so a duplicate key (or a <see langword="null"/> key) throws only after the whole read.
	/// </para>
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	/// <seealso cref="ToList{T}(IMappedQuery{T})"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this IMappedQuery<T> query, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToDictionary(keySelector, comparer)
			: query.AsSync().Read(items => items.ToDictionary(keySelector, comparer));
	}

	/// <summary>
	/// Reads every mapped row into a new <see cref="Dictionary{TKey, TValue}"/>,
	/// keyed by <paramref name="keySelector"/>, with the value
	/// <paramref name="valueSelector"/> returns for the row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <typeparam name="TValue">The type of the dictionary value.</typeparam>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="valueSelector">Produces the value for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A dictionary of all the rows. It is empty if there are no rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="query"/>, <paramref name="keySelector"/> or <paramref name="valueSelector"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two rows produce the same key.</exception>
	/// <remarks>
	/// <para>
	/// Each selector is called once per row, key first. The built-in queries read every row first and build the dictionary right after the last one, so a duplicate key (or a <see langword="null"/> key) throws, and the selectors run, only after the whole read.
	/// A duplicate key throws, as it does for <see cref="ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/> and for LINQ's <c>ToDictionary</c>.
	/// </para>
	/// <para>
	/// The selectors run while the reader is open, so keep them quick and do not touch the connection from inside them.
	/// </para>
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/>
	/// <seealso cref="ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this IMappedQuery<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		if (valueSelector is null) throw new ArgumentNullException(nameof(valueSelector));
		Contract.EndContractBlock();

		return query is IMappedTerminals<T> terminals
			? terminals.Core.ToDictionary(keySelector, valueSelector, comparer)
			: query.AsSync().Read(
				items => items.ToDictionary(keySelector, valueSelector, comparer));
	}

	/// <summary>
	/// Reads every mapped row into a new <see cref="HashSet{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <param name="comparer">The comparer for the rows, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A set of the distinct rows. Rows that compare equal to an earlier row are dropped.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)"/>
	/// <seealso cref="ToList{T}(IMappedQuery{T})"/>
	public static HashSet<T> ToHashSet<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer = null)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
			return terminals.Core.ToHashSet(comparer);

#if NETSTANDARD2_0
		var result = new HashSet<T>(comparer);
		return query.AsSync().Read(items =>
		{
			foreach (var item in items)
			{
				result.Add(item);
			}

			return result;
		});
#else
		return query.AsSync().Read(items => items.ToHashSet(comparer));
#endif
	}

	/// <summary>
	/// Reads the first mapped row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The first row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <para>
	/// Rows after the first are not mapped. Add an <c>ORDER BY</c> to the SQL when "first" must be deterministic.
	/// </para>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.FirstOrDefault();
		}

		return query.AsSync().Read(items => items.FirstOrDefault());
	}

	/// <summary>
	/// Reads the only mapped row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The single row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns more than one row.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='terminal-through']/*" />
	/// </remarks>
	/// <seealso cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.SingleOrDefault();
		}

		return query.AsSync().Read(items => items.SingleOrDefault());
	}

	/// <summary>
	/// Reads the first mapped row; throws if the query returns no rows.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The first row.</returns>
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
	/// <seealso cref="MappedQueryExtensions.FirstAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="FirstOrDefault{T}(IMappedQuery{T})"/>
	/// <seealso cref="Single{T}(IMappedQuery{T})"/>
	public static T First<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.First();
		}

		return query.AsSync().Read(items => items.First());
	}

	/// <summary>
	/// Reads the only mapped row; throws if the query returns no rows or more than one.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The single row.</returns>
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
	/// <seealso cref="MappedQueryExtensions.SingleAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// <seealso cref="SingleOrDefault{T}(IMappedQuery{T})"/>
	/// <seealso cref="First{T}(IMappedQuery{T})"/>
	public static T Single<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		if (query is IMappedTerminals<T> terminals)
		{
			return terminals.Single();
		}

		return query.AsSync().Read(items => items.Single());
	}
}
