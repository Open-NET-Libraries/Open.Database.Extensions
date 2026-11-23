namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking terminals on a mapped query or command view (<c>Map&lt;T&gt;()</c> on a command or a query definition, with or without <c>Skip</c> and <c>Take</c>), which hold the calling thread until the rows are read.
/// </summary>
/// <remarks>
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// A reader or a <c>DataTable</c> view (<c>MappedReader&lt;T&gt;</c>, <c>MappedTable&lt;T&gt;</c>) keeps its own terminals.
/// </remarks>
public static class SyncMappedViewExtensions
{
	#region MappedQuery

	/// <summary>
	/// Reads every mapped row into a new <see cref="List{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>A list of all the rows, in the order the database returned them. The list is empty if there are no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToListAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToArray()"/>
	public static List<T> ToList<T>(this MappedQuery<T> query)
		=> query.ToList();

	/// <summary>
	/// Reads every mapped row into a new array.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>An array of all the rows, in the order the database returned them. The array is empty if there are no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToArrayAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToList()"/>
	public static T[] ToArray<T>(this MappedQuery<T> query)
		=> query.ToArray();

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
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.FirstOrDefaultAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.SingleOrDefault()"/>
	public static T? FirstOrDefault<T>(this MappedQuery<T> query)
		=> query.FirstOrDefault();

	/// <summary>
	/// Reads the only mapped row.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The single row, or <see langword="default"/> if the query returns no rows.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns more than one row.</exception>
	/// <seealso cref="MappedQuery{T}.SingleOrDefaultAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.FirstOrDefault()"/>
	public static T? SingleOrDefault<T>(this MappedQuery<T> query)
		=> query.SingleOrDefault();

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
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.FirstAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.FirstOrDefault()"/>
	/// <seealso cref="MappedQuery{T}.Single()"/>
	public static T First<T>(this MappedQuery<T> query)
		=> query.First();

	// The provider is not asked for a single row, because it could then hide the second one.
	/// <summary>
	/// Reads the only mapped row; throws if the query returns no rows or more than one.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>The single row.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <exception cref="InvalidOperationException">The query returns no rows, or more than one row.</exception>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-row']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='no-rows']/*" />
	/// <para>
	/// The more-than-one error is thrown at the second row, before the later result sets are read, so it is never replaced by a later failure.
	/// </para>
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.SingleAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.SingleOrDefault()"/>
	/// <seealso cref="MappedQuery{T}.First()"/>
	public static T Single<T>(this MappedQuery<T> query)
		=> query.Single();

	/// <summary>
	/// Reads every mapped row into a new <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <returns>An immutable array of all the rows, in the order the database returned them.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToImmutableArrayAsync(CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToArray()"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQuery<T> query)
		=> query.ToImmutableArray();

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
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToList()"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQuery<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

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
	/// Each selector is called once per row, key first.
	/// The built-in queries read every row first and build the dictionary right after the last one, while the reader is still open:
	/// a duplicate key (or a <see langword="null"/> key) throws, and the selectors run, only then, as it does for <see cref="MappedQuery{T}.ToDictionary{TKey}(Func{T, TKey}, IEqualityComparer{TKey})"/> and for LINQ's <c>ToDictionary</c>.
	/// Keep the selectors quick and do not touch the connection from inside them.
	/// </para>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToDictionary{TKey}(Func{T, TKey}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQuery<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <summary>
	/// Reads every mapped row into a new <see cref="HashSet{T}"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query']/*" />
	/// <param name="comparer">The comparer for the rows, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A set of the distinct rows. Rows that compare equal to an earlier row are dropped.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='query-null']/*" />
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='buffered']/*" />
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <seealso cref="MappedQuery{T}.ToHashSetAsync(IEqualityComparer{T}, CancellationToken)"/>
	/// <seealso cref="MappedQuery{T}.ToList()"/>
	public static HashSet<T> ToHashSet<T>(this MappedQuery<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedQueryWithSkip

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToList{T}(MappedQuery{T})"/>
	public static List<T> ToList<T>(this MappedQueryWithSkip<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToArray{T}(MappedQuery{T})"/>
	public static T[] ToArray<T>(this MappedQueryWithSkip<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.FirstOrDefault{T}(MappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedQueryWithSkip<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.SingleOrDefault{T}(MappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedQueryWithSkip<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.First{T}(MappedQuery{T})"/>
	public static T First<T>(this MappedQueryWithSkip<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.Single{T}(MappedQuery{T})"/>
	public static T Single<T>(this MappedQueryWithSkip<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToImmutableArray{T}(MappedQuery{T})"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQueryWithSkip<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey}(MappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQueryWithSkip<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey, TValue}(MappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQueryWithSkip<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToHashSet{T}(MappedQuery{T}, IEqualityComparer{T})"/>
	public static HashSet<T> ToHashSet<T>(this MappedQueryWithSkip<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedQueryWithTake

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToList{T}(MappedQuery{T})"/>
	public static List<T> ToList<T>(this MappedQueryWithTake<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToArray{T}(MappedQuery{T})"/>
	public static T[] ToArray<T>(this MappedQueryWithTake<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.FirstOrDefault{T}(MappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedQueryWithTake<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.SingleOrDefault{T}(MappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedQueryWithTake<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.First{T}(MappedQuery{T})"/>
	public static T First<T>(this MappedQueryWithTake<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.Single{T}(MappedQuery{T})"/>
	public static T Single<T>(this MappedQueryWithTake<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToImmutableArray{T}(MappedQuery{T})"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQueryWithTake<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey}(MappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQueryWithTake<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey, TValue}(MappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQueryWithTake<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToHashSet{T}(MappedQuery{T}, IEqualityComparer{T})"/>
	public static HashSet<T> ToHashSet<T>(this MappedQueryWithTake<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedCommand

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToList{T}(MappedQuery{T})"/>
	public static List<T> ToList<T>(this MappedCommand<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToArray{T}(MappedQuery{T})"/>
	public static T[] ToArray<T>(this MappedCommand<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.FirstOrDefault{T}(MappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedCommand<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.SingleOrDefault{T}(MappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedCommand<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.First{T}(MappedQuery{T})"/>
	public static T First<T>(this MappedCommand<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.Single{T}(MappedQuery{T})"/>
	public static T Single<T>(this MappedCommand<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToImmutableArray{T}(MappedQuery{T})"/>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedCommand<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey}(MappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedCommand<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToDictionary{T, TKey, TValue}(MappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})"/>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedCommand<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedViewExtensions.ToHashSet{T}(MappedQuery{T}, IEqualityComparer{T})"/>
	public static HashSet<T> ToHashSet<T>(this MappedCommand<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion
}
