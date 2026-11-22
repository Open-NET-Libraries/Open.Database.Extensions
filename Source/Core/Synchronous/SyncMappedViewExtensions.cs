namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking terminals on a mapped query or command view (<c>Map&lt;T&gt;()</c> on a command or a query definition, with or without <c>Skip</c> and <c>Take</c>):
/// they hold the calling thread until the rows are read.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
/// <remarks>
/// A view reads through its own fast path; these extensions only call it.
/// A reader or a <c>DataTable</c> view (<c>MappedReader&lt;T&gt;</c>, <c>MappedTable&lt;T&gt;</c>) keeps its own terminals.
/// </remarks>
public static class SyncMappedViewExtensions
{
	#region MappedQuery

	/// <inheritdoc cref="IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"/>
	public static void Read<T>(this MappedQuery<T> query, Action<IEnumerable<T>> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="IMappedQuerySync{T}.Read{TResult}(Func{IEnumerable{T}, TResult})"/>
	public static TResult Read<T, TResult>(this MappedQuery<T> query, Func<IEnumerable<T>, TResult> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static List<T> ToList<T>(this MappedQuery<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static T[] ToArray<T>(this MappedQuery<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedQuery<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedQuery<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public static T First<T>(this MappedQuery<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public static T Single<T>(this MappedQuery<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQuery<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQuery<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQuery<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static HashSet<T> ToHashSet<T>(this MappedQuery<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedQueryWithSkip

	/// <inheritdoc cref="IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"/>
	public static void Read<T>(this MappedQueryWithSkip<T> query, Action<IEnumerable<T>> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="IMappedQuerySync{T}.Read{TResult}(Func{IEnumerable{T}, TResult})"/>
	public static TResult Read<T, TResult>(this MappedQueryWithSkip<T> query, Func<IEnumerable<T>, TResult> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static List<T> ToList<T>(this MappedQueryWithSkip<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static T[] ToArray<T>(this MappedQueryWithSkip<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedQueryWithSkip<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedQueryWithSkip<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public static T First<T>(this MappedQueryWithSkip<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public static T Single<T>(this MappedQueryWithSkip<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQueryWithSkip<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQueryWithSkip<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQueryWithSkip<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static HashSet<T> ToHashSet<T>(this MappedQueryWithSkip<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedQueryWithTake

	/// <inheritdoc cref="IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"/>
	public static void Read<T>(this MappedQueryWithTake<T> query, Action<IEnumerable<T>> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="IMappedQuerySync{T}.Read{TResult}(Func{IEnumerable{T}, TResult})"/>
	public static TResult Read<T, TResult>(this MappedQueryWithTake<T> query, Func<IEnumerable<T>, TResult> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static List<T> ToList<T>(this MappedQueryWithTake<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static T[] ToArray<T>(this MappedQueryWithTake<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedQueryWithTake<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedQueryWithTake<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public static T First<T>(this MappedQueryWithTake<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public static T Single<T>(this MappedQueryWithTake<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedQueryWithTake<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedQueryWithTake<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedQueryWithTake<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static HashSet<T> ToHashSet<T>(this MappedQueryWithTake<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion

	#region MappedCommand

	/// <inheritdoc cref="IMappedQuerySync{T}.Read(Action{IEnumerable{T}})"/>
	public static void Read<T>(this MappedCommand<T> query, Action<IEnumerable<T>> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="IMappedQuerySync{T}.Read{TResult}(Func{IEnumerable{T}, TResult})"/>
	public static TResult Read<T, TResult>(this MappedCommand<T> query, Func<IEnumerable<T>, TResult> handler)
		=> query.Read(handler);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static List<T> ToList<T>(this MappedCommand<T> query)
		=> query.ToList();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static T[] ToArray<T>(this MappedCommand<T> query)
		=> query.ToArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public static T? FirstOrDefault<T>(this MappedCommand<T> query)
		=> query.FirstOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public static T? SingleOrDefault<T>(this MappedCommand<T> query)
		=> query.SingleOrDefault();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public static T First<T>(this MappedCommand<T> query)
		=> query.First();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public static T Single<T>(this MappedCommand<T> query)
		=> query.Single();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static ImmutableArray<T> ToImmutableArray<T>(this MappedCommand<T> query)
		=> query.ToImmutableArray();

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this MappedCommand<T> query,
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(
		this MappedCommand<T> query,
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="Synchronous.SyncMappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public static HashSet<T> ToHashSet<T>(this MappedCommand<T> query, IEqualityComparer<T>? comparer = null)
		=> query.ToHashSet(comparer);

	#endregion
}
