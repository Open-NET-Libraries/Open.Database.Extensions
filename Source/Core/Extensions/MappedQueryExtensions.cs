namespace Open.Database.Extensions;

/// <summary>
/// Extensions for <see cref="IMappedQuery{T}"/>.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Convenience and style.")]
public static class MappedQueryExtensions
{
	/// <inheritdoc cref="IMappedQuery{T}.ReadAsync(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask}, CancellationToken)"/>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="query"/> or <paramref name="handler"/>is null.</exception>
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask> handler, CancellationToken cancellationToken = default)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();
		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <inheritdoc cref="IMappedQuery{T}.ReadAsync{TResult}(Func{IAsyncEnumerable{T}, CancellationToken, ValueTask{TResult}}, CancellationToken)"/>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="query"/> or <paramref name="handler"/>is null.</exception>
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, Func<IAsyncEnumerable<T>, ValueTask<TResult>> handler, CancellationToken cancellationToken = default)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return query.ReadAsync((items, _) => handler(items), cancellationToken);
	}

	/// <inheritdoc cref="ReadAsync{T}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask}, CancellationToken)"/>
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, ValueTask> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="ReadAsync{T, TResult}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask{TResult}}, CancellationToken)"/>
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, ValueTask<TResult>> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="ReadAsync{T}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask}, CancellationToken)"/>
	public static ValueTask ReadAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="ReadAsync{T, TResult}(IMappedQuery{T}, Func{IAsyncEnumerable{T}, ValueTask{TResult}}, CancellationToken)"/>
	public static ValueTask<TResult> ReadAsync<T, TResult>(this IMappedQuery<T> query, CancellationToken cancellationToken, Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler)
		=> query.ReadAsync(handler, cancellationToken);

	/// <summary>
	/// Creates a <see cref="List{T}"/> from the results of the query.
	/// </summary>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="query"/> is null.</exception>
	public static List<T> ToList<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.ToList());

	/// <inheritdoc cref="ToList{T}(IMappedQuery{T})"/>
	public static ValueTask<List<T>> ToListAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(cancellationToken,
			async (items, token) => await items.ToListAsync(token).ConfigureAwait(false));

	/// <summary>Creates an array from the results of the query.</summary>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="query"/> is null.</exception>
	public static T[] ToArray<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.ToArray());

	/// <inheritdoc cref="ToArray{T}(IMappedQuery{T})"/>
	public static ValueTask<T[]> ToArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(cancellationToken,
			async (items, token) => await items.ToArrayAsync(token).ConfigureAwait(false));

	/// <summary>Creates an <see cref="ImmutableArray{T}"/> from the results of the query.</summary>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="query"/> is null.</exception>
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

	/// <inheritdoc cref="ToImmutableArray{T}(IMappedQuery{T})"/>
	public static ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default) => query.ReadAsync(cancellationToken,
			async (items, token) => await items.ToImmutableArrayAsync(token).ConfigureAwait(false));

	/// <summary>Creates a <see cref="Dictionary{TKey, TValue}"/> from the results of the query.</summary>
	public static Dictionary<TKey, T> ToDictionary<T, TKey>(
		this IMappedQuery<T> query, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> query.Read(items => items.ToDictionary(keySelector, comparer));

	/// <inheritdoc cref="ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
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
		return query.ReadAsync(cancellationToken,
			async (items, token) => await items.ToDictionaryAsync(keySelector, comparer, token).ConfigureAwait(false));
	}

	/// <summary>Creates a <see cref="HashSet{T}"/> from the results of the query.</summary>
	public static HashSet<T> ToHashSet<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer = null)
	{
#if NETSTANDARD2_0
		var result = new HashSet<T>(comparer);
		return query.Read(items => {
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

	/// <inheritdoc cref="ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})"/>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(this IMappedQuery<T> query, IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default)
	{
#if NETSTANDARD2_0
		var result = new HashSet<T>(comparer);
		return query.ReadAsync(cancellationToken, async (items, token) => {
			await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
			{
				result.Add(item);
			}

			return result;
		});
#else
		return query.ReadAsync(cancellationToken, (items, token) => items.ToHashSetAsync(comparer, token));
#endif
	}

	/// <summary>Returns the first element of the query, or a default value if no element is found.</summary>
	public static T? FirstOrDefault<T>(this IMappedQuery<T> query)
	{
		if (query is null) throw new ArgumentNullException(nameof(query));
		Contract.EndContractBlock();

		return query.Read(items => items.FirstOrDefault());
	}

	/// <inheritdoc cref="FirstOrDefault{T}(IMappedQuery{T})"/>
	public static ValueTask<T?> FirstOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(cancellationToken,
			async (items, token) => await items.FirstOrDefaultAsync(token).ConfigureAwait(false));

	/// <summary>Returns the single element of the query, or a default value if no element is found.</summary>
	public static T? SingleOrDefault<T>(this IMappedQuery<T> query)
		=> query.Read(items => items.SingleOrDefault());

	/// <inheritdoc cref="SingleOrDefault{T}(IMappedQuery{T})"/>
	public static ValueTask<T?> SingleOrDefaultAsync<T>(this IMappedQuery<T> query, CancellationToken cancellationToken = default)
		=> query.ReadAsync(cancellationToken,
			async (items, token) => await items.SingleOrDefaultAsync(token).ConfigureAwait(false));
}
