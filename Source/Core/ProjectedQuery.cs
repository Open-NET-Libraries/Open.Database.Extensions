namespace Open.Database.Extensions;

// What MappedQueryExtensions.Select returns: a lazy projection of another
// query. It holds the source and the selector and runs nothing itself.
// Each terminal hands the handler a stream of the source's rows, projected one
// at a time as the handler enumerates it, so a projection of a projection
// still reads through one reader.
// A caller sees only IMappedQuery<T>, so this type stays internal.
sealed class ProjectedQuery<TSource, T>(
	IMappedQuery<TSource> source,
	Func<TSource, T> selector)
	: IMappedQuery<T>
{
	public void Read(Action<IEnumerable<T>> handler)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		source.Read(rows => handler(Project(rows, selector)));
	}

	public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return source.Read(rows => handler(Project(rows, selector)));
	}

	public ValueTask ReadAsync(
		Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler,
		CancellationToken cancellationToken)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return source.ReadAsync(
			(rows, token) => handler(ProjectAsync(rows, selector, token), token),
			cancellationToken);
	}

	public ValueTask<TResult> ReadAsync<TResult>(
		Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler,
		CancellationToken cancellationToken)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return source.ReadAsync(
			(rows, token) => handler(ProjectAsync(rows, selector, token), token),
			cancellationToken);
	}

	// The foreach disposes the source's enumerator when this one is disposed,
	// so a handler that stops early still releases the reader.
	static IEnumerable<T> Project(
		IEnumerable<TSource> rows,
		Func<TSource, T> selector)
	{
		foreach (TSource row in rows)
		{
			yield return selector(row);
		}
	}

	static async IAsyncEnumerable<T> ProjectAsync(
		IAsyncEnumerable<TSource> rows,
		Func<TSource, T> selector,
		[EnumeratorCancellation] CancellationToken cancellationToken)
	{
		await foreach (TSource row in rows
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			yield return selector(row);
		}
	}
}
