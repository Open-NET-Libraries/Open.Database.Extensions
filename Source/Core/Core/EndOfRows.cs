namespace Open.Database.Extensions;

/// <summary>
/// For the pipelines that take the rows of a reader as a sequence: once the sequence has run to its end, the reader is moved past the result set (<c>NextResult</c>),
/// which is what makes a provider raise an error that follows the rows. A sequence that stops early (the target closed, the pipeline stopped, the caller cancelled) is left alone.
/// </summary>
internal static class EndOfRows
{
	public static IEnumerable<T> ThenNextResult<T>(this IEnumerable<T> rows, IDataReader reader, CancellationToken cancellationToken = default)
	{
		foreach (T row in rows)
			yield return row;

		if (!cancellationToken.IsCancellationRequested) reader.NextResult();
	}

	public static async IAsyncEnumerable<T> ThenNextResultAsync<T>(this IAsyncEnumerable<T> rows, DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		await foreach (T row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
			yield return row;

		if (!cancellationToken.IsCancellationRequested) await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
	}
}
