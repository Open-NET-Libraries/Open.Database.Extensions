namespace Open.Database.Extensions.Core;

// ToBufferedAsyncStream: the chunk stream above, flattened. Each
// element is copied into a local before it is yielded, so no span
// lives across a yield and the chunk's array goes back to the pool
// at the next chunk step, as for the chunk stream.
internal sealed class BufferedStream<T>(
	MappedQueryCore<T> query,
	MemoryPool<T> pool,
	CancellationToken caller)
	: IAsyncEnumerable<T>
{
	readonly BufferStream<T> _chunks = new(query, pool, caller);

	public IAsyncEnumerator<T> GetAsyncEnumerator(
		CancellationToken cancellationToken = default)
		// Throws at the call when the stream was already taken.
		=> Flatten(_chunks.GetAsyncEnumerator(cancellationToken));

	static async IAsyncEnumerator<T> Flatten(
		IAsyncEnumerator<ReadOnlyMemory<T>> chunks)
	{
		await using (chunks.ConfigureAwait(false))
		{
			while (await chunks.MoveNextAsync().ConfigureAwait(false))
			{
				ReadOnlyMemory<T> memory = chunks.Current;
				for (int i = 0; i < memory.Length; ++i)
				{
					T item = memory.Span[i];
					yield return item;
				}
			}
		}
	}
}
