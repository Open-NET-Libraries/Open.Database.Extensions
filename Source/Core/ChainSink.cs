namespace Open.Database.Extensions.Core;

// The chain's row sink: the read loop (RowReader.FillAsync) hands each
// mapped row to Add, which stores it in the chain. A struct over a
// reference, so the loop specializes and inlines the call, and a copy of
// it (the loop passes the sink by value across awaits) shares the one chain.
internal readonly struct ChainSink<T>(ChunkChain<T> chain)
	: IRowSink<T>
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
		=> chain.Add(item);
}
