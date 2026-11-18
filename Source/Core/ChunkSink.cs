namespace Open.Database.Extensions.Core;

// The producer's row sink: the read loop (RowReader.FillAsync) hands each
// mapped row to Add, which writes it straight into the producer's current
// buffer. A struct over the producer (a reference), so the loop
// specializes and inlines the call, and a copy of it (the loop passes the
// sink by value across awaits) shares the one producer.
internal readonly struct ChunkSink<T>(ChunkProducer<T> producer)
	: IRowSink<T>
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
		=> producer.Add(item);
}
