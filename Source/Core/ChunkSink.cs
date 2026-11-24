namespace Open.Database.Extensions.Core;

/// <summary>
/// The producer's row sink: the read loop hands each mapped row to
/// <see cref="Add"/>, which writes it straight into the producer's current
/// buffer.
/// </summary>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="producer">The producer that receives the rows.</param>
// The read loop is RowReader.FillAsync.
// This is a struct over the producer (a reference), so the loop specializes
// and inlines the call, and a copy of it (the loop passes the sink by value
// across awaits) shares the one producer.
internal readonly struct ChunkSink<T>(ChunkProducer<T> producer)
	: IRowSink<T>
{
	/// <summary>Stores a row in the producer's current buffer.</summary>
	/// <param name="item">The mapped row.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
		=> producer.Add(item);

	/// <summary>Gets the number of rows the producer has stored.</summary>
	public int Count
		=> producer.Count;
}
