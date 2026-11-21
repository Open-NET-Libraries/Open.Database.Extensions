namespace Open.Database.Extensions.Core;

/// <summary>
/// The producer's row sink: the read loop hands each mapped row to
/// <see cref="Add"/>, which writes it straight into the producer's current
/// buffer.
/// </summary>
/// <remarks>
/// The read loop is <c>RowReader.FillAsync</c>.
/// This is a struct over the producer (a reference), so the loop specializes
/// and inlines the call, and a copy of it (the loop passes the sink by value
/// across awaits) shares the one producer.
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="producer">The producer that receives the rows.</param>
internal readonly struct ChunkSink<T>(ChunkProducer<T> producer)
	: IRowSink<T>
{
	/// <summary>Stores a row in the producer's current buffer.</summary>
	/// <param name="item">The mapped row.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
		=> producer.Add(item);
}
