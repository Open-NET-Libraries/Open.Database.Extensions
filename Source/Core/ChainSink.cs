namespace Open.Database.Extensions.Core;

/// <summary>
/// The chain's row sink: the read loop hands each mapped row to
/// <see cref="Add"/>, which stores it in the chain.
/// </summary>
/// <remarks>
/// The read loop is <c>RowReader.FillAsync</c>.
/// This is a struct over a reference, so the loop specializes and inlines the
/// call, and a copy of it (the loop passes the sink by value across awaits)
/// shares the one chain.
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="chain">The chain that receives the rows.</param>
internal readonly struct ChainSink<T>(ChunkChain<T> chain)
	: IRowSink<T>
{
	/// <summary>Stores a row in the chain.</summary>
	/// <param name="item">The mapped row.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(T item)
		=> chain.Add(item);
}
