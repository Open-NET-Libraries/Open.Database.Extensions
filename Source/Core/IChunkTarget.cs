namespace Open.Database.Extensions.Core;

/// <summary>
/// Where the producer hands its chunks.
/// </summary>
/// <typeparam name="T">The type of the rows in a chunk.</typeparam>
/// <remarks>
/// The unit of every chunk is an <see cref="ArrayLease{T}"/>: a pooled array and the rows
/// written to it, exactly the rows of one buffer. Releasing it returns the
/// array to its pool (after clearing the rows, when that applies).
/// </remarks>
internal interface IChunkTarget<T>
{
	/// <summary>
	/// Gets a value that indicates whether the owner disposed the target.
	/// </summary>
	/// <value>
	/// <see langword="true"/> once the owner disposed the target; otherwise, <see langword="false"/>.
	/// </value>
	/// <remarks>
	/// The producer then stops, and what it was canceled with is not an error.
	/// </remarks>
	bool IsDisposed { get; }

	/// <summary>
	/// Returns the size of the array that holds a chunk.
	/// </summary>
	/// <param name="index">The number of the chunk.</param>
	/// <returns>The size of the array that holds chunk number <paramref name="index"/>.</returns>
	int CapacityOf(int index);

	/// <summary>
	/// Takes over a chunk.
	/// </summary>
	/// <param name="chunk">The chunk to hand over.</param>
	/// <returns>
	/// <see langword="true"/> if the target took over the chunk; <see langword="false"/> if it refused
	/// because it is disposed, in which case the chunk stays the producer's, which returns it.
	/// </returns>
	bool Publish(ArrayLease<T> chunk);

	/// <summary>
	/// Tells the target that the producer ended.
	/// </summary>
	/// <param name="error">
	/// The failure that ended the producer, or <see langword="null"/> for an end that is not
	/// a failure (a complete read, or a cancel the owner asked for).
	/// </param>
	void Finish(Exception? error);
}
