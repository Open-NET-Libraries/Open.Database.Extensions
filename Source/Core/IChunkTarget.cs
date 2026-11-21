namespace Open.Database.Extensions.Core;

// The unit of every chunk is an ArrayLease<T>: a pooled array and the rows
// written to it, exactly the rows of one buffer. Releasing it returns the
// array to its pool (after clearing the rows, when that applies).

/// <summary>Where the producer hands its chunks.</summary>
internal interface IChunkTarget<T>
{
	/// <summary>
	/// True once the owner disposed the target: the producer
	/// then stops and what it was cancelled with is not an error.
	/// </summary>
	bool IsDisposed { get; }

	/// <summary>The size of the array that holds chunk number index.</summary>
	int CapacityOf(int index);

	/// <summary>
	/// Takes over a chunk. False means refused (disposed):
	/// the chunk stays the producer's, which returns it.
	/// </summary>
	bool Publish(ArrayLease<T> chunk);

	/// <summary>
	/// The producer ended; the error is null for an end that is not
	/// a failure (a complete read, or a cancel the owner asked for).
	/// </summary>
	void Finish(Exception? error);
}
