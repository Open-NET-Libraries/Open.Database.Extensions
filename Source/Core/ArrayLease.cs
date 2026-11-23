namespace Open.Database.Extensions.Core;

/// <summary>
/// What happens to the rows of a chunk when it is released.
/// </summary>
/// <remarks>
/// Decided once, when the chunk is sealed and the number of rows it holds
/// is known (<see cref="ChunkConfig{T}.Seal"/>), so releasing it is a
/// switch and nothing else.
/// </remarks>
internal enum ClearMode : byte
{
	/// <summary>Nothing is cleared: the array goes straight back.</summary>
	None,

	/// <summary>
	/// The rows are cleared where the release happens, then the array goes back.
	/// </summary>
	// Small chunks: cheaper than a hand-off.
	Immediate,

	/// <summary>
	/// The array is handed to the recycle bin, which clears the rows and gives
	/// the array back off the releasing thread.
	/// </summary>
	// Large chunks.
	Deferred,
}

/// <summary>
/// A pooled array and the number of rows written to it, released once.
/// </summary>
/// <remarks>
/// The array may be longer than the rows: only <see cref="Used"/> slots are read
/// or cleared.
/// Release the lease exactly once, by the party that holds it last: a copy shares
/// the array, so releasing two copies returns the array twice and hands the same
/// array to two renters.
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="array">The pooled array.</param>
/// <param name="used">How many slots, from the start, hold rows.</param>
/// <param name="config">The configuration the array came from (its pool).</param>
/// <param name="mode">What to do with the rows on release.</param>
// A readonly struct, so it costs no allocation: the array, how many of its
// slots hold rows, the configuration it came from (the pool it goes back
// to) and what to do with the rows on release. There is no interface and
// no owner object.
// The holder keeps the once-only guard: a struct cannot set itself to null.
internal readonly struct ArrayLease<T>(
	T[] array,
	int used,
	ChunkConfig<T> config,
	ClearMode mode)
{
	/// <summary>The pooled array.</summary>
	public T[] Array { get; } = array;

	/// <summary>How many slots, from the start, hold rows.</summary>
	public int Used { get; } = used;

	/// <summary>The rows, as memory.</summary>
	public ReadOnlyMemory<T> Memory
		=> new(Array, 0, Used);

	/// <summary>The rows, as a span.</summary>
	public ReadOnlySpan<T> Span
		=> new(Array, 0, Used);

	/// <summary>
	/// Gives the array back to its pool, after the rows are cleared if
	/// they are to be (see <see cref="ClearMode"/>).
	/// </summary>
	/// <remarks>
	/// A pool whose <c>Return</c> throws, throws from the release, except from a
	/// deferred one that the recycle bin took.
	/// </remarks>
	public void Release()
	{
		switch (mode)
		{
			case ClearMode.None:
				config.Pool.Return(Array, clearArray: false);
				break;

			case ClearMode.Immediate:
				config.ClearAndReturn(Array, Used);
				break;

			default:
				// A bin that cannot take it (full, or broken) is not a reason to
				// keep dirty rows: the clear runs here instead. Never a dirty
				// array to a pool, and no one waits on the loop.
				if (config.Bin.Post(
					new RecycleBin.Work(
						ChunkRunner<T>.Instance,
						Array,
						Used,
						config)) == RecycleBin.PostResult.Full)
				{
					config.ClearAndReturn(Array, Used);
				}

				break;
		}
	}
}

/// <summary>The one runner of each row type.</summary>
/// <include file="Internals.docs.xml" path="docs/members[@name='row']/*" />
// One cached instance for each row type: no allocation per post.
internal sealed class ChunkRunner<T> : RecycleBin.IRunner
{
	/// <summary>The one runner of this <typeparamref name="T"/>.</summary>
	public static readonly ChunkRunner<T> Instance = new();

	/// <summary>Clears the first <paramref name="used"/> rows and gives the array back.</summary>
	/// <param name="array">The array to clear and return.</param>
	/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
	/// <param name="config">The <see cref="ChunkConfig{T}"/> the array came from.</param>
	public void Run(Array array, int used, object config)
		=> ((ChunkConfig<T>)config).ClearAndReturn((T[])array, used);
}
