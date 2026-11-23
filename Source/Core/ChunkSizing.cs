#if NET
using System.Numerics;
#endif

namespace Open.Database.Extensions.Core;

/// <summary>
/// The sizes of the chunks the collectors and the buffer terminals rent: a
/// configuration object, taken by the chain, the buffer and the stream.
/// </summary>
/// <remarks>
/// The shipped values are <see cref="Default"/>.
/// A value that does not fit is replaced by its default (a stream maximum is never below the first chunk),
/// so a wrong value cannot corrupt a read.
/// </remarks>
// Taking the sizes as an object lets a test or a benchmark give a call other
// sizes without changing anything global.
// What the arithmetic allows: every buffer chunk is twice the one before it:
// the first has FirstRows rows, the next 2 * FirstRows, and so on. That is
// what keeps the PooledArray<T> indexer O(1): the chunk of row i is
// floor(log2(i / FirstRows + 1)), found with a shift and a log, and the chunk
// starts at FirstRows * (2^k - 1). So FirstRows must be a power of two.
// The stream stops doubling at StreamMaxRows (also a power of two, not below
// FirstRows); a buffer never stops doubling, because its indexer arithmetic is
// the doubling.
// The chain that ToArrayAsync and ToListAsync collect rows in (ChunkChain<T>)
// has no consumer working beside the read, so its schedule is its own: the
// first chunk, the shift (1 doubles, 2 quadruples) and the largest chunk.
internal sealed class ChunkSizes
{
	const int DefaultFirstRows = 16;
	const int DefaultStreamMaxRows = 4096;
	const int DefaultCollectFirstRows = 16;
	const int DefaultCollectShift = 1;
	const int DefaultCollectMaxRows = 8192;

	/// <summary>The shipped sizes.</summary>
	public static readonly ChunkSizes Default = new();

	/// <summary>Creates the sizes; a value that does not fit is replaced by its default.</summary>
	/// <param name="firstRows">
	/// The rows in the first chunk of a buffer or a stream: a power of two, at
	/// most 2^20. The default is 16.
	/// </param>
	/// <param name="streamMaxRows">
	/// The rows at which a stream stops doubling: a power of two, not below
	/// <paramref name="firstRows"/>, at most 2^26. The default is 4,096 (or
	/// <see cref="FirstRows"/>, if that is larger).
	/// </param>
	/// <param name="collectFirstRows">
	/// The rows in the first chunk of the collecting chain: a power of two, at
	/// most 2^20. The default is 16.
	/// </param>
	/// <param name="collectShift">
	/// The doublings per chunk of the chain: 1 to 4. The default is 1.
	/// </param>
	/// <param name="collectMaxRows">
	/// The rows in the largest chunk of the chain: a power of two, at most 2^26.
	/// The default is 8,192.
	/// </param>
	public ChunkSizes(
		int firstRows = DefaultFirstRows,
		int streamMaxRows = DefaultStreamMaxRows,
		int collectFirstRows = DefaultCollectFirstRows,
		int collectShift = DefaultCollectShift,
		int collectMaxRows = DefaultCollectMaxRows)
	{
		FirstRows = ChunkSizing.IsPowerOfTwo(firstRows) && firstRows <= 1 << 20
			? firstRows
			: DefaultFirstRows;

		StreamMaxRows = ChunkSizing.IsPowerOfTwo(streamMaxRows)
			&& streamMaxRows >= FirstRows
			&& streamMaxRows <= 1 << 26
			? streamMaxRows
			: Math.Max(DefaultStreamMaxRows, FirstRows);

		CollectFirstRows = ChunkSizing.IsPowerOfTwo(collectFirstRows)
			&& collectFirstRows <= 1 << 20
			? collectFirstRows
			: DefaultCollectFirstRows;

		CollectShift = (uint)(collectShift - 1) <= 3u
			? collectShift
			: DefaultCollectShift;

		CollectMaxRows = ChunkSizing.IsPowerOfTwo(collectMaxRows)
			&& collectMaxRows <= 1 << 26
			? collectMaxRows
			: DefaultCollectMaxRows;

		LastStreamDoubling = ChunkSizing.Log2(StreamMaxRows / FirstRows);
	}

	/// <summary>Gets the number of rows in the first chunk of a buffer or a stream.</summary>
	public int FirstRows { get; }

	/// <summary>Gets the number of rows at which a stream stops doubling.</summary>
	public int StreamMaxRows { get; }

	/// <summary>Gets the number of rows in the first chunk of the collecting chain.</summary>
	public int CollectFirstRows { get; }

	/// <summary>Gets the number of doublings per chunk of the chain.</summary>
	public int CollectShift { get; }

	/// <summary>Gets the number of rows in the largest chunk of the chain.</summary>
	public int CollectMaxRows { get; }

	/// <summary>
	/// Gets the number of doublings a stream makes before it stays at
	/// <see cref="StreamMaxRows"/>.
	/// </summary>
	public int LastStreamDoubling { get; }

	/// <summary>Gets the size of the chain chunk after one of <paramref name="size"/>.</summary>
	/// <param name="size">The size of the chunk before.</param>
	/// <returns>
	/// <paramref name="size"/> shifted by <see cref="CollectShift"/>, but not
	/// beyond <see cref="CollectMaxRows"/> (or <paramref name="size"/> itself,
	/// if that is larger).
	/// </returns>
	public int NextCollectRows(int size)
	{
		long next = (long)size << CollectShift;
		return (int)Math.Min(next, Math.Max(CollectMaxRows, size));
	}
}

/// <summary>Arithmetic on chunk sizes.</summary>
internal static class ChunkSizing
{
	/// <summary>Determines whether a number is a power of two.</summary>
	/// <param name="value">The number to test.</param>
	/// <returns>
	/// <see langword="true"/> if <paramref name="value"/> is a power of two;
	/// otherwise, <see langword="false"/>.
	/// </returns>
	public static bool IsPowerOfTwo(int value)
		=> value > 0 && (value & (value - 1)) == 0;

	/// <summary>Gets the base 2 logarithm of a number, rounded down.</summary>
	/// <param name="value">A positive number.</param>
	/// <returns>The position of the highest set bit of <paramref name="value"/>.</returns>
	public static int Log2(int value)
	{
#if NET
		return BitOperations.Log2((uint)value);
#else
		int log = 0;
		while ((value >>= 1) != 0) ++log;
		return log;
#endif
	}
}