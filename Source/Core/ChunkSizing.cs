#if NET
using System.Numerics;
#endif

namespace Open.Database.Extensions.Core;

// The sizes of the chunks the buffer terminals rent: a TEST SEAM, not an
// option. The fields hold the shipped values. There is no
// InternalsVisibleTo, so a test or a benchmark project that wants other
// sizes (to sweep them, say) sets the fields by reflection, and puts
// them back.
//
// What the arithmetic allows. Every chunk is twice the one before it: the
// first has FirstRows rows, the next 2 * FirstRows, and so on. That is
// what keeps the PooledArray indexer O(1): the chunk of row i is
// floor(log2(i / FirstRows + 1)), found with a shift and a log, and the
// chunk starts at FirstRows * (2^k - 1). So FirstRows must be a power of
// two. The stream stops doubling at StreamMaxRows (also a power of two,
// not below FirstRows); a buffer never stops doubling, because its
// indexer arithmetic is the doubling. A value that does not fit is
// ignored and the default is used, so a wrong knob cannot corrupt a
// read. The values are read once, when a buffer or a stream is made.
internal static class ChunkSizing
{
	const int DefaultFirstRows = 16;
	const int DefaultStreamMaxRows = 4096;

	// Rows in the first chunk of a buffer or a stream (set by reflection
	// as "_firstRows").
	internal static int _firstRows = DefaultFirstRows;

	// Rows at which a stream stops doubling (set by reflection as
	// "_streamMaxRows").
	internal static int _streamMaxRows = DefaultStreamMaxRows;

	internal static int ResolveFirstRows()
	{
		int first = Volatile.Read(ref _firstRows);
		return IsPowerOfTwo(first) && first <= 1 << 20
			? first
			: DefaultFirstRows;
	}

	internal static int ResolveStreamMaxRows(int first)
	{
		int max = Volatile.Read(ref _streamMaxRows);
		return IsPowerOfTwo(max) && max >= first && max <= 1 << 26
			? max
			: Math.Max(DefaultStreamMaxRows, first);
	}

	// The chain that ToArrayAsync and ToListAsync collect rows in (see
	// ChunkChain): no consumer works beside the read, so the schedule
	// is its own. The first chunk, the shift (1 doubles, 2 quadruples)
	// and the largest chunk are set by reflection as "_collectFirstRows",
	// "_collectShift" and "_collectMaxRows".
	const int DefaultCollectFirstRows = 16;
	const int DefaultCollectShift = 1;
	const int DefaultCollectMaxRows = 8192;

	internal static int _collectFirstRows = DefaultCollectFirstRows;
	internal static int _collectShift = DefaultCollectShift;
	internal static int _collectMaxRows = DefaultCollectMaxRows;

	internal static int ResolveCollectFirstRows()
	{
		int first = Volatile.Read(ref _collectFirstRows);
		return IsPowerOfTwo(first) && first <= 1 << 20
			? first
			: DefaultCollectFirstRows;
	}

	// The size of the chunk after one of this size.
	internal static int NextCollectRows(int size)
	{
		int max = Volatile.Read(ref _collectMaxRows);
		if (!IsPowerOfTwo(max) || max > 1 << 26) max = DefaultCollectMaxRows;

		int shift = Volatile.Read(ref _collectShift);
		if ((uint)(shift - 1) > 3u) shift = DefaultCollectShift;

		long next = (long)size << shift;
		return (int)Math.Min(next, Math.Max(max, size));
	}

	static bool IsPowerOfTwo(int value)
		=> value > 0 && (value & (value - 1)) == 0;

	// floor(log2(value)) for a positive int.
	internal static int Log2(int value)
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
