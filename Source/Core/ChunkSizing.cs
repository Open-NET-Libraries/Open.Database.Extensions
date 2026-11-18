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
	internal const int DefaultFirstRows = 16;
	internal const int DefaultStreamMaxRows = 4096;

	// Rows in the first chunk of a buffer or a stream.
	internal static int FirstRows = DefaultFirstRows;

	// Rows at which a stream stops doubling.
	internal static int StreamMaxRows = DefaultStreamMaxRows;

	internal static int ResolveFirstRows()
	{
		int first = Volatile.Read(ref FirstRows);
		return IsPowerOfTwo(first) && first <= 1 << 20
			? first
			: DefaultFirstRows;
	}

	internal static int ResolveStreamMaxRows(int first)
	{
		int max = Volatile.Read(ref StreamMaxRows);
		return IsPowerOfTwo(max) && max >= first && max <= 1 << 26
			? max
			: Math.Max(DefaultStreamMaxRows, first);
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
