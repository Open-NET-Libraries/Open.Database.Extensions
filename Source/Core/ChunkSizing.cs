#if NET
using System.Numerics;
#endif

namespace Open.Database.Extensions.Core;

// The sizes of the chunks the collectors and the buffer terminals rent: a
// configuration object (the shipped values are ChunkSizes.Default), taken
// by the chain, the buffer and the stream, so a test or a benchmark can
// give a call other sizes without changing anything global.
//
// What the arithmetic allows. Every buffer chunk is twice the one before
// it: the first has FirstRows rows, the next 2 * FirstRows, and so on.
// That is what keeps the PooledArray indexer O(1): the chunk of row i is
// floor(log2(i / FirstRows + 1)), found with a shift and a log, and the
// chunk starts at FirstRows * (2^k - 1). So FirstRows must be a power of
// two. The stream stops doubling at StreamMaxRows (also a power of two,
// not below FirstRows); a buffer never stops doubling, because its
// indexer arithmetic is the doubling. A value that does not fit is
// ignored and the default is used, so a wrong value cannot corrupt a
// read.
//
// The chain that ToArrayAsync and ToListAsync collect rows in (see
// ChunkChain) has no consumer working beside the read, so its schedule is
// its own: the first chunk, the shift (1 doubles, 2 quadruples) and the
// largest chunk.
internal sealed class ChunkSizes
{
	const int DefaultFirstRows = 16;
	const int DefaultStreamMaxRows = 4096;
	const int DefaultCollectFirstRows = 16;
	const int DefaultCollectShift = 1;
	const int DefaultCollectMaxRows = 8192;

	/// <summary>The shipped sizes.</summary>
	public static readonly ChunkSizes Default = new();

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

	/// <summary>Rows in the first chunk of a buffer or a stream.</summary>
	public int FirstRows { get; }

	/// <summary>Rows at which a stream stops doubling.</summary>
	public int StreamMaxRows { get; }

	/// <summary>Rows in the first chunk of the collecting chain.</summary>
	public int CollectFirstRows { get; }

	/// <summary>How many doublings per chunk of the chain.</summary>
	public int CollectShift { get; }

	/// <summary>Rows in the largest chunk of the chain.</summary>
	public int CollectMaxRows { get; }

	/// <summary>
	/// How many doublings a stream makes before it stays at
	/// <see cref="StreamMaxRows"/>.
	/// </summary>
	public int LastStreamDoubling { get; }

	/// <summary>The size of the chain chunk after one of <paramref name="size"/>.</summary>
	public int NextCollectRows(int size)
	{
		long next = (long)size << CollectShift;
		return (int)Math.Min(next, Math.Max(CollectMaxRows, size));
	}
}

internal static class ChunkSizing
{
	public static bool IsPowerOfTwo(int value)
		=> value > 0 && (value & (value - 1)) == 0;

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