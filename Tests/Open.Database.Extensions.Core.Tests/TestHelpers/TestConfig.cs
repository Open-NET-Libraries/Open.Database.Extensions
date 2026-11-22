using System.Buffers;
using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>
/// Builds a <see cref="ChunkConfig{T}"/> for a test, with every part given:
/// the library never fills one in, so a test says what it uses (and may leave
/// the sizes, the bin and the threshold to the shipped ones).
/// </summary>
[ExcludeFromCodeCoverage]
internal static class TestConfig
{
	public static ChunkConfig<T> Of<T>(
		ArrayPool<T> pool,
		bool? clearOnReturn,
		RecycleBin? bin = null,
		int deferFromBytes = ChunkConfig<T>.DefaultDeferFromBytes,
		ChunkSizes? sizes = null)
		=> new(
			pool,
			clearOnReturn,
			sizes ?? ChunkSizes.Default,
			bin ?? RecycleBin.Default,
			deferFromBytes);
}
