using System.Buffers;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ArrayPool<T>.Shared cannot be inspected, but it can be observed: on one thread the array that was returned last is the one the next
// Rent of the same size gets back (the thread's own slot is checked first). So a test that returns a probe array, runs a call that rents
// and, whichever way it ends, returns its buffer, finds the same probe array again; a call that kept its buffer leaves the slot empty.
// Each test has a model type of its own, so no other test or call uses the same pool, and the fake database completes every task
// synchronously, so the whole call runs on the test's thread.
// An array that is not returned is not a memory leak (the garbage collector reclaims it); it costs the pool one array.
[ExcludeFromCodeCoverage]
public static class LeakPoolTests
{
	sealed class SuccessRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class SyncFailRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class AsyncSuccessRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class AsyncFailRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class AsyncMappingFailRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class AsyncCancelRow { public int Id { get; set; } public string? Name { get; set; } }
	sealed class AsyncImmutableFailRow { public int Id { get; set; } public string? Name { get; set; } }

	static readonly object?[][] ThreeRows = [[1, "a"], [2, "b"], [3, "c"]];

	// Runs the call, then says whether the buffer it rented came back to the pool.
	static async Task<bool> ReturnsItsBuffer<TRow>(DbScript script, Func<ExpressiveDbCommand, Task> call)
		where TRow : class
	{
		TRow[] probe = ArrayPool<TRow>.Shared.Rent(16);
		ArrayPool<TRow>.Shared.Return(probe);

		var tally = new DbTally(script);
		try
		{
			await call(tally.NewOpenConnection().Command("select 1"));
		}
		catch (Exception ex) when (ex is InvalidOperationException or InvalidCastException or ValueMappingException or OperationCanceledException)
		{
			// The scenarios below make the call fail on purpose.
		}

		return ReferenceEquals(probe, ArrayPool<TRow>.Shared.Rent(16));
	}

	[Fact]
	public static async Task ToArray_Success_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<SuccessRow>(new() { Rows = ThreeRows },
			c => { c.Map<SuccessRow>().ToArray(); return Task.CompletedTask; }));

	[Fact]
	public static async Task ToArray_ReadFails_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<SyncFailRow>(new() { Rows = ThreeRows, FailOnRead = 2 },
			c => { c.Map<SyncFailRow>().ToArray(); return Task.CompletedTask; }));

	[Fact]
	public static async Task ToArrayAsync_Success_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<AsyncSuccessRow>(new() { Rows = ThreeRows },
			async c => await c.Map<AsyncSuccessRow>().ToArrayAsync(TestContext.Current.CancellationToken)));

	[Fact]
	public static async Task ToArrayAsync_ReadFails_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<AsyncFailRow>(new() { Rows = ThreeRows, FailOnRead = 2 },
			async c => await c.Map<AsyncFailRow>().ToArrayAsync(TestContext.Current.CancellationToken)));

	[Fact]
	public static async Task ToArrayAsync_MappingFails_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<AsyncMappingFailRow>(new() { Rows = [[1, "a"], ["not a number", "b"]] },
			async c => await c.Map<AsyncMappingFailRow>().ToArrayAsync(TestContext.Current.CancellationToken)));

	[Fact]
	public static async Task ToArrayAsync_CancelledMidRead_ReturnsTheBuffer()
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
		Assert.True(await ReturnsItsBuffer<AsyncCancelRow>(new() { Rows = ThreeRows, OnRead = n => { if (n == 2) cts.Cancel(); } },
			async c => await c.Map<AsyncCancelRow>().ToArrayAsync(cts.Token)));
	}

	[Fact]
	public static async Task ToImmutableArrayAsync_ReadFails_ReturnsTheBuffer()
		=> Assert.True(await ReturnsItsBuffer<AsyncImmutableFailRow>(new() { Rows = ThreeRows, FailOnRead = 2 },
			async c => await c.Map<AsyncImmutableFailRow>().ToImmutableArrayAsync(TestContext.Current.CancellationToken)));
}
