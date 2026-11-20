using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>
/// A recycle bin a test owns: it counts every post and can answer "Full" or
/// throw, instead of the real thing. A test hands it to a
/// <see cref="ChunkConfig{T}"/>, so nothing is global.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class TestBin : RecycleBin
{
	int _posts;

	/// <summary>"Normal", "Full" or "Throw".</summary>
	public string Mode { get; init; } = "Normal";

	/// <summary>How many posts were attempted.</summary>
	public int Posts => Volatile.Read(ref _posts);

	protected override PostResult TryPost(Work work)
	{
		Interlocked.Increment(ref _posts);
		return Mode switch
		{
			"Full" => PostResult.Full,
			"Throw" => throw new InvalidOperationException("the bin broke"),
			_ => base.TryPost(work),
		};
	}

	/// <summary>
	/// A work item that runs <paramref name="runner"/> and nothing else:
	/// for a test of the bin itself.
	/// </summary>
	public static Work Item(IRunner runner)
		=> new(runner, Array.Empty<object>(), 0, new object());

	/// <summary>A runner that blocks the bin's one loop until the gate opens.</summary>
	public sealed class GateRunner(
		ManualResetEventSlim started,
		ManualResetEventSlim gate)
		: IRunner
	{
		public void Run(Array array, int used, object config)
		{
			started.Set();
			gate.Wait(TimeSpan.FromSeconds(30));
		}
	}

	/// <summary>A runner that runs an action.</summary>
	public sealed class ActionRunner(Action action) : IRunner
	{
		public void Run(Array array, int used, object config)
			=> action();
	}
}
