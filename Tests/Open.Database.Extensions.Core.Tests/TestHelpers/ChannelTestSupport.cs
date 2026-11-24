using System.Data.Common;
using System.Threading.Tasks.Dataflow;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>The row the channel tests map: the <c>Id</c> column.</summary>
[ExcludeFromCodeCoverage]
internal sealed record ChannelRow(int Id);

/// <summary>
/// A reader of <c>Id</c> 1..N that counts how it is read
/// (<c>Read</c> against <c>ReadAsync</c>, <c>NextResult</c>, disposal),
/// can fail at a given row or a given <c>NextResult</c>,
/// and can hold a given read until the test lets it go.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class ChannelTestReader(
	int rowCount,
	int laterSets = 0,
	int? failAtNextResult = null,
	int? failAtRow = null)
	: TypedReader(Columns, Rows(rowCount))
{
	readonly ManualResetEventSlim _gate = new(true);
	readonly ManualResetEventSlim _reached = new(false);
	int _calls;
	int _sets;
	bool _inAsync;

	int _gateAtRow = -1;

	public int SyncReads { get; private set; }
	public int AsyncReads { get; private set; }
	public int NextResultCalls { get; private set; }
	public int AsyncNextResults { get; private set; }
	public int Disposes { get; private set; }

	/// <summary>
	/// Blocks the read of the given row (1-based)
	/// until <see cref="Release"/>.
	/// </summary>
	public void Hold(int row)
	{
		_gateAtRow = row;
		_gate.Reset();
	}

	public void Release() => _gate.Set();

	public bool WaitReached(TimeSpan timeout) => _reached.Wait(timeout);

	public override bool Read()
	{
		int call = ++_calls;
		if (!_inAsync) SyncReads++;
		if (call == _gateAtRow)
		{
			_reached.Set();
			if (!_gate.Wait(TimeSpan.FromSeconds(30)))
				throw new TimeoutException("the held read was never released");
		}

		if (call == failAtRow)
			throw new InvalidOperationException("a row failed");

		return base.Read();
	}

	public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		AsyncReads++;
		_inAsync = true;
		try { return await base.ReadAsync(cancellationToken); }
		finally { _inAsync = false; }
	}

	public override bool NextResult() => Advance();

	public override Task<bool> NextResultAsync(
		CancellationToken cancellationToken)
	{
		AsyncNextResults++;
		return Task.FromResult(Advance());
	}

	bool Advance()
	{
		NextResultCalls++;
		if (NextResultCalls == failAtNextResult)
			throw new LaterSetFailure("a later statement failed");

		return _sets++ < laterSets;
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing) Disposes++;
		base.Dispose(disposing);
	}
}

[ExcludeFromCodeCoverage]
internal static class ChannelWait
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

	/// <summary>Fails with the reason instead of waiting forever.</summary>
	public static async Task Until(
		Func<bool> condition,
		string because = "the condition")
	{
		DateTime end = DateTime.UtcNow + Limit;
		while (!condition())
		{
			if (DateTime.UtcNow > end)
				throw new TimeoutException("never became true: " + because);

			await Task.Delay(10, Ct);
		}
	}

	public static Task Within(this Task task)
		=> task.WaitAsync(Limit, Ct);

	public static Task<T> Within<T>(this Task<T> task)
		=> task.WaitAsync(Limit, Ct);

	/// <summary>
	/// Reads one row, bounded by the limit and watching the pass that feeds the channel: a pass that stopped (a stall, a failure) leaves the channel open
	/// by default, so an unbounded read would wait forever and hang the run; here the test fails with the pass's own exception (or a timeout) instead.
	/// </summary>
	public static async Task<ChannelRow> ReadRow(ChannelReader<ChannelRow> reader, Task pass)
	{
		Task<ChannelRow> read = reader.ReadAsync(Ct).AsTask().Within();
		Task first = await Task.WhenAny(read, pass).ConfigureAwait(false);
		if (first == pass && !read.IsCompleted) await pass.ConfigureAwait(false); // a failed pass throws here
		return await read.ConfigureAwait(false);
	}


	/// <summary>
	/// <see cref="ReadRow"/> for a block: receives one row, bounded by the limit and watching the pass, so a pass that stopped fails the test with its own exception.
	/// </summary>
	public static async Task<ChannelRow> ReceiveRow(ISourceBlock<ChannelRow> block, Task pass)
	{
		Task<ChannelRow> receive = block.ReceiveAsync(Ct).Within();
		Task first = await Task.WhenAny(receive, pass).ConfigureAwait(false);
		if (first == pass && !receive.IsCompleted) await pass.ConfigureAwait(false); // a failed pass throws here
		return await receive.ConfigureAwait(false);
	}
	/// <summary>
	/// Reads to the end of the channel: the ids of the rows,
	/// and the exception the channel ended with, if it did.
	/// A read that waits longer than the limit ends with a timeout.
	/// </summary>
	public static async Task<(List<int> Rows, Exception? Error)> Collect(
		ChannelReader<ChannelRow> reader)
	{
		var rows = new List<int>();
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
		cts.CancelAfter(Limit);
		try
		{
			await foreach (ChannelRow row in reader.ReadAllAsync(cts.Token))
				rows.Add(row.Id);

			return (rows, null);
		}
		catch (OperationCanceledException ex)
			when (ex.CancellationToken == cts.Token && !Ct.IsCancellationRequested)
		{
			// The wait here ran out: that is a failure of the test,
			// not a cancel the feed reported.
			throw new TimeoutException("the channel never ended", ex);
		}
		catch (Exception ex)
		{
			return (rows, ex);
		}
	}

	public static List<int> Ids(int count)
		=> [.. Enumerable.Range(1, count)];
}
