using System.Data.Common;
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

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		AsyncReads++;
		_inAsync = true;
		try { return base.ReadAsync(cancellationToken); }
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

internal delegate Channel<ChannelRow> CountedChannel(
	out Task<long> readCount,
	ChannelOptions? options,
	bool startImmediately,
	CancellationToken cancellationToken);

/// <summary>
/// One of the four views with the three channel forms bound to it, over
/// readers the test supplies, counting how often the query executed.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class ChannelRig
{
	public static readonly string[] Views =
		["Query", "Skip", "Take", "Command"];
	public static readonly string[] Forms = ["Reader", "Channel", "Counted"];

	public ChannelTestReader? Reader { get; private set; }

	/// <summary>
	/// How many readers were created: how often the command ran.
	/// Counted atomically: feeds that start at once run on
	/// different threads.
	/// </summary>
	public int Executes => Volatile.Read(ref _executes);

	int _executes;

	/// <summary>Only the Command view has a connection.</summary>
	public FakeDbConnection? Connection { get; private set; }

	public Func<
		ChannelOptions?,
		bool,
		CancellationToken,
		ChannelReader<ChannelRow>> ToReader
	{ get; private set; } = null!;

	public Func<
		ChannelOptions?,
		bool,
		CancellationToken,
		Channel<ChannelRow>> ToChannel
	{ get; private set; } = null!;

	public CountedChannel ToCounted { get; private set; } = null!;

	public static TheoryData<string> ViewData
	{
		get
		{
			var data = new TheoryData<string>();
			foreach (string view in Views) data.Add(view);
			return data;
		}
	}

	public static TheoryData<string, string> ViewAndFormData
	{
		get
		{
			var data = new TheoryData<string, string>();
			foreach (string view in Views)
			{
				foreach (string form in Forms)
				{
					data.Add(view, form);
				}
			}

			return data;
		}
	}

	/// <summary>The reader of the form: the one a test reads from.</summary>
	public ChannelReader<ChannelRow> Open(
		string form,
		ChannelOptions? options = null,
		bool startImmediately = false,
		CancellationToken cancellationToken = default)
		=> form switch
		{
			"Reader" => ToReader(options, startImmediately, cancellationToken),
			"Channel"
				=> ToChannel(options, startImmediately, cancellationToken)
					.Reader,
			_
				=> ToCounted(
					out _, options, startImmediately, cancellationToken)
					.Reader,
		};

	public static ChannelRig Create(
		string view,
		Func<ChannelTestReader> make,
		bool asyncReads = false,
		ConnectionState state = ConnectionState.Closed)
	{
		var rig = new ChannelRig();
		DbDataReader Make()
		{
			Interlocked.Increment(ref rig._executes);
			return rig.Reader = make();
		}

		switch (view)
		{
			case "Query":
			{
				var q = new FakeExecuteReader(Make).Map<ChannelRow>();
				if (asyncReads) q = q.AsyncReads();
				rig.ToReader = (o, s, t) => q.ToChannelReader(o, s, t);
				rig.ToChannel = (o, s, t) => q.ToChannel(o, s, t);
				rig.ToCounted = (out n, o, s, t)
					=> q.ToChannel(out n, o, s, t);
				break;
			}

			case "Skip":
			{
				var q = new FakeExecuteReader(Make).Map<ChannelRow>();
				if (asyncReads) q = q.AsyncReads();
				var v = q.Skip(0);
				rig.ToReader = (o, s, t) => v.ToChannelReader(o, s, t);
				rig.ToChannel = (o, s, t) => v.ToChannel(o, s, t);
				rig.ToCounted = (out n, o, s, t)
					=> v.ToChannel(out n, o, s, t);
				break;
			}

			case "Take":
			{
				var q = new FakeExecuteReader(Make).Map<ChannelRow>();
				if (asyncReads) q = q.AsyncReads();
				var v = q.Take(1000);
				rig.ToReader = (o, s, t) => v.ToChannelReader(o, s, t);
				rig.ToChannel = (o, s, t) => v.ToChannel(o, s, t);
				rig.ToCounted = (out n, o, s, t)
					=> v.ToChannel(out n, o, s, t);
				break;
			}

			case "Command":
			{
				var connection = new FakeDbConnection();
				connection.SetState(state);
				rig.Connection = connection;
				var command = new FakeDbCommand(connection, Make);
				var v = command.Map<ChannelRow>();
				if (asyncReads) v = v.AsyncReads();
				rig.ToReader = (o, s, t) => v.ToChannelReader(o, s, t);
				rig.ToChannel = (o, s, t) => v.ToChannel(o, s, t);
				rig.ToCounted = (out n, o, s, t)
					=> v.ToChannel(out n, o, s, t);
				break;
			}

			default:
				throw new ArgumentOutOfRangeException(nameof(view));
		}

		return rig;
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
