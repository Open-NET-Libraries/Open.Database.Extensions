using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A foreign IMappedQuery<T> (one that is not ODX's own) is read through its
// ReadAsync handler. Every terminal then enumerates the rows itself, and the
// library always uses ConfigureAwait(false): the terminals must never resume
// on the caller's SynchronizationContext, and must give what the in-box
// System.Linq.AsyncEnumerable terminal gives over the same rows.
[ExcludeFromCodeCoverage]
public static class ForeignQueryContextTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	sealed class CountingContext : SynchronizationContext
	{
		readonly ConcurrentQueue<string> _posts = new();

		public int Posts => _posts.Count;

		// What was posted: the awaiter's continuation names its state machine.
		public string What => string.Join(" ; ", _posts);

		public override void Post(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			ThreadPool.QueueUserWorkItem(_ => d(state));
		}

		public override void Send(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			d(state);
		}

		static string Describe(object? state)
			=> state switch
			{
				null => "null",
				Delegate del => $"{del.Target?.GetType()} {del.Method.Name}",
				ITuple tuple => string.Join(
					" | ",
					Enumerable.Range(0, tuple.Length).Select(i => Describe(tuple[i]))),
				_ => state.GetType().ToString(),
			};
	}

	// A source of rows whose one awaited step (the first MoveNextAsync, or the
	// disposal) completes only when the gate opens. With "honor" it throws when
	// the token it was given is cancelled; otherwise it ignores the token.
	sealed class Rows(
		int[] values,
		TaskCompletionSource gate,
		string gated,
		bool honor)
		: IAsyncEnumerable<int>
	{
		readonly string _gated = gated;
		readonly bool _honor = honor;
		// [0] enumerators created, [1] enumerators disposed.
		readonly int[] _counts = new int[2];

		public int Disposed => Volatile.Read(ref _counts[1]);
		public int Enumerators => Volatile.Read(ref _counts[0]);
		public int[] Values { get; } = values;
		public TaskCompletionSource Gate { get; } = gate;

		public IAsyncEnumerator<int> GetAsyncEnumerator(
			CancellationToken cancellationToken = default)
		{
			Interlocked.Increment(ref _counts[0]);
			return new Enumerator(this, cancellationToken);
		}

		sealed class Enumerator(Rows rows, CancellationToken token)
			: IAsyncEnumerator<int>
		{
			int _index = -1;

			public int Current => rows.Values[_index];

			public ValueTask<bool> MoveNextAsync()
			{
				if (rows._honor) token.ThrowIfCancellationRequested();

				_index++;
				bool has = _index < rows.Values.Length;
				return _index == 0 && rows._gated == "move"
					? new(Opened(rows.Gate.Task, has))
					: new(has);
			}

			static async Task<bool> Opened(Task gate, bool has)
			{
				await gate.ConfigureAwait(false);
				return has;
			}

			public ValueTask DisposeAsync()
			{
				Interlocked.Increment(ref rows._counts[1]);
				return rows._gated == "dispose" ? new(rows.Gate.Task) : default;
			}
		}
	}

	sealed class ForeignQuery(Rows rows) : IMappedQuery<int>
	{
		public void Read(Action<IEnumerable<int>> handler)
			=> handler(rows.Values);

		public TResult Read<TResult>(Func<IEnumerable<int>, TResult> handler)
			=> handler(rows.Values);

		public ValueTask ReadAsync(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> handler(rows, cancellationToken);

		public ValueTask<TResult> ReadAsync<TResult>(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> handler(rows, cancellationToken);
	}

	static Rows NewRows(int[] values, string gated = "none", bool honor = true)
		=> new(
			values,
			new TaskCompletionSource(
				TaskCreationOptions.RunContinuationsAsynchronously),
			gated,
			honor);

	static async Task<object?> Box<T>(ValueTask<T> task)
		=> await task.ConfigureAwait(false);

	// Each terminal as called on a foreign query, and the in-box LINQ terminal
	// it stands for, over the same rows.
	sealed record Terminal(
		Func<IMappedQuery<int>, CancellationToken, Task<object?>> Foreign,
		Func<IAsyncEnumerable<int>, CancellationToken, Task<object?>> InBox);

	static readonly Dictionary<string, Terminal> Terminals = new()
	{
		["ToListAsync"] = new(
			(q, ct) => Box(q.ToListAsync(ct)),
			(e, ct) => Box(e.ToListAsync(ct))),
		["ToArrayAsync"] = new(
			(q, ct) => Box(q.ToArrayAsync(ct)),
			(e, ct) => Box(e.ToArrayAsync(ct))),
		["ToImmutableArrayAsync"] = new(
			(q, ct) => Box(q.ToImmutableArrayAsync(ct)),
			async (e, ct) => (await e.ToListAsync(ct).ConfigureAwait(false)).ToImmutableArray()),
		["ToDictionaryAsync(key)"] = new(
			(q, ct) => Box(q.ToDictionaryAsync(i => i % 2, comparer: null, cancellationToken: ct)),
			(e, ct) => Box(e.ToDictionaryAsync(i => i % 2, comparer: null, cancellationToken: ct))),
		["ToDictionaryAsync(key,value)"] = new(
			(q, ct) => Box(q.ToDictionaryAsync(i => i % 2, i => i * 10, comparer: null, cancellationToken: ct)),
			(e, ct) => Box(e.ToDictionaryAsync(i => i % 2, i => i * 10, comparer: null, cancellationToken: ct))),
		["ToHashSetAsync"] = new(
			(q, ct) => Box(q.ToHashSetAsync(comparer: null, cancellationToken: ct)),
			(e, ct) => Box(e.ToHashSetAsync(comparer: null, cancellationToken: ct))),
		["FirstOrDefaultAsync"] = new(
			(q, ct) => Box(q.FirstOrDefaultAsync(ct)),
			(e, ct) => Box(e.FirstOrDefaultAsync(ct))),
		["SingleOrDefaultAsync"] = new(
			(q, ct) => Box(q.SingleOrDefaultAsync(ct)),
			(e, ct) => Box(e.SingleOrDefaultAsync(ct))),
		["FirstAsync"] = new(
			(q, ct) => Box(q.FirstAsync(ct)),
			(e, ct) => Box(e.FirstAsync(ct))),
		["SingleAsync"] = new(
			(q, ct) => Box(q.SingleAsync(ct)),
			(e, ct) => Box(e.SingleAsync(ct))),
	};

	static readonly Dictionary<string, int[]> RowSets = new()
	{
		["empty"] = [],
		["one"] = [5],
		["many"] = [1, 2, 3],
		["duplicates"] = [3, 3, 1],
	};

	static string Describe(object? value)
		=> value switch
		{
			null => "null",
			IDictionary<int, int> d => "dictionary{"
				+ string.Join(",", d.OrderBy(p => p.Key).Select(p => $"{p.Key}:{p.Value}"))
				+ "}",
			HashSet<int> set => "set{" + string.Join(",", set.Order()) + "}",
			IEnumerable<int> items => value.GetType().Name
				+ "[" + string.Join(",", items) + "]",
			_ => value.ToString() ?? "",
		};

	static async Task<string> Outcome(Func<Task<object?>> call)
	{
		try
		{
			return Describe(await call());
		}
		catch (Exception ex)
		{
			return $"throws {ex.GetType().Name}: {ex.Message}";
		}
	}

	static TheoryData<string, string> TerminalsByRowSets()
	{
		TheoryData<string, string> data = [];
		foreach (string terminal in Terminals.Keys)
		{
			foreach (string rows in RowSets.Keys)
			{
				data.Add(terminal, rows);
			}
		}

		return data;
	}

	public static TheoryData<string, string> Results => TerminalsByRowSets();

	// Every terminal gives, over the same rows, what the in-box LINQ terminal
	// gives: the value or the exception (type and message), and the enumerator
	// is enumerated once and disposed once.
	[Theory]
	[MemberData(nameof(Results))]
	public static async Task TheFallback_GivesWhatTheInBoxTerminalGives(
		string terminal,
		string rowSet)
	{
		Terminal t = Terminals[terminal];
		int[] values = RowSets[rowSet];

		Rows foreignRows = NewRows(values);
		Rows referenceRows = NewRows(values);

		string actual = await Outcome(
			() => t.Foreign(new ForeignQuery(foreignRows), Ct));
		string expected = await Outcome(
			() => t.InBox(referenceRows, Ct));

		Assert.Equal(expected, actual);
		Assert.Equal(1, foreignRows.Enumerators);
		Assert.Equal(1, foreignRows.Disposed);
		Assert.Equal(referenceRows.Disposed, foreignRows.Disposed);
	}

	// A token that is already cancelled: a source that honors it throws, one
	// that ignores it is read to the end; either way it is what the in-box
	// terminal does, and the enumerator is still disposed.
	[Theory]
	[MemberData(nameof(Results))]
	public static async Task TheFallback_HonorsACancelledTokenAsTheInBoxTerminalDoes(
		string terminal,
		string rowSet)
	{
		Terminal t = Terminals[terminal];
		int[] values = RowSets[rowSet];
		using CancellationTokenSource cts = new();
		cts.Cancel();

		foreach (bool honor in new[] { true, false })
		{
			Rows foreignRows = NewRows(values, honor: honor);
			Rows referenceRows = NewRows(values, honor: honor);

			string actual = await Outcome(
				() => t.Foreign(new ForeignQuery(foreignRows), cts.Token));
			string expected = await Outcome(
				() => t.InBox(referenceRows, cts.Token));

			Assert.Equal(expected, actual);
			Assert.Equal(referenceRows.Disposed, foreignRows.Disposed);
			if (honor)
				Assert.StartsWith("throws OperationCanceledException", actual);
		}
	}

	record Posted(CountingContext Context, string Result);

	// Runs the call under a context that counts posts; the gate is opened
	// after the call has returned an incomplete task.
	static async Task<Posted> PostsDuring(Rows rows, Terminal terminal)
	{
		CountingContext context = new();
		SynchronizationContext? prior = SynchronizationContext.Current;
		ForeignQuery query = new(rows);

		Task<object?> task;
		SynchronizationContext.SetSynchronizationContext(context);
		try
		{
			task = terminal.Foreign(query, Ct);
		}
		finally
		{
			SynchronizationContext.SetSynchronizationContext(prior);
		}

		Assert.False(
			task.IsCompleted,
			"the gated step must be genuinely asynchronous");
		rows.Gate.TrySetResult();
		return new(context, await Outcome(() => task));
	}

	public static TheoryData<string, string, string> Gated()
	{
		TheoryData<string, string, string> data = [];
		foreach (string terminal in Terminals.Keys)
		{
			foreach (string gated in new[] { "move", "dispose" })
			{
				foreach (string rows in new[] { "empty", "one", "many" })
				{
					data.Add(terminal, gated, rows);
				}
			}
		}

		return data;
	}

	// The step the caller awaits (the first row, or the disposal) completes
	// from another thread, and the terminal must continue there: a post to the
	// caller's context means the terminal awaited without ConfigureAwait(false).
	[Theory]
	[MemberData(nameof(Gated))]
	public static async Task TheFallback_NeverResumesOnTheCallersContext(
		string terminal,
		string gated,
		string rowSet)
	{
		Rows rows = NewRows(RowSets[rowSet], gated);

		Posted posted = await PostsDuring(rows, Terminals[terminal]);

		Assert.True(
			posted.Context.Posts == 0,
			$"{terminal}/{gated}/{rowSet} posted {posted.Context.Posts} time(s) to the caller's context: {posted.Context.What}");
	}
}
