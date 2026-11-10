using System.Data.Common;
using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The leak matrix. Every terminal of the command API runs against a tracking fake database (see TestHelpers/LeakTracking.cs) for every kind of
// connection source and every outcome that applies to it, and afterwards the fake says what was left behind:
//   - every command and reader the library created was disposed, exactly once;
//   - a connection from a factory was disposed exactly once and is closed;
//   - a connection the caller handed in closed was opened and closed the same number of times (never disposed);
//   - a connection the caller handed in open was not touched at all, and neither was a transaction the caller began;
//   - a pool was given back every connection it handed out, and the library never disposed it.
// An outcome is one of: success, no rows, the reader throwing on the first or a later row, the mapping throwing, the execute throwing,
// a token cancelled before the call or in the middle of the reads, and the caller walking away from a stream after its first element.
[ExcludeFromCodeCoverage]
public static class LeakMatrixTests
{
	sealed class Row
	{
		public int Id { get; set; }
		public string? Name { get; set; }
	}

	[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Test Only")]
	sealed class ExecuteFailed : Exception;

	public enum Outcome
	{
		Success,
		NoRows,
		ReadThrowsFirst,
		ReadThrowsLater,
		MappingThrows,
		ExecuteThrows,
		CancelledBefore,
		CancelledDuringRead,
		Abandoned,
	}

	[Flags]
	enum Traits
	{
		None = 0,
		Reader = 1,
		Mapped = 2,
		Streams = 4,
		Async = 8,
	}

	sealed record Terminal<TCommand>(string Name, Traits Traits, Func<TCommand, CancellationToken, Task> Run);

	static Terminal<TCommand> Sync<TCommand, TResult>(string name, Traits traits, Func<TCommand, TResult> run)
		=> new(name, traits, (c, _) =>
		{
			run(c);
			return Task.CompletedTask;
		});

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static Terminal<TCommand> Async<TCommand, TResult>(string name, Traits traits, Func<TCommand, CancellationToken, ValueTask<TResult>> run)
		=> new(name, traits | Traits.Async, async (c, ct) => await run(c, ct));

	const Traits RM = Traits.Reader | Traits.Mapped;

	static readonly Terminal<ExpressiveDbCommand>[] DbTerminals =
	[
		Sync<ExpressiveDbCommand, List<Row>>("Map.ToList", RM, c => c.Map<Row>().ToList()),
		Sync<ExpressiveDbCommand, Row[]>("Map.ToArray", RM, c => c.Map<Row>().ToArray()),
		Sync<ExpressiveDbCommand, Row?>("Map.FirstOrDefault", RM, c => c.Map<Row>().FirstOrDefault()),
		Sync<ExpressiveDbCommand, Row?>("Map.SingleOrDefault", RM, c => c.Map<Row>().SingleOrDefault()),
		Sync<ExpressiveDbCommand, Dictionary<int, Row>>("Map.ToDictionary", RM, c => c.Map<Row>().ToDictionary(r => r.Id)),
		Sync<ExpressiveDbCommand, HashSet<Row>>("Map.ToHashSet", RM, c => c.Map<Row>().ToHashSet()),
		Sync<ExpressiveDbCommand, ImmutableArray<Row>>("Map.ToImmutableArray", RM, c => c.Map<Row>().ToImmutableArray()),
		Sync<ExpressiveDbCommand, bool>("Map.Read.Abandon", RM | Traits.Streams, c => c.Map<Row>().Read(seq =>
		{
			using IEnumerator<Row> e = seq.GetEnumerator();
			return e.MoveNext();
		})),
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Sync<ExpressiveDbCommand, IEnumerable<Row>>("Results", RM, c => c.Results<Row>()),
		#pragma warning restore CS0618
		Sync<ExpressiveDbCommand, bool>("ExecuteReader.State", Traits.Reader, c => c.ExecuteReader(0, static (r, _) => r.Read())),
		Sync<ExpressiveDbCommand, QueryResultQueue<object[]>>("Retrieve", Traits.Reader, c => c.Retrieve()),
		Sync<ExpressiveDbCommand, object?>("ExecuteScalar", Traits.None, c => c.ExecuteScalar()),
		Sync<ExpressiveDbCommand, int>("ExecuteNonQuery", Traits.None, c => c.ExecuteNonQuery()),
		Sync<ExpressiveDbCommand, object?>("ExecuteReturn", Traits.None, c => c.ExecuteReturn()),

		Async<ExpressiveDbCommand, List<Row>>("Map.ToListAsync", RM, (c, ct) => c.Map<Row>().ToListAsync(ct)),
		Async<ExpressiveDbCommand, Row[]>("Map.ToArrayAsync", RM, (c, ct) => c.Map<Row>().ToArrayAsync(ct)),
		Async<ExpressiveDbCommand, Row?>("Map.FirstOrDefaultAsync", RM, (c, ct) => c.Map<Row>().FirstOrDefaultAsync(ct)),
		Async<ExpressiveDbCommand, Row?>("Map.SingleOrDefaultAsync", RM, (c, ct) => c.Map<Row>().SingleOrDefaultAsync(ct)),
		Async<ExpressiveDbCommand, Dictionary<int, Row>>("Map.ToDictionaryAsync", RM, (c, ct) => c.Map<Row>().ToDictionaryAsync(r => r.Id, cancellationToken: ct)),
		Async<ExpressiveDbCommand, HashSet<Row>>("Map.ToHashSetAsync", RM, (c, ct) => c.Map<Row>().ToHashSetAsync(cancellationToken: ct)),
		Async<ExpressiveDbCommand, ImmutableArray<Row>>("Map.ToImmutableArrayAsync", RM, (c, ct) => c.Map<Row>().ToImmutableArrayAsync(ct)),
		Async<ExpressiveDbCommand, bool>("Map.ReadAsync.Abandon", RM | Traits.Streams, (c, ct) => c.Map<Row>().ReadAsync<Row, bool>(async seq =>
		{
			await using IAsyncEnumerator<Row> e = seq.GetAsyncEnumerator(ct);
			return await e.MoveNextAsync();
		}, ct)),
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Async<ExpressiveDbCommand, IEnumerable<Row>>("ResultsAsync", RM, (c, ct) => c.UseCancellationToken(ct).ResultsAsync<Row>()),
		#pragma warning restore CS0618
		Async<ExpressiveDbCommand, bool>("ExecuteReaderAsync.State", Traits.Reader, (c, ct) => c.UseCancellationToken(ct).ExecuteReaderAsync(0, static (r, _) => new ValueTask<bool>(r.Read()))),
		Async<ExpressiveDbCommand, QueryResultQueue<object[]>>("RetrieveAsync", Traits.Reader, (c, ct) => c.UseCancellationToken(ct).RetrieveAsync()),
		Async<ExpressiveDbCommand, object?>("ExecuteScalarAsync", Traits.None, (c, ct) => c.UseCancellationToken(ct).ExecuteScalarAsync()),
		Async<ExpressiveDbCommand, int>("ExecuteNonQueryAsync", Traits.None, (c, ct) => c.UseCancellationToken(ct).ExecuteNonQueryAsync()),
		Async<ExpressiveDbCommand, object?>("ExecuteReturnAsync", Traits.None, (c, ct) => c.UseCancellationToken(ct).ExecuteReturnAsync()),
	];

	// The same commands made from the interface-typed entry points (IDbConnection, IDbConnectionFactory, IDbConnectionPool): ExpressiveCommand.
	static readonly Terminal<ExpressiveCommand>[] InterfaceTerminals =
	[
		Sync<ExpressiveCommand, List<Row>>("Map.ToList", RM, c => c.Map<Row>().ToList()),
		Sync<ExpressiveCommand, Row?>("Map.FirstOrDefault", RM, c => c.Map<Row>().FirstOrDefault()),
		Sync<ExpressiveCommand, bool>("Map.Read.Abandon", RM | Traits.Streams, c => c.Map<Row>().Read(seq =>
		{
			using IEnumerator<Row> e = seq.GetEnumerator();
			return e.MoveNext();
		})),
		Sync<ExpressiveCommand, bool>("ExecuteReader.State", Traits.Reader, c => c.ExecuteReader(0, static (r, _) => r.Read())),
		Sync<ExpressiveCommand, object?>("ExecuteScalar", Traits.None, c => c.ExecuteScalar()),
		Sync<ExpressiveCommand, int>("ExecuteNonQuery", Traits.None, c => c.ExecuteNonQuery()),
		Async<ExpressiveCommand, List<Row>>("Map.ToListAsync", RM, (c, ct) => c.Map<Row>().ToListAsync(ct)),
		Async<ExpressiveCommand, bool>("Map.ReadAsync.Abandon", RM | Traits.Streams, (c, ct) => c.Map<Row>().ReadAsync<Row, bool>(async seq =>
		{
			await using IAsyncEnumerator<Row> e = seq.GetAsyncEnumerator(ct);
			return await e.MoveNextAsync();
		}, ct)),
	];

	static bool Applies(Traits traits, Outcome outcome) => outcome switch
	{
		Outcome.Success => true,
		Outcome.ExecuteThrows => true,
		Outcome.NoRows or Outcome.ReadThrowsFirst or Outcome.ReadThrowsLater => (traits & Traits.Reader) != 0,
		Outcome.MappingThrows => (traits & Traits.Mapped) != 0,
		Outcome.CancelledBefore => (traits & Traits.Async) != 0,
		Outcome.CancelledDuringRead => (traits & (Traits.Async | Traits.Reader)) == (Traits.Async | Traits.Reader),
		Outcome.Abandoned => (traits & Traits.Streams) != 0,
		_ => false,
	};

	public static TheoryData<string, LeakSource, Outcome> DbCases { get; } = Cases(DbTerminals);

	public static TheoryData<string, LeakSource, Outcome> InterfaceCases { get; } = Cases(InterfaceTerminals);

	static TheoryData<string, LeakSource, Outcome> Cases<TCommand>(Terminal<TCommand>[] terminals)
	{
		var data = new TheoryData<string, LeakSource, Outcome>();
		foreach (Terminal<TCommand> t in terminals)
		{
			foreach (LeakSource source in Enum.GetValues<LeakSource>())
			{
				foreach (Outcome outcome in Enum.GetValues<Outcome>())
				{
					if (Applies(t.Traits, outcome)) data.Add(t.Name, source, outcome);
				}
			}
		}

		return data;
	}

	static DbScript ScriptFor(Outcome outcome, CancellationTokenSource? cancelDuringRead)
	{
		object?[][] three = [[1, "a"], [2, "b"], [3, "c"]];
		return outcome switch
		{
			Outcome.NoRows => new() { Rows = [] },
			Outcome.ReadThrowsFirst => new() { Rows = three, FailOnRead = 1 },
			Outcome.ReadThrowsLater => new() { Rows = three, FailOnRead = 2 },
			Outcome.MappingThrows => new() { Rows = [["not a number", "a"]] },
			Outcome.ExecuteThrows => new() { ExecuteFailure = new ExecuteFailed() },
			Outcome.CancelledDuringRead => new() { Rows = three, OnRead = n => { if (n == 2) cancelDuringRead!.Cancel(); } },
			Outcome.Abandoned => new() { Rows = three },
			_ => new(),
		};
	}

	[Theory]
	[MemberData(nameof(DbCases))]
	public static async Task Db_NothingIsLeftBehind(string terminal, LeakSource source, Outcome outcome)
		=> await Run(Array.Find(DbTerminals, t => t.Name == terminal)!, source, outcome, (tally, src, pool) => tally.Command(src, pool), TestContext.Current.CancellationToken);

	[Theory]
	[MemberData(nameof(InterfaceCases))]
	public static async Task Interface_NothingIsLeftBehind(string terminal, LeakSource source, Outcome outcome)
		=> await Run(Array.Find(InterfaceTerminals, t => t.Name == terminal)!, source, outcome, (tally, src, pool) => src switch
		{
			LeakSource.Factory => ((IDbConnectionFactory)tally.Factory).Command("select 1"),
			LeakSource.Pool => ((IDbConnectionPool)pool).Command("select 1"),
			LeakSource.BareClosed => ((IDbConnection)tally.NewConnection()).Command("select 1"),
			LeakSource.BareOpen => ((IDbConnection)tally.NewOpenConnection()).Command("select 1"),
			_ => ((IDbTransaction)tally.NewOpenConnection().BeginTransaction()).Command("select 1"),
		}, TestContext.Current.CancellationToken);

	static async Task Run<TCommand>(
		Terminal<TCommand> terminal, LeakSource source, Outcome outcome,
		Func<DbTally, LeakSource, TrackedPool, TCommand> build,
		CancellationToken testToken)
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(testToken);
		var tally = new DbTally(ScriptFor(outcome, cts));
		TrackedPool pool = tally.Pool;
		TCommand command = build(tally, source, pool);

		if (outcome == Outcome.CancelledBefore) await cts.CancelAsync();

		Exception? thrown = null;
		try
		{
			await terminal.Run(command, cts.Token);
		}
		catch (Exception ex)
		{
			thrown = ex;
		}

		bool reader = (terminal.Traits & Traits.Reader) != 0;
		bool cancelledBefore = outcome == Outcome.CancelledBefore;
		bool executeFailed = outcome == Outcome.ExecuteThrows;

		// What the call itself did.
		switch (outcome)
		{
			case Outcome.Success or Outcome.NoRows or Outcome.Abandoned:
				Assert.Null(thrown);
				break;
			case Outcome.ExecuteThrows:
				Assert.IsType<ExecuteFailed>(thrown);
				break;
			case Outcome.CancelledBefore:
				Assert.IsType<OperationCanceledException>(thrown, exactMatch: false);
				break;
			case Outcome.ReadThrowsFirst or Outcome.MappingThrows:
				Assert.NotNull(thrown);
				break;
		}

		// Commands and readers: created only when the call got that far, and each disposed exactly once.
		Assert.Equal(cancelledBefore ? 0 : 1, tally.Commands.Count);
		Assert.Equal(reader && !cancelledBefore && !executeFailed ? 1 : 0, tally.Readers.Count);
		LeakAssert.CommandsAndReadersDisposed(tally);

		// Connections, by who owns them.
		LeakAssert.ConnectionsReleased(tally, source, pool, cancelledBefore ? 0 : 1);
	}
}
