extern alias mssql;

using System.Collections;
using System.Data.Common;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// command.SingleResult().Map<T>() is exactly command.Map<T>().SingleResult():
// the query reads only the first result set, so a failure in a later one is not seen,
// where without it the terminal reads through and throws it.
// It is written before Map on an expressive command (IExecuteReader) and on a raw command (IDbCommand, DbCommand),
// and the form after Map keeps working.
[ExcludeFromCodeCoverage]
public static class SingleResultBeforeMapTests
{
	sealed record IdRow(int Id);

	sealed record IdName(int Id, string? Name);

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	const int LaterSets = 2;

	const CommandBehavior Generated = CommandBehavior.SingleResult | CommandBehavior.SequentialAccess;

	static FakeExecuteReader Expressive(int rows, out Func<MultiSetReader> reader, int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		return new FakeExecuteReader(() => last = new MultiSetReader(Rows(rows), LaterSets, failAt));
	}

	static (FakeDbConnection Connection, FakeDbCommand Command) Raw(int rows, out Func<MultiSetReader> reader, int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		var connection = new FakeDbConnection();
		return (connection, new FakeDbCommand(connection, () => last = new MultiSetReader(Rows(rows), LaterSets, failAt)));
	}

	// An IDbCommand that is not a DbCommand: the synchronous path of the raw command view.
	static IDbCommand Plain(int rows, out Func<MultiSetReader> reader, List<CommandBehavior> behaviors, CommandType type = CommandType.Text, int? failAt = null)
	{
		MultiSetReader? last = null;
		reader = () => last ?? throw new InvalidOperationException("never executed");
		IDbConnection connection = Substitute.For<IDbConnection>();
		connection.State.Returns(ConnectionState.Open);
		IDbCommand command = Substitute.For<IDbCommand>();
		command.Connection.Returns(connection);
		command.CommandType = type;
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(call =>
		{
			behaviors.Add(call.Arg<CommandBehavior>());
			return last = new MultiSetReader(Rows(rows), LaterSets, failAt);
		});

		return command;
	}

	// ---- every terminal ----

	public static TheoryData<string> Terminals
		=> [
			"ToList", "ToArray", "ToImmutableArray", "FirstOrDefault", "SingleOrDefault",
			"ToDictionary", "ToHashSet", "Read", "ReadResult",
			"Skip", "Take", "SkipTake", "TakeFirst"];

	// SingleOrDefault throws on a second row, so it reads a one-row set.
	static int RowCount(string terminal)
		=> terminal == "SingleOrDefault" ? 1 : 3;

	static string Show(object? result)
		=> result is IEnumerable items
			? string.Join(",", items.Cast<object?>())
			: result?.ToString() ?? "null";

	static int Sum(Action<Action<IEnumerable<int>>> read)
	{
		int sum = 0;
		read(items =>
		{
			foreach (int i in items) sum += i;
		});

		return sum;
	}

	static async Task<int> SumAsync(Func<Func<IAsyncEnumerable<int>, CancellationToken, ValueTask>, ValueTask> read)
	{
		int sum = 0;
		await read(async (items, token) =>
		{
			await foreach (int i in items.WithCancellation(token)) sum += i;
		});

		return sum;
	}

	static object? RunSync(MappedQuery<int> q, string terminal)
	{
		return terminal switch
		{
			"ToList" => q.ToList(),
			"ToArray" => q.ToArray(),
			"ToImmutableArray" => q.ToImmutableArray(),
			"FirstOrDefault" => q.FirstOrDefault(),
			"SingleOrDefault" => q.SingleOrDefault(),
			"ToDictionary" => q.ToDictionary(i => i),
			"ToHashSet" => q.ToHashSet(),
			"Read" => Sum(q.Read),
			"ReadResult" => q.Read(items => items.Sum()),
			"Skip" => q.Skip(1).ToList(),
			"Take" => q.Take(2).ToList(),
			"SkipTake" => q.Skip(1).Take(1).ToList(),
			"TakeFirst" => q.Take(2).FirstOrDefault(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};
	}

	static object? RunSync(MappedCommand<int> q, string terminal)
	{
		return terminal switch
		{
			"ToList" => q.ToList(),
			"ToArray" => q.ToArray(),
			"ToImmutableArray" => q.ToImmutableArray(),
			"FirstOrDefault" => q.FirstOrDefault(),
			"SingleOrDefault" => q.SingleOrDefault(),
			"ToDictionary" => q.ToDictionary(i => i),
			"ToHashSet" => q.ToHashSet(),
			"Read" => Sum(q.Read),
			"ReadResult" => q.Read(items => items.Sum()),
			"Skip" => q.Skip(1).ToList(),
			"Take" => q.Take(2).ToList(),
			"SkipTake" => q.Skip(1).Take(1).ToList(),
			"TakeFirst" => q.Take(2).FirstOrDefault(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};
	}

	static async Task<object?> RunAsync(MappedQuery<int> q, string terminal)
	{
		return terminal switch
		{
			"ToList" => await q.ToListAsync(Ct),
			"ToArray" => await q.ToArrayAsync(Ct),
			"ToImmutableArray" => await q.ToImmutableArrayAsync(Ct),
			"FirstOrDefault" => await q.FirstOrDefaultAsync(Ct),
			"SingleOrDefault" => await q.SingleOrDefaultAsync(Ct),
			"ToDictionary" => await q.ToDictionaryAsync(i => i, null, Ct),
			"ToHashSet" => await q.ToHashSetAsync(null, Ct),
			"Read" => await SumAsync(handler => q.ReadAsync(handler, Ct)),
			"ReadResult" =>
				await q.ReadAsync(
					async (items, token) =>
					{
						int sum = 0;
						await foreach (int i in items.WithCancellation(token)) sum += i;
						sum;
					},
					Ct),
			"Skip" => await q.Skip(1).ToListAsync(Ct),
			"Take" => await q.Take(2).ToListAsync(Ct),
			"SkipTake" => await q.Skip(1).Take(1).ToListAsync(Ct),
			"TakeFirst" => await q.Take(2).FirstOrDefaultAsync(Ct),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};
	}

	static async Task<object?> RunAsync(MappedCommand<int> q, string terminal)
	{
		return terminal switch
		{
			"ToList" => await q.ToListAsync(Ct),
			"ToArray" => await q.ToArrayAsync(Ct),
			"ToImmutableArray" => await q.ToImmutableArrayAsync(Ct),
			"FirstOrDefault" => await q.FirstOrDefaultAsync(Ct),
			"SingleOrDefault" => await q.SingleOrDefaultAsync(Ct),
			"ToDictionary" => await q.ToDictionaryAsync(i => i, null, Ct),
			"ToHashSet" => await q.ToHashSetAsync(null, Ct),
			"Read" => await SumAsync(handler => q.ReadAsync(handler, Ct)),
			"ReadResult" =>
				await q.ReadAsync(
					async (items, token) =>
					{
						int sum = 0;
						await foreach (int i in items.WithCancellation(token)) sum += i;
						sum;
					},
					Ct),
			"Skip" => await q.Skip(1).ToListAsync(Ct),
			"Take" => await q.Take(2).ToListAsync(Ct),
			"SkipTake" => await q.Skip(1).Take(1).ToListAsync(Ct),
			"TakeFirst" => await q.Take(2).FirstOrDefaultAsync(Ct),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};
	}

	// ---- an expressive command (IExecuteReader) ----

	[Theory, MemberData(nameof(Terminals))]
	public static void Expressive_BeforeMap_IsTheSameAsAfterMap(string terminal)
	{
		FakeExecuteReader before = Expressive(RowCount(terminal), out Func<MultiSetReader> beforeReader);
		FakeExecuteReader after = Expressive(RowCount(terminal), out Func<MultiSetReader> afterReader);

		object? expected = RunSync(after.Map<int>().SingleResult(), terminal);
		object? actual = RunSync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(before.Behaviors);
		Assert.Equal(after.Behaviors, before.Behaviors);
		Assert.All(before.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task Expressive_BeforeMap_IsTheSameAsAfterMap_Async(string terminal)
	{
		FakeExecuteReader before = Expressive(RowCount(terminal), out Func<MultiSetReader> beforeReader);
		FakeExecuteReader after = Expressive(RowCount(terminal), out Func<MultiSetReader> afterReader);

		object? expected = await RunAsync(after.Map<int>().SingleResult(), terminal);
		object? actual = await RunAsync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(before.Behaviors);
		Assert.Equal(after.Behaviors, before.Behaviors);
		Assert.All(before.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static void Expressive_ALaterFailure_IsSeenWithoutTheOptOut_AndNotWithIt(string terminal)
	{
		FakeExecuteReader plain = Expressive(RowCount(terminal), out Func<MultiSetReader> plainReader, failAt: 1);
		FakeExecuteReader single = Expressive(RowCount(terminal), out Func<MultiSetReader> singleReader, failAt: 1);

		Assert.Throws<LaterSetFailure>(() => RunSync(plain.Map<int>(), terminal));
		Assert.Equal(1, plainReader().NextResultCalls);

		RunSync(single.SingleResult().Map<int>(), terminal);
		Assert.Equal(0, singleReader().NextResultCalls);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task Expressive_ALaterFailure_IsSeenWithoutTheOptOut_AndNotWithIt_Async(string terminal)
	{
		FakeExecuteReader plain = Expressive(RowCount(terminal), out Func<MultiSetReader> plainReader, failAt: 1);
		FakeExecuteReader single = Expressive(RowCount(terminal), out Func<MultiSetReader> singleReader, failAt: 1);

		await Assert.ThrowsAsync<LaterSetFailure>(() => RunAsync(plain.Map<int>(), terminal));
		Assert.Equal(1, plainReader().NextResultCalls);

		await RunAsync(single.SingleResult().Map<int>(), terminal);
		Assert.Equal(0, singleReader().NextResultCalls);
	}

	// ---- a raw DbCommand ----

	[Theory, MemberData(nameof(Terminals))]
	public static void DbCommand_BeforeMap_IsTheSameAsAfterMap(string terminal)
	{
		(FakeDbConnection beforeConnection, FakeDbCommand before) = Raw(RowCount(terminal), out Func<MultiSetReader> beforeReader);
		(_, FakeDbCommand after) = Raw(RowCount(terminal), out Func<MultiSetReader> afterReader);

		object? expected = RunSync(after.Map<int>().SingleResult(), terminal);
		object? actual = RunSync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(before.Behaviors);
		Assert.Equal(after.Behaviors, before.Behaviors);
		Assert.All(before.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
		Assert.Equal(["open", "close"], beforeConnection.Log);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task DbCommand_BeforeMap_IsTheSameAsAfterMap_Async(string terminal)
	{
		(FakeDbConnection beforeConnection, FakeDbCommand before) = Raw(RowCount(terminal), out Func<MultiSetReader> beforeReader);
		(_, FakeDbCommand after) = Raw(RowCount(terminal), out Func<MultiSetReader> afterReader);

		object? expected = await RunAsync(after.Map<int>().SingleResult(), terminal);
		object? actual = await RunAsync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(before.Behaviors);
		Assert.Equal(after.Behaviors, before.Behaviors);
		Assert.All(before.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
		Assert.Equal(["open", "close"], beforeConnection.Log);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static void DbCommand_ALaterFailure_IsSeenWithoutTheOptOut_AndNotWithIt(string terminal)
	{
		(_, FakeDbCommand plain) = Raw(RowCount(terminal), out Func<MultiSetReader> plainReader, failAt: 1);
		(FakeDbConnection connection, FakeDbCommand single) = Raw(RowCount(terminal), out Func<MultiSetReader> singleReader, failAt: 1);

		Assert.Throws<LaterSetFailure>(() => RunSync(plain.Map<int>(), terminal));
		Assert.Equal(1, plainReader().NextResultCalls);

		RunSync(single.SingleResult().Map<int>(), terminal);
		Assert.Equal(0, singleReader().NextResultCalls);
		Assert.Equal(["open", "close"], connection.Log);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task DbCommand_ALaterFailure_IsSeenWithoutTheOptOut_AndNotWithIt_Async(string terminal)
	{
		(_, FakeDbCommand plain) = Raw(RowCount(terminal), out Func<MultiSetReader> plainReader, failAt: 1);
		(FakeDbConnection connection, FakeDbCommand single) = Raw(RowCount(terminal), out Func<MultiSetReader> singleReader, failAt: 1);

		await Assert.ThrowsAsync<LaterSetFailure>(() => RunAsync(plain.Map<int>(), terminal));
		Assert.Equal(1, plainReader().NextResultCalls);

		await RunAsync(single.SingleResult().Map<int>(), terminal);
		Assert.Equal(0, singleReader().NextResultCalls);
		Assert.Equal(["open", "close"], connection.Log);
	}

	// ---- an IDbCommand that is not a DbCommand ----

	[Theory, MemberData(nameof(Terminals))]
	public static void IDbCommand_BeforeMap_IsTheSameAsAfterMap(string terminal)
	{
		List<CommandBehavior> beforeBehaviors = [];
		List<CommandBehavior> afterBehaviors = [];
		IDbCommand before = Plain(RowCount(terminal), out Func<MultiSetReader> beforeReader, beforeBehaviors);
		IDbCommand after = Plain(RowCount(terminal), out Func<MultiSetReader> afterReader, afterBehaviors);

		object? expected = RunSync(after.Map<int>().SingleResult(), terminal);
		object? actual = RunSync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(beforeBehaviors);
		Assert.Equal(afterBehaviors, beforeBehaviors);
		Assert.All(beforeBehaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
	}

	[Theory, MemberData(nameof(Terminals))]
	public static async Task IDbCommand_BeforeMap_IsTheSameAsAfterMap_Async(string terminal)
	{
		List<CommandBehavior> beforeBehaviors = [];
		List<CommandBehavior> afterBehaviors = [];
		IDbCommand before = Plain(RowCount(terminal), out Func<MultiSetReader> beforeReader, beforeBehaviors);
		IDbCommand after = Plain(RowCount(terminal), out Func<MultiSetReader> afterReader, afterBehaviors);

		object? expected = await RunAsync(after.Map<int>().SingleResult(), terminal);
		object? actual = await RunAsync(before.SingleResult().Map<int>(), terminal);

		Assert.Equal(Show(expected), Show(actual));
		Assert.NotEmpty(beforeBehaviors);
		Assert.Equal(afterBehaviors, beforeBehaviors);
		Assert.All(beforeBehaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(0, beforeReader().NextResultCalls);
		Assert.Equal(0, afterReader().NextResultCalls);
	}

	// ---- text and stored-procedure commands: the command type changes nothing ----

	[Theory]
	[InlineData(CommandType.Text)]
	[InlineData(CommandType.StoredProcedure)]
	public static async Task ARealExpressiveCommand_OfEitherType_DoesNotSeeALaterFailure_WithTheOptOut(CommandType type)
	{
		(IDbConnection connection, Func<MultiSetReader> reader, List<CommandBehavior> behaviors) = ConnectionOf(type, failAt: 1);

		Assert.Throws<LaterSetFailure>(
			() => new ExpressiveCommand(connection, type, "dbo.p").Map<int>().ToList());
		Assert.Equal(1, reader().NextResultCalls);

		Assert.Equal([1, 2], new ExpressiveCommand(connection, type, "dbo.p").SingleResult().Map<int>().ToList());
		Assert.Equal(0, reader().NextResultCalls);
		Assert.Equal(
			[1, 2],
			await new ExpressiveCommand(connection, type, "dbo.p").SingleResult().Map<int>().ToListAsync(Ct));
		Assert.Equal(0, reader().NextResultCalls);
		Assert.All(behaviors.Skip(1), behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(3, behaviors.Count);
	}

	static (IDbConnection Connection, Func<MultiSetReader> Reader, List<CommandBehavior> Behaviors) ConnectionOf(CommandType type, int? failAt)
	{
		List<CommandBehavior> behaviors = [];
		IDbCommand command = Plain(2, out Func<MultiSetReader> reader, behaviors, type, failAt);
		IDbConnection connection = command.Connection!;
		connection.CreateCommand().Returns(command);
		return (connection, reader, behaviors);
	}

	[Theory]
	[InlineData(CommandType.Text)]
	[InlineData(CommandType.StoredProcedure)]
	public static async Task ADbCommand_OfEitherType_DoesNotSeeALaterFailure_WithTheOptOut(CommandType type)
	{
		(_, FakeDbCommand plain) = Raw(2, out Func<MultiSetReader> plainReader, failAt: 1);
		(_, FakeDbCommand single) = Raw(2, out Func<MultiSetReader> singleReader, failAt: 1);
		plain.CommandType = type;
		single.CommandType = type;

		Assert.Throws<LaterSetFailure>(() => plain.Map<int>().ToList());
		Assert.Equal(1, plainReader().NextResultCalls);

		Assert.Equal([1, 2], single.SingleResult().Map<int>().ToList());
		Assert.Equal([1, 2], await single.SingleResult().Map<int>().ToListAsync(Ct));
		Assert.Equal(0, singleReader().NextResultCalls);
	}

	[Theory]
	[InlineData(CommandType.Text)]
	[InlineData(CommandType.StoredProcedure)]
	public static async Task AnIDbCommand_OfEitherType_DoesNotSeeALaterFailure_WithTheOptOut(CommandType type)
	{
		List<CommandBehavior> plainBehaviors = [];
		List<CommandBehavior> singleBehaviors = [];
		IDbCommand plain = Plain(2, out Func<MultiSetReader> plainReader, plainBehaviors, type, failAt: 1);
		IDbCommand single = Plain(2, out Func<MultiSetReader> singleReader, singleBehaviors, type, failAt: 1);

		await Assert.ThrowsAsync<LaterSetFailure>(async () => await plain.Map<int>().ToListAsync(Ct));
		Assert.Equal(1, plainReader().NextResultCalls);

		Assert.Equal([1, 2], single.SingleResult().Map<int>().ToList());
		Assert.Equal([1, 2], await single.SingleResult().Map<int>().ToListAsync(Ct));
		Assert.Equal(0, singleReader().NextResultCalls);
		Assert.All(singleBehaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
	}

	// ---- the five Map overloads that follow keep their parameter lists ----

	[Fact]
	public static void EveryMapOverload_AfterSingleResult_OnAnExpressiveCommand_ReturnsAMappedQuery_WithTheFlagSet()
	{
		FakeExecuteReader command = Expressive(2, out Func<MultiSetReader> reader, failAt: 1);
		KeyValuePair<string, string?> id = new("Id", "Id");
		KeyValuePair<string, string?> name = new("Name", "Name");

		MappedQuery<IdName> model = command.SingleResult().Map<IdName>();
		MappedQuery<IdName> withMap = command.SingleResult().Map(RowMap<IdName>.Default);
		MappedQuery<IdName> withNull = command.SingleResult().Map((RowMap<IdName>?)null);
		MappedQuery<IdRow> withPair = command.SingleResult().Map<IdRow>(id);
		MappedQuery<IdName> withPairs = command.SingleResult().Map<IdName>(id, name);
		MappedQuery<int> column = command.SingleResult().Map<int>();
		MappedQuery<int> selector = command.SingleResult().Map(r => r.GetInt32(0));

		Assert.Equal([1, 2], model.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withMap.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withNull.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withPair.ToList().Select(r => r.Id));
		Assert.Equal(["n1", "n2"], withPairs.ToList().Select(r => r.Name));
		Assert.Equal([1, 2], column.ToList());
		Assert.Equal([1, 2], selector.ToList());

		CommandBehavior[] expected = [Generated, Generated, Generated, Generated, Generated, Generated, CommandBehavior.SingleResult];
		Assert.Equal(expected, command.Behaviors);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static void EveryMapOverload_AfterSingleResult_OnARawCommand_ReturnsAMappedCommand_WithTheFlagSet()
	{
		(_, FakeDbCommand command) = Raw(2, out Func<MultiSetReader> reader, failAt: 1);
		KeyValuePair<string, string?> id = new("Id", "Id");
		KeyValuePair<string, string?> name = new("Name", "Name");

		MappedCommand<IdName> model = command.SingleResult().Map<IdName>();
		MappedCommand<IdName> withMap = command.SingleResult().Map(RowMap<IdName>.Default);
		MappedCommand<IdName> withNull = command.SingleResult().Map((RowMap<IdName>?)null);
		MappedCommand<IdRow> withPair = command.SingleResult().Map<IdRow>(id);
		MappedCommand<IdName> withPairs = command.SingleResult().Map<IdName>(id, name);
		MappedCommand<int> column = command.SingleResult().Map<int>();
		MappedCommand<int> selector = command.SingleResult().Map(r => r.GetInt32(0));

		Assert.Equal([1, 2], model.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withMap.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withNull.ToList().Select(r => r.Id));
		Assert.Equal([1, 2], withPair.ToList().Select(r => r.Id));
		Assert.Equal(["n1", "n2"], withPairs.ToList().Select(r => r.Name));
		Assert.Equal([1, 2], column.ToList());
		Assert.Equal([1, 2], selector.ToList());

		Assert.All(command.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
		Assert.Equal(7, command.Behaviors.Count);
		Assert.Equal(0, reader().NextResultCalls);
	}

	[Fact]
	public static void AGeneratedMapping_KeepsSequentialAccess_AndASelectorDoesNot()
	{
		FakeExecuteReader command = Expressive(2, out _);

		command.SingleResult().Map<IdRow>().ToList();
		command.SingleResult().Map(r => r.GetInt32(0)).ToList();
		command.SingleResult().Map<int>().FirstOrDefault();

		Assert.Equal(
			[Generated, CommandBehavior.SingleResult, Generated | CommandBehavior.SingleRow],
			command.Behaviors);
	}

	[Fact]
	public static void SkipAndTake_AfterMap_KeepTheOptOut()
	{
		FakeExecuteReader command = Expressive(4, out Func<MultiSetReader> reader, failAt: 1);

		Assert.Equal([2, 3, 4], command.SingleResult().Map<int>().Skip(1).ToList());
		Assert.Equal([1, 2], command.SingleResult().Map<int>().Take(2).ToList());
		Assert.Equal([2, 3], command.SingleResult().Map<int>().Skip(1).Take(2).ToList());
		Assert.Equal(2, command.SingleResult().Map<int>().Skip(1).FirstOrDefault());

		Assert.Equal(0, reader().NextResultCalls);
		Assert.All(command.Behaviors, behavior => Assert.True(behavior.HasFlag(CommandBehavior.SingleResult)));
	}

	// ---- the form after Map keeps working, and the two can be mixed ----

	[Fact]
	public static void TheFormAfterMap_StillWorks_OnEveryCommandKind()
	{
		FakeExecuteReader expressive = Expressive(2, out Func<MultiSetReader> expressiveReader, failAt: 1);
		(_, FakeDbCommand raw) = Raw(2, out Func<MultiSetReader> rawReader, failAt: 1);
		List<CommandBehavior> behaviors = [];
		IDbCommand plain = Plain(2, out Func<MultiSetReader> plainReader, behaviors, failAt: 1);

		Assert.Equal([1, 2], expressive.Map<int>().SingleResult().ToList());
		Assert.Equal([1, 2], raw.Map<int>().SingleResult().ToList());
		Assert.Equal([1, 2], plain.Map<int>().SingleResult().ToList());

		Assert.Equal(0, expressiveReader().NextResultCalls);
		Assert.Equal(0, rawReader().NextResultCalls);
		Assert.Equal(0, plainReader().NextResultCalls);
	}

	[Fact]
	public static void BothForms_Together_AreTheSameAsOne()
	{
		FakeExecuteReader expressive = Expressive(2, out Func<MultiSetReader> expressiveReader, failAt: 1);
		(_, FakeDbCommand raw) = Raw(2, out Func<MultiSetReader> rawReader, failAt: 1);

		Assert.Equal([1, 2], expressive.SingleResult().Map<int>().SingleResult().ToList());
		Assert.Equal([1, 2], raw.SingleResult().Map<int>().SingleResult().ToList());
		Assert.Equal([Generated], expressive.Behaviors);
		Assert.Equal([Generated], raw.Behaviors.Select(b => b & ~CommandBehavior.CloseConnection));
		Assert.Equal(0, expressiveReader().NextResultCalls);
		Assert.Equal(0, rawReader().NextResultCalls);
	}

	[Fact]
	public static void CallingSingleResultTwice_ReturnsTheSameView()
	{
		FakeExecuteReader expressive = Expressive(1, out _);
		(_, FakeDbCommand raw) = Raw(1, out _);

		IExecuteReader single = expressive.SingleResult();
		IDbCommand singleRaw = raw.SingleResult();

		Assert.NotSame(expressive, single);
		Assert.NotSame(raw, singleRaw);
		Assert.Same(single, single.SingleResult());
		Assert.Same(singleRaw, singleRaw.SingleResult());
	}

	// ---- the command is not changed ----

	[Fact]
	public static void TheCommandItself_StillReadsThrough_AfterAnOptedOutQuery()
	{
		FakeExecuteReader command = Expressive(2, out Func<MultiSetReader> reader);
		IExecuteReader single = command.SingleResult();

		single.Map<int>().ToList();
		Assert.Equal(0, reader().NextResultCalls);

		command.Map<int>().ToList();
		Assert.Equal(LaterSets + 1, reader().NextResultCalls);

		single.Map<int>().ToList();
		Assert.Equal(0, reader().NextResultCalls);

		Assert.Equal([Generated, CommandBehavior.SequentialAccess, Generated], command.Behaviors);
	}

	[Fact]
	public static void TheRawCommandItself_StillReadsThrough_AfterAnOptedOutQuery()
	{
		(_, FakeDbCommand command) = Raw(2, out Func<MultiSetReader> reader, failAt: 1);
		IDbCommand single = command.SingleResult();

		single.Map<int>().ToList();
		Assert.Equal(0, reader().NextResultCalls);

		Assert.Throws<LaterSetFailure>(() => command.Map<int>().ToList());
		Assert.Equal(1, reader().NextResultCalls);
	}

	// ---- nulls and cancellation ----

	[Fact]
	public static void ANullCommand_Throws()
	{
		IExecuteReader? expressive = null;
		IDbCommand? plain = null;
		DbCommand? db = null;

		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => expressive!.SingleResult()).ParamName);
		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => plain!.SingleResult()).ParamName);
		Assert.Equal("command", Assert.Throws<ArgumentNullException>(() => db!.SingleResult()).ParamName);
	}

	static RecordingAsyncExecuteReader Recording(bool useAsyncRead = false)
		=> new(["Id"], [1], [2]) { UseAsyncRead = useAsyncRead };

	[Fact]
	public static async Task ACanceledCommandToken_StopsTheExpressiveQuery_BeforeItRuns()
	{
		using var cts = new CancellationTokenSource();
		await cts.CancelAsync();
		RecordingAsyncExecuteReader command = Recording();
		command.CancellationToken = cts.Token;

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command.SingleResult().Map<int>().ToListAsync(Ct));
	}

	// Either token cancels the call, as it does without the opt-out.
	[Theory]
	[InlineData(true)]
	[InlineData(false)]
	public static async Task TheCommandsToken_AndTheTerminalsToken_BothCancelTheQuery(bool cancelTerminalToken)
	{
		using var commandSource = new CancellationTokenSource();
		using var terminalSource = new CancellationTokenSource();
		RecordingAsyncExecuteReader command = Recording();
		command.CancellationToken = commandSource.Token;
		command.Stall = true;

		Task<List<int>> task = command
			.SingleResult()
			.Map<int>()
			.ToListAsync(terminalSource.Token)
			.AsTask();
		await command.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

		CancellationToken seen = Assert.Single(command.ReaderTokens);
		Assert.True(seen.CanBeCanceled);
		await (cancelTerminalToken ? terminalSource : commandSource).CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
	}

	[Fact]
	public static async Task ACanceledToken_ThrowsBeforeAnythingOpens_OnARawCommand()
	{
		(FakeDbConnection connection, FakeDbCommand command) = Raw(2, out _);
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await command.SingleResult().Map<int>().ToListAsync(cts.Token));

		Assert.Empty(connection.Log);
		Assert.Empty(command.Behaviors);
	}

	// ---- the view, used on its own as the interface it returns ----

	[Fact]
	public static async Task TheExpressiveView_AddsSingleResultToEveryExecution_AndForwardsTheRest()
	{
		using var cts = new CancellationTokenSource();
		var command = new FakeExecuteReader(() => Reader(Rows(1))) { CancellationToken = cts.Token };
		IExecuteReader view = command.SingleResult();

		view.ExecuteReader(_ => { });
		view.ExecuteReader(_ => 1, CommandBehavior.SequentialAccess);
		view.ExecuteReader(0, (_, state) => state, CommandBehavior.KeyInfo);
		await view.ExecuteReaderAsync(_ => ValueTask.CompletedTask);
		await view.ExecuteReaderAsync(_ => ValueTask.FromResult(1), CommandBehavior.SequentialAccess);
		await view.ExecuteReaderAsync(0, (_, state) => ValueTask.FromResult(state), CommandBehavior.KeyInfo);
		await view.ExecuteReaderAsync(0, (_, state) => ValueTask.FromResult(state), CommandBehavior.KeyInfo, Ct);

		Assert.Equal(cts.Token, view.CancellationToken);
		Assert.Equal(
			[
				CommandBehavior.SingleResult,
				CommandBehavior.SingleResult | CommandBehavior.SequentialAccess,
				CommandBehavior.SingleResult | CommandBehavior.KeyInfo,
				CommandBehavior.SingleResult,
				CommandBehavior.SingleResult | CommandBehavior.SequentialAccess,
				CommandBehavior.SingleResult | CommandBehavior.KeyInfo,
				CommandBehavior.SingleResult | CommandBehavior.KeyInfo
			],
			command.Behaviors);
		Assert.Equal(Ct, command.ReaderTokens.Last());
	}

	[Fact]
	public static void TheExpressiveView_ReportsTheCommandsUseAsyncRead()
	{
		RecordingAsyncExecuteReader reading = Recording(useAsyncRead: true);
		FakeExecuteReader other = Expressive(1, out _);

		Assert.True(Assert.IsAssignableFrom<IExecuteReaderAsync>(reading.SingleResult()).UseAsyncRead);
		Assert.False(Assert.IsAssignableFrom<IExecuteReaderAsync>(other.SingleResult()).UseAsyncRead);
	}

	[Fact]
	public static void TheRawView_ForwardsEveryMember_AndAddsSingleResultToTheReaders()
	{
		IDbCommand inner = Substitute.For<IDbCommand>();
		IDbCommand view = inner.SingleResult();
		IDbConnection connection = Substitute.For<IDbConnection>();
		IDbTransaction transaction = Substitute.For<IDbTransaction>();

		view.Connection = connection;
		view.Transaction = transaction;
		view.CommandText = "dbo.p";
		view.CommandTimeout = 7;
		view.CommandType = CommandType.StoredProcedure;
		view.UpdatedRowSource = UpdateRowSource.None;

		Assert.Same(connection, inner.Connection);
		Assert.Same(transaction, inner.Transaction);
		Assert.Equal("dbo.p", inner.CommandText);
		Assert.Equal(7, inner.CommandTimeout);
		Assert.Equal(CommandType.StoredProcedure, inner.CommandType);
		Assert.Equal(UpdateRowSource.None, inner.UpdatedRowSource);
		Assert.Same(connection, view.Connection);
		Assert.Same(transaction, view.Transaction);
		Assert.Equal("dbo.p", view.CommandText);
		Assert.Equal(7, view.CommandTimeout);
		Assert.Equal(CommandType.StoredProcedure, view.CommandType);
		Assert.Equal(UpdateRowSource.None, view.UpdatedRowSource);
		Assert.Same(inner.Parameters, view.Parameters);

		view.Cancel();
		view.Prepare();
		view.CreateParameter();
		view.ExecuteNonQuery();
		view.ExecuteScalar();
		view.ExecuteReader();
		view.ExecuteReader(CommandBehavior.SequentialAccess);
		view.Dispose();

		inner.Received(1).Cancel();
		inner.Received(1).Prepare();
		inner.Received(1).CreateParameter();
		inner.Received(1).ExecuteNonQuery();
		inner.Received(1).ExecuteScalar();
		inner.Received(1).ExecuteReader(CommandBehavior.SingleResult);
		inner.Received(1).ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);
		inner.Received(1).Dispose();
	}

	// ---- the library's own command types bind, with no ambiguity ----

	// The type argument is the check: it must compile against the exact type.
	static void Is<T>(T value)
		=> Assert.IsType<T>(value);

	[Fact]
	public static void TheLibrarysCommandTypes_BindToTheRightMap()
	{
#pragma warning disable CS0618 // System.Data.SqlClient is obsolete; it is one of the two packages under test
		using var legacyConnection = new System.Data.SqlClient.SqlConnection();
		using var legacyCommand = new System.Data.SqlClient.SqlCommand();
		var legacyExpressive = new ExpressiveSqlCommand(legacyConnection, CommandType.Text, "select 1");
#pragma warning restore CS0618
		using var connection = new Microsoft.Data.SqlClient.SqlConnection();
		using var sqlCommand = new Microsoft.Data.SqlClient.SqlCommand();
		var expressiveSql = new mssql::Open.Database.Extensions.ExpressiveSqlCommand(connection, CommandType.Text, "select 1");
		var expressive = new ExpressiveCommand(Substitute.For<IDbConnection>(), CommandType.Text, "select 1");
		var expressiveDb = new ExpressiveDbCommand(new FakeDbConnection(), CommandType.Text, "select 1");

		// The expressive commands implement IExecuteReader only.
		Is<MappedQuery<int>>(legacyExpressive.SingleResult().Map<int>());
		Is<MappedQuery<int>>(expressiveSql.SingleResult().Map<int>());
		Is<MappedQuery<int>>(expressive.SingleResult().Map<int>());
		Is<MappedQuery<int>>(expressiveDb.SingleResult().Map<int>());
		Is<MappedQuery<IdRow>>(expressiveDb.SingleResult().Map(RowMap<IdRow>.Default));
		Is<MappedQuery<int>>(expressiveDb.SingleResult().Map(r => r.GetInt32(0)));

		// Raw commands are IDbCommand only.
#pragma warning disable CS0618
		Is<MappedCommand<int>>(legacyCommand.SingleResult().Map<int>());
#pragma warning restore CS0618
		Is<MappedCommand<int>>(sqlCommand.SingleResult().Map<int>());
		Is<MappedCommand<IdRow>>(sqlCommand.SingleResult().Map(RowMap<IdRow>.Default));

		DbCommand db = sqlCommand;
		IDbCommand plain = sqlCommand;
		Is<MappedCommand<int>>(db.SingleResult().Map<int>());
		Is<MappedCommand<int>>(plain.SingleResult().Map<int>());
		Is<MappedCommand<int>>(db.SingleResult().Map(r => r.GetInt32(0)));
	}
}
