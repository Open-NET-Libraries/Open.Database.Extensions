using System.Data.Common;
using Open.Database.Extensions.Core;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ExpressiveDbCommand.RetrieveAsync takes a trailing CancellationToken on the three overloads that can.
// The method-group conversions fail to compile if a signature moves;
// the call forms fail to compile (or bind elsewhere) if an existing call stops binding to the same member;
// the behavior tests show each form runs the same query and honors the token.
[ExcludeFromCodeCoverage]
public static class ExpressiveRetrieveAsyncTokenTests
{
	static (TrackedConnection Connection, ExpressiveDbCommand Command) Make()
	{
		var tally = new DbTally(new DbScript { Columns = Columns, Rows = TwoRows });
		TrackedConnection connection = tally.NewConnection();
		return (connection, new ExpressiveDbCommand(connection, CommandType.Text, "select 1"));
	}

	[Fact]
	public static void EveryOverload_HasTheExpectedSignature()
	{
		ExpressiveDbCommand command = Make().Command;

		// Compile-time pins: each conversion names exactly one parameter list.
		Func<CancellationToken, ValueTask<QueryResultQueue<object[]>>> a = command.RetrieveAsync;
		Func<IEnumerable<int>, CancellationToken, ValueTask<QueryResultQueue<object[]>>> b = command.RetrieveAsync;
		Func<IEnumerable<string>, bool, CancellationToken, ValueTask<QueryResultQueue<object[]>>> c = command.RetrieveAsync;
		Func<int, IEnumerable<int>, ValueTask<QueryResultQueue<object[]>>> d = command.RetrieveAsync;
		Func<string, IEnumerable<string>, ValueTask<QueryResultQueue<object[]>>> e = command.RetrieveAsync;

		Assert.All<Delegate>([a, b, c, d, e], x => Assert.Same(command, x.Target));
		Assert.Equal(5, typeof(ExpressiveDbCommand).GetMethods().Count(m => m.Name == nameof(ExpressiveDbCommand.RetrieveAsync)));
	}

	[Fact]
	public static void TheTrailingToken_IsOptional_OnlyOnTheThreeOverloads()
	{
		var withToken = typeof(ExpressiveDbCommand).GetMethods()
			.Where(m => m.Name == nameof(ExpressiveDbCommand.RetrieveAsync))
			.Select(m => m.GetParameters())
			.Where(p => p.Length != 0 && p[^1].ParameterType == typeof(CancellationToken))
			.ToArray();

		Assert.Equal(3, withToken.Length);
		Assert.All(withToken, p => Assert.True(p[^1].IsOptional));
	}

#pragma warning disable xUnit1051 // The point is the forms that pass no token.
	[Fact]
	public static async Task EveryExistingCallForm_StillBinds_AndReturnsTheSameRows()
	{
		(TrackedConnection connection, ExpressiveDbCommand command) = Make();
		IEnumerable<int> ordinals = [2, 0];
		IEnumerable<string> names = ["Note", "Id"];

		// These compile only while each form still binds to a member of the shape it had.
		QueryResultQueue<object[]> all = await command.RetrieveAsync();
		QueryResultQueue<object[]> byOrdinals = await command.RetrieveAsync(ordinals);
		QueryResultQueue<object[]> byNames = await command.RetrieveAsync(names);
		QueryResultQueue<object[]> byNamesNormalized = await command.RetrieveAsync(names, true);
		QueryResultQueue<object[]> firstAndOthers = await command.RetrieveAsync(2, 0);
		QueryResultQueue<object[]> firstOnly = await command.RetrieveAsync(1);
		QueryResultQueue<object[]> namedAndOthers = await command.RetrieveAsync("Note", "Id");
		QueryResultQueue<object[]> nameOnly = await command.RetrieveAsync("Id");
		QueryResultQueue<object[]> defaultToken = await command.RetrieveAsync(default(CancellationToken));

		Assert.Equal("Id,Name,Note", string.Join(",", all.Names));
		Assert.Equal("Id,Name,Note", string.Join(",", defaultToken.Names)); // the token overload; a bare `default` literal is ambiguous between the overloads
		Assert.Equal("Note,Id", string.Join(",", byOrdinals.Names));
		Assert.Equal("Note,Id", string.Join(",", byNames.Names));
		Assert.All(
			new[] { all, byOrdinals, byNames, byNamesNormalized, firstAndOthers, firstOnly, namedAndOthers, nameOnly, defaultToken },
			r => Assert.Equal(2, r.Result.Count));
		Assert.Equal(ConnectionState.Closed, connection.State);
	}
#pragma warning restore xUnit1051

	[Theory]
	[InlineData("token")]
	[InlineData("ordinals, token")]
	[InlineData("names, token")]
	[InlineData("names, normalize, token")]
	public static async Task EveryTokenForm_Runs_AndALiveTokenChangesNothing(string variant)
	{
		(TrackedConnection connection, ExpressiveDbCommand command) = Make();

		QueryResultQueue<object[]> result = await RetrieveVia(command, variant, TestContext.Current.CancellationToken);

		Assert.Equal(2, result.Result.Count);
		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
	}

	[Theory]
	[InlineData("token")]
	[InlineData("ordinals, token")]
	[InlineData("names, token")]
	[InlineData("names, normalize, token")]
	public static async Task EveryTokenForm_ACancelledToken_Throws_AndLeavesNothingOpen(string variant)
	{
		(TrackedConnection connection, ExpressiveDbCommand command) = Make();
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await RetrieveVia(command, variant, cts.Token));

		Assert.Equal(ConnectionState.Closed, connection.State);
		Assert.Equal(connection.Opens, connection.Closes);
		Assert.Equal(0, connection.Disposes);
	}

	static ValueTask<QueryResultQueue<object[]>> RetrieveVia(
		ExpressiveDbCommand command,
		string variant,
		CancellationToken token)
	{
		IEnumerable<int> ordinals = [2, 0];
		IEnumerable<string> names = ["Note", "Id"];
		return variant switch
		{
			"token" => command.RetrieveAsync(token),
			"ordinals, token" => command.RetrieveAsync(ordinals, token),
			"names, token" => command.RetrieveAsync(names, cancellationToken: token),
			_ => command.RetrieveAsync(names, true, token), // "names, normalize, token"
		};
	}
}
