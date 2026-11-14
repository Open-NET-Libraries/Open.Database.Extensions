using System.Buffers;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Matching the members of an AddParams object keeps the name each parameter ends up with in an array rented from ArrayPool<string?>.Shared, which is process-wide:
// whoever returned an array without clearing it leaves its names behind. A member that the procedure (or the text) does not declare is not sent, and must not
// be sent under a name that a previous renter left in its slot. Each case dirties the pool between AddParams and the terminal, so the array the command rents is full of names.
[ExcludeFromCodeCoverage]
public static class PooledNameSlotTests
{
	const string Stale = "@stale";

	// Fills every slot of several arrays of the smallest size with a name and returns them WITHOUT clearing, as a careless renter would.
	static void DirtyThePool()
	{
		var held = new string?[32][];
		for (int i = 0; i < held.Length; i++)
		{
			held[i] = ArrayPool<string?>.Shared.Rent(16);
			Array.Fill(held[i], Stale);
		}

		for (int i = 0; i < held.Length; i++)
			ArrayPool<string?>.Shared.Return(held[i], clearArray: false);
	}

	sealed class Args
	{
		public int OrderId { get; set; } = 5;
		public string Name { get; set; } = "ann";
		public int Extra { get; set; } = 1;
	}

	sealed class OnlyExtra
	{
		public int Extra { get; set; } = 1;
	}

	static string[] Sorted(string[] names) => [.. names.Order(StringComparer.Ordinal)];

	// ---- a stored procedure that does not declare the member ----

	[Fact]
	public static void AnUnmatchedMember_OfAProcedure_IsNotSent_WhateverThePoolHolds()
	{
		var run = new SyncNameRun("@order_id");

		string[] names = run.Run(c => { c.AddParams(new OnlyExtra()); DirtyThePool(); });

		Assert.Empty(names);
	}

	[Fact]
	public static async Task AnUnmatchedMember_OfAProcedure_IsNotSent_WhateverThePoolHolds_Async()
	{
		var run = new AsyncNameRun("@order_id");

		string[] names = await run.RunAsync(c => { c.AddParams(new OnlyExtra()); DirtyThePool(); });

		Assert.Empty(names);
	}

	[Fact]
	public static void MatchedAndUnmatchedMembers_OfAProcedure_SendOnlyTheMatchedOnes_WhateverThePoolHolds()
	{
		var run = new SyncNameRun("@order_id", "@name");

		string[] names = run.Run(c => { c.AddParams(new Args()); DirtyThePool(); });

		Assert.Equal(["@name", "@order_id"], Sorted(names));
		Assert.Equal(5, run.Values["@order_id"]);
		Assert.Equal("ann", run.Values["@name"]);
	}

	[Fact]
	public static async Task MatchedAndUnmatchedMembers_OfAProcedure_SendOnlyTheMatchedOnes_WhateverThePoolHolds_Async()
	{
		var run = new AsyncNameRun("@order_id", "@name");

		string[] names = await run.RunAsync(c => { c.AddParams(new Args()); DirtyThePool(); });

		Assert.Equal(["@name", "@order_id"], Sorted(names));
		Assert.Equal(5, run.Values["@order_id"]);
		Assert.Equal("ann", run.Values["@name"]);
	}

	// ---- a text command: the same array holds the names resolved against the @name markers of the text ----

	[Fact]
	public static void AnUnmatchedMember_OfATextCommand_IsNotSent_WhateverThePoolHolds()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var command = new ExpressiveCommand(probe.Connection, CommandType.Text, "select 1 where a = @order_id and n = @Name");
		command.AddParams(new Args());
		DirtyThePool();

		command.ExecuteNonQuery();

		Assert.Equal(["@Name", "@order_id"], Sorted([.. probe.Parameters.Select(p => p.ParameterName)]));
	}

	[Fact]
	public static async Task AnUnmatchedMember_OfATextCommand_IsNotSent_WhateverThePoolHolds_Async()
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var command = new ExpressiveDbCommand(new DerivedParameterNameTests.OpenDbConnection(db), CommandType.Text, "select 1 where a = @order_id and n = @Name");
		command.AddParams(new Args());
		DirtyThePool();

		await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

		Assert.Equal(["@Name", "@order_id"], Sorted([.. db.Sent.Select(p => p.ParameterName)]));
	}

	[Fact]
	public static void OnlyUnmatchedMembers_OfATextCommand_SendNothing_WhateverThePoolHolds()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var command = new ExpressiveCommand(probe.Connection, CommandType.Text, "select 1 where a = @order_id");
		command.AddParams(new OnlyExtra());
		DirtyThePool();

		command.ExecuteNonQuery();

		Assert.Empty(probe.Parameters);
	}
}
