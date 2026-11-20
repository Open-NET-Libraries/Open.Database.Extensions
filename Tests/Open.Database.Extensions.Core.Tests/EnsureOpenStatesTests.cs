using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// EnsureOpen asks "does this connection have to be opened?" (NeedsOpen)
/// and returns the state it found, so that whoever opened it can close it
/// again. These tests run every combination of the ConnectionState flags
/// (0 to 31: any OR of Closed, Open, Connecting, Executing, Fetching and
/// Broken) through both EnsureOpen and EnsureOpenAsync.
/// </summary>
[ExcludeFromCodeCoverage]
public static class EnsureOpenStatesTests
{
	const ConnectionState OpenBit = ConnectionState.Open;
	const ConnectionState BrokenBit = ConnectionState.Broken;

	static readonly Func<ConnectionState, bool> NeedsOpen
		= typeof(ConnectionExtensions)
			.GetMethod(
				"NeedsOpen",
				BindingFlags.Static | BindingFlags.NonPublic)!
			.CreateDelegate<Func<ConnectionState, bool>>();

	/// <summary>Every combination of the flags: 0 to 31.</summary>
	public static TheoryData<int> AllStates { get; } = BuildAllStates();

	static TheoryData<int> BuildAllStates()
	{
		var data = new TheoryData<int>();
		for (int i = 0; i < 32; i++) data.Add(i);
		return data;
	}

	// Written out again here, from the plain rule, rather than
	// calling the code under test.
	static bool ExpectedNeedsOpen(ConnectionState state)
		=> (state & OpenBit) == 0 || (state & BrokenBit) != 0;

	// Close first only for a broken connection, then Open.
	static string ExpectedCalls(ConnectionState state, string open)
		=> (state & BrokenBit) != 0 ? $"Close,{open}"
		: ExpectedNeedsOpen(state) ? open
		: string.Empty;

	[Theory]
	[MemberData(nameof(AllStates))]
	public static void NeedsOpen_IsTheOpenBitNotSet_OrTheBrokenBitSet(int bits)
	{
		var state = (ConnectionState)bits;

		Assert.Equal(ExpectedNeedsOpen(state), NeedsOpen(state));
	}

	[Theory]
	[MemberData(nameof(AllStates))]
	public static void EnsureOpen_OpensExactlyWhenNeeded_ClosingABrokenOneFirst(
		int bits)
	{
		var state = (ConnectionState)bits;
		var probe = new ConnectionProbe(state);

		ConnectionState prior = probe.Connection.EnsureOpen();

		Assert.Equal(state, prior);
		Assert.Equal(ExpectedCalls(state, "Open"), probe.Log);
		Assert.Equal(
			NeedsOpen(state) ? ConnectionState.Open : state,
			probe.State);
	}

	[Theory]
	[MemberData(nameof(AllStates))]
	public static async Task EnsureOpenAsync_OpensExactlyWhenNeeded_ClosingABrokenOneFirst(
		int bits)
	{
		var state = (ConnectionState)bits;
		var probe = new ConnectionProbe(state, dbConnection: true);

		ConnectionState prior = await probe.Db!.EnsureOpenAsync(
			TestContext.Current.CancellationToken);

		Assert.Equal(state, prior);
		Assert.Equal(ExpectedCalls(state, "OpenAsync"), probe.Log);
		Assert.Equal(
			NeedsOpen(state) ? ConnectionState.Open : state,
			probe.State);
	}
}
