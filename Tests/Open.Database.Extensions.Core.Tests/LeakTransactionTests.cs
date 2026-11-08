using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The ExecuteTransaction helpers on a connection that is closed (they open it, and must close it again) and on one that is open (it is left as it was):
// the transaction is disposed exactly once on every path, is committed only when the action succeeds and nothing was cancelled, is never rolled back
// explicitly (the dispose does it), and the connection is never disposed.
[ExcludeFromCodeCoverage]
public static class LeakTransactionTests
{
	public enum Call
	{
		Sync,
		Async,
	}

	public enum Outcome
	{
		Success,
		ActionThrows,
		CancelledBefore,
		CancelledInAction,
	}

	public static TheoryData<Call, LeakSource, Outcome> Cases { get; } = BuildCases();

	static TheoryData<Call, LeakSource, Outcome> BuildCases()
	{
		var data = new TheoryData<Call, LeakSource, Outcome>();
		foreach (Call call in Enum.GetValues<Call>())
		{
			foreach (LeakSource source in new[] { LeakSource.BareClosed, LeakSource.BareOpen })
			{
				foreach (Outcome outcome in Enum.GetValues<Outcome>())
					data.Add(call, source, outcome);
			}
		}

		return data;
	}

	[Theory]
	[MemberData(nameof(Cases))]
	public static async Task Transaction_IsDisposedOnEveryPath_AndTheConnectionIsLeftAsItsOwnerWantsIt(Call call, LeakSource source, Outcome outcome)
	{
		using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
		var tally = new DbTally();
		TrackedConnection connection = source == LeakSource.BareOpen ? tally.NewOpenConnection() : tally.NewConnection();
		if (outcome == Outcome.CancelledBefore) await cts.CancelAsync();

		void Act(DbTransaction _)
		{
			if (outcome == Outcome.ActionThrows) throw new InvalidOperationException("The action failed.");
			if (outcome == Outcome.CancelledInAction) cts.Cancel();
		}

		Exception? thrown = null;
		try
		{
			if (call == Call.Sync)
			{
				Action<DbTransaction> action = Act;
				connection.ExecuteTransaction(action, IsolationLevel.Unspecified, cts.Token);
			}
			else
			{
				ValueTask ActAsync(DbTransaction tx)
				{
					Act(tx);
					return ValueTask.CompletedTask;
				}

				Func<DbTransaction, ValueTask> action = ActAsync;
				await connection.ExecuteTransactionAsync(action, IsolationLevel.Unspecified, cts.Token);
			}
		}
		catch (Exception ex)
		{
			thrown = ex;
		}

		switch (outcome)
		{
			case Outcome.Success:
				Assert.Null(thrown);
				break;
			case Outcome.ActionThrows:
				Assert.IsType<InvalidOperationException>(thrown);
				break;
			default:
				Assert.IsType<OperationCanceledException>(thrown, exactMatch: false);
				break;
		}

		if (outcome == Outcome.CancelledBefore)
		{
			Assert.Empty(tally.Transactions);
		}
		else
		{
			TrackedTransaction tx = Assert.Single(tally.Transactions);
			Assert.Equal(1, tx.Disposes);
			Assert.Equal(outcome == Outcome.Success ? 1 : 0, tx.Commits);
			Assert.Equal(0, tx.Rollbacks);
		}

		Assert.Equal(0, connection.Disposes);
		if (source == LeakSource.BareClosed)
		{
			Assert.Equal(ConnectionState.Closed, connection.State);
			Assert.Equal(connection.Opens, connection.Closes);
		}
		else
		{
			Assert.Equal(ConnectionState.Open, connection.State);
			Assert.Equal(0, connection.Opens);
			Assert.Equal(0, connection.Closes);
		}
	}
}
