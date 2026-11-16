using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// R20: the names are resolved in every run now, so a repeat run of one
// command object with derived names must still cost what the same command
// with explicit names costs: nothing per run for the send list once warm
// (the list is kept per thread and reused). The explicit run is the
// no-derived path, which is measured in the same pass.
[Collection("Heap measurements")]
[ExcludeFromCodeCoverage]
public static class DerivedNamesEveryRunCostTests
{
	sealed class NamedCommand(
		DbConnection connection,
		string[] names)
		: ExpressiveDbCommand(
			connection,
			CommandType.StoredProcedure,
			ParameterNameHarness.NewProcedure())
	{
		int _lookups;

		public int Lookups => Volatile.Read(ref _lookups);

		protected override ImmutableArray<string> GetProcedureParameterNames(
			DbConnection connection,
			string procedure)
		{
			Interlocked.Increment(ref _lookups);
			return [.. names];
		}

		protected override ValueTask<ImmutableArray<string>>
			GetProcedureParameterNamesAsync(
				DbConnection connection,
				string procedure,
				CancellationToken cancellationToken)
			=> new(GetProcedureParameterNames(connection, procedure));
	}

	sealed class Args
	{
		public int OrderId { get; set; } = 5;
		public int CustomerId { get; set; } = 7;
		public DateTime StartDate { get; set; } = new(2026, 1, 2);
	}

	static readonly string[] Real = ["@order_id", "@customer_id", "@start_date"];

	sealed class Rig
	{
		readonly DerivedParameterNameTests.RecordingDbCommand _db = new();
		public DbConnection Connection { get; }

		public Rig()
			=> Connection = new DerivedParameterNameTests.OpenDbConnection(_db);

		// One command object, run again and again: the names are cached
		// after the first run.
		public Action Sync(ExpressiveDbCommand command)
			=> () =>
			{
				command.ExecuteNonQuery();
				_db.Sent.Clear();
			};

		[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
		public Action Async(ExpressiveDbCommand command)
			=> () =>
			{
				command.ExecuteNonQueryAsync().AsTask().GetAwaiter().GetResult();
				_db.Sent.Clear();
			};
	}

	static NamedCommand Derived(Rig rig)
	{
		var command = new NamedCommand(rig.Connection, Real);
		const int orderId = 5;
		const int customerId = 7;
		var startDate = new DateTime(2026, 1, 2);
		command
			.AddParam(orderId)
			.AddParam(customerId)
			.AddParam(startDate);
		return command;
	}

	static NamedCommand Explicit(Rig rig)
	{
		var command = new NamedCommand(rig.Connection, Real);
		command
			.AddParam("@order_id", 5)
			.AddParam("@customer_id", 7)
			.AddParam("@start_date", new DateTime(2026, 1, 2));
		return command;
	}

	[Fact]
	public static void ARepeatRun_WithDerivedNames_CostsWhatExplicitNamesCost()
	{
		var rig = new Rig();
		long derived = DerivedNameCostTests.BytesPerCall(rig.Sync(Derived(rig)));
		long named = DerivedNameCostTests.BytesPerCall(rig.Sync(Explicit(rig)));

		DerivedNameCostTests.AssertWithinBudget(
			"repeat run, three derived parameters",
			derived,
			named);
	}

	[Fact]
	public static void ARepeatRun_WithAnObject_CostsWhatExplicitNamesCost()
	{
		var rig = new Rig();
		var command = new NamedCommand(rig.Connection, Real);
		command.AddParams(new Args());

		long derived = DerivedNameCostTests.BytesPerCall(rig.Sync(command));
		long named = DerivedNameCostTests.BytesPerCall(rig.Sync(Explicit(rig)));

		DerivedNameCostTests.AssertWithinBudget(
			"repeat run, an object of three members",
			derived,
			named);
	}

	[Fact]
	public static void ARepeatRun_OnTheAsyncPath_CostsWhatExplicitNamesCost()
	{
		var rig = new Rig();
		NamedCommand command = Derived(rig);
		long derived = DerivedNameCostTests.BytesPerCall(rig.Async(command));
		long named = DerivedNameCostTests.BytesPerCall(rig.Async(Explicit(rig)));

		DerivedNameCostTests.AssertWithinBudget(
			"repeat run, async, three derived parameters",
			derived,
			named);

		// The names were read once, not in every run.
		Assert.Equal(1, command.Lookups);
	}
}
