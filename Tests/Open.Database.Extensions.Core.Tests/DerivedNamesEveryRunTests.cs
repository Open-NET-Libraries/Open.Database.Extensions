using System.Collections.Concurrent;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

// R20: the names of AddParam(value) and AddParams(object) parameters are
// worked out in every run, against the connection that run uses, and the
// caller's Params are never rewritten. One command object then serves two
// databases whose procedure of the same name takes different parameter names.
[ExcludeFromCodeCoverage]
public static class DerivedNamesEveryRunTests
{
	static CancellationToken Token => TestContext.Current.CancellationToken;

	// ---- the fakes ----

	// What one run was given: the database to connect to, and what was
	// created for it.
	sealed class Slot(string database)
	{
		public string Database { get; } = database;
		public DerivedParameterNameTests.RecordingDbCommand? Command { get; set; }
		public int Disposals;
	}

	sealed class DatabaseConnection(string database, DbCommand command)
		: DbConnection
	{
		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => database;
		public override string DataSource => "fake";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() { }
		public override void Close() { }

		protected override DbTransaction BeginDbTransaction(
			IsolationLevel isolationLevel)
			=> throw new NotSupportedException();

		protected override DbCommand CreateDbCommand() => command;
	}

	// Connections come from here, to the database the running flow chose:
	// the choice travels with the flow, so it also holds across an await.
	sealed class Rig
	{
		readonly AsyncLocal<Slot?> _current = new();

		public Slot Use(string database)
		{
			var slot = new Slot(database);
			_current.Value = slot;
			return slot;
		}

		public DatabaseConnection Create()
		{
			Slot slot = _current.Value!;
			var command = new DerivedParameterNameTests.RecordingDbCommand();
			command.Disposed += (_, _)
				=> Interlocked.Increment(ref slot.Disposals);
			slot.Command = command;
			return new DatabaseConnection(slot.Database, command);
		}
	}

	// The same procedure on every database, declaring what that database says.
	sealed class DeclaredCommand(
		Rig rig,
		Dictionary<string, string[]> declared)
		: ExpressiveDbCommand(
			rig.Create,
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
			return [.. declared[connection.Database]];
		}

		protected override ValueTask<ImmutableArray<string>>
			GetProcedureParameterNamesAsync(
				DbConnection connection,
				string procedure,
				CancellationToken cancellationToken)
			=> new(GetProcedureParameterNames(connection, procedure));
	}

	// A provider's command that reads Params in its own hook, as the
	// library's four do.
	sealed class HookCommand(
		Rig rig,
		Dictionary<string, string[]> declared)
		: ExpressiveDbCommand(
			rig.Create,
			CommandType.StoredProcedure,
			ParameterNameHarness.NewProcedure())
	{

		// What Params held inside the hook.
		public string[]? Seen { get; private set; }
		public bool SawTheCallersList { get; private set; }
		public List<ExpressiveDbCommand.Param>? Callers { get; set; }

		// Run before the hook reads Params (a second command to execute,
		// or a failure).
		public Action? Before { get; set; }

		protected override ImmutableArray<string> GetProcedureParameterNames(
			DbConnection connection,
			string procedure)
			=> [.. declared[connection.Database]];

		protected override void AddParams(DbCommand command)
		{
			Before?.Invoke();
			Seen = [.. Params.Select(static p => p.Name)];
			SawTheCallersList = ReferenceEquals(Params, Callers);
			base.AddParams(command);
		}
	}

	sealed class Args
	{
		public int OrderId { get; set; } = 5;
		public string Region { get; set; } = "west";
	}

	// The same procedure, as two databases declare it.
	static Dictionary<string, string[]> Twice()
		=> new()
		{
			["a"] = ["@order_id", "@customer_id"],
			["b"] = ["@p_order_id", "@p_customer_id"],
		};

	static string[] Sent(Slot slot)
		=> [.. slot.Command!.Sent.Select(static p => p.ParameterName)];

	static string[] Added(ExpressiveDbCommand command)
		=> [.. command.Params.Select(static p => p.Name)];

	// ---- the defect ----

	[Fact]
	public static void OneCommand_TwoDatabases_EachRunSendsItsOwnNames()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);

		Slot first = rig.Use("a");
		command.ExecuteNonQuery();
		Slot second = rig.Use("b");
		command.ExecuteNonQuery();
		Slot third = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Equal(["@order_id", "@customer_id"], Sent(first));
		Assert.Equal(["@p_order_id", "@p_customer_id"], Sent(second));
		Assert.Equal(["@order_id", "@customer_id"], Sent(third));

		// One read per database: the memory is right, only the rewrite was not.
		Assert.Equal(2, command.Lookups);
	}

	[Fact]
	public static async Task OneCommand_TwoDatabases_EachRunSendsItsOwnNamesAsync()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);

		Slot first = rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);
		Slot second = rig.Use("b");
		await command.ExecuteNonQueryAsync(Token);
		Slot third = rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);

		Assert.Equal(["@order_id", "@customer_id"], Sent(first));
		Assert.Equal(["@p_order_id", "@p_customer_id"], Sent(second));
		Assert.Equal(["@order_id", "@customer_id"], Sent(third));
	}

	[Fact]
	public static void ADroppedMember_IsSentOnADatabaseThatTakesIt()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(
			rig,
			new()
			{
				["a"] = ["@order_id"],
				["b"] = ["@order_id", "@region"],
			});
		command.AddParams(new Args());

		Slot first = rig.Use("a");
		command.ExecuteNonQuery();
		Slot second = rig.Use("b");
		command.ExecuteNonQuery();
		Slot third = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Equal(["@order_id"], Sent(first));
		Assert.Equal(["@order_id", "@region"], Sent(second));
		Assert.Equal(["@order_id"], Sent(third));
		Assert.Equal(["OrderId", "Region"], Added(command));
	}

	[Fact]
	public static async Task ADroppedMember_IsSentOnADatabaseThatTakesIt_Async()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(
			rig,
			new()
			{
				["a"] = ["@order_id"],
				["b"] = ["@order_id", "@region"],
			});
		command.AddParams(new Args());

		Slot first = rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);
		Slot second = rig.Use("b");
		await command.ExecuteNonQueryAsync(Token);

		Assert.Equal(["@order_id"], Sent(first));
		Assert.Equal(["@order_id", "@region"], Sent(second));
	}

	// ---- Params is what the caller added ----

	[Fact]
	public static void Params_AreUnchangedByARun()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);
		List<ExpressiveDbCommand.Param> list = command.Params;

		rig.Use("b");
		command.ExecuteNonQuery();
		rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Same(list, command.Params);
		Assert.Equal(["orderId", "customerId"], Added(command));
		Assert.Equal([5, 7], command.Params.Select(static p => p.Value));
	}

	[Fact]
	public static async Task Params_AreUnchangedByARun_Async()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);
		List<ExpressiveDbCommand.Param> list = command.Params;

		rig.Use("b");
		await command.ExecuteNonQueryAsync(Token);
		rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);

		Assert.Same(list, command.Params);
		Assert.Equal(["orderId", "customerId"], Added(command));
	}

	// ---- the same command on several threads at once ----

	[Fact]
	public static void OneCommand_ManyThreads_EveryRunSendsItsOwnNames()
	{
		const int threads = 8;
		const int commands = 40;
		const int rounds = 6;
		var failures = new ConcurrentQueue<string>();

		for (int c = 0; c < commands; c++)
		{
			var rig = new Rig();
			var command = new DeclaredCommand(rig, Twice());
			int orderId = 5;
			int customerId = 7;
			command
				.AddParam(orderId)
				.AddParam(customerId);
			var barrier = new Barrier(threads);

			// Own threads: a barrier of this size would starve the pool.
			Thread[] all = [.. Enumerable.Range(0, threads).Select(t =>
				new Thread(() =>
				{
					string database = t % 2 == 0 ? "a" : "b";
					string[] expected = database == "a"
						? ["@order_id", "@customer_id"]
						: ["@p_order_id", "@p_customer_id"];
					for (int r = 0; r < rounds; r++)
					{
						barrier.SignalAndWait(TimeSpan.FromSeconds(30));
						try
						{
							Slot slot = rig.Use(database);
							command.ExecuteNonQuery();
							string[] sent = Sent(slot);
							if (!expected.SequenceEqual(sent))
							{
								failures.Enqueue(
									$"{database}: {string.Join(",", sent)}");
							}
						}
						catch (Exception ex)
						{
							failures.Enqueue($"{database}: {ex.Message}");
						}
					}
				}))];

			foreach (Thread thread in all) thread.Start();
			foreach (Thread thread in all) thread.Join();
			barrier.Dispose();
		}

		Assert.Empty(failures);
	}

	[Fact]
	public static async Task OneCommand_ManyTasks_EveryRunSendsItsOwnNames()
	{
		const int tasks = 8;
		const int rounds = 200;
		var failures = new ConcurrentQueue<string>();
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);

		await Task.WhenAll(Enumerable.Range(0, tasks).Select(t => Task.Run(
			async () =>
			{
				string database = t % 2 == 0 ? "a" : "b";
				string[] expected = database == "a"
					? ["@order_id", "@customer_id"]
					: ["@p_order_id", "@p_customer_id"];
				for (int r = 0; r < rounds; r++)
				{
					Slot slot = rig.Use(database);
					await command.ExecuteNonQueryAsync(Token);
					string[] sent = Sent(slot);
					if (!expected.SequenceEqual(sent))
						failures.Enqueue($"{database}: {string.Join(",", sent)}");
				}
			},
			Token)));

		Assert.Empty(failures);
	}

	// ---- a subclass's hook reads Params ----

	[Fact]
	public static void AHook_SeesTheSendList_AndParamsOutsideIsTheCallers()
	{
		var rig = new Rig();
		var command = new HookCommand(
			rig,
			new()
			{
				["a"] = ["@order_id"],
				["b"] = ["@order_id", "@region"],
			});
		command.AddParams(new Args());
		command.Callers = command.Params;

		rig.Use("a");
		command.ExecuteNonQuery();

		// The dropped member is absent inside the hook, with the resolved names.
		Assert.Equal(["@order_id"], command.Seen!);
		Assert.False(command.SawTheCallersList);

		// Outside the hook it is what was added.
		Assert.Same(command.Callers, command.Params);
		Assert.Equal(["OrderId", "Region"], Added(command));

		rig.Use("b");
		command.ExecuteNonQuery();
		Assert.Equal(["@order_id", "@region"], command.Seen!);
	}

	[Fact]
	public static void AHook_WithOnlyExplicitNames_SeesTheCallersList()
	{
		var rig = new Rig();
		var command = new HookCommand(rig, Twice());
		command.AddParam("@order_id", 5);
		command.Callers = command.Params;

		Slot slot = rig.Use("a");
		command.ExecuteNonQuery();

		// Nothing is derived, so there is no send list at all.
		Assert.True(command.SawTheCallersList);
		Assert.Equal(["@order_id"], Sent(slot));
	}

	static DeclaredCommand InnerCommand(Rig rig)
	{
		var inner = new DeclaredCommand(rig, Twice());
		int orderId = 50;
		int customerId = 70;
		inner
			.AddParam(orderId)
			.AddParam(customerId);
		return inner;
	}

	[Fact]
	public static void AHookThatRunsAnotherCommand_DoesNotBreakTheOuterRun()
	{
		var outerRig = new Rig();
		var innerRig = new Rig();
		DeclaredCommand inner = InnerCommand(innerRig);

		var outer = new HookCommand(outerRig, Twice());
		int orderId = 5;
		int customerId = 7;
		outer
			.AddParam(orderId)
			.AddParam(customerId);
		outer.Callers = outer.Params;

		Slot innerSlot = null!;
		outer.Before = () =>
		{
			innerSlot = innerRig.Use("b");
			inner.ExecuteNonQuery();
		};

		Slot outerSlot = outerRig.Use("a");
		outer.ExecuteNonQuery();

		Assert.Equal(["@p_order_id", "@p_customer_id"], Sent(innerSlot));
		Assert.Equal(["@order_id", "@customer_id"], Sent(outerSlot));
		Assert.Equal(["@order_id", "@customer_id"], outer.Seen!);
		Assert.Equal(
			[5, 7],
			outerSlot.Command!.Sent.Select(static p => p.Value));
		Assert.Same(outer.Callers, outer.Params);
		Assert.Equal(["orderId", "customerId"], Added(outer));
	}

	// ---- edge cases ----

	[Fact]
	public static void NoParameters_RunsWithNoLookup()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());

		Slot slot = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Empty(Sent(slot));
		Assert.Empty(command.Params);
		Assert.Equal(0, command.Lookups);
	}

	[Fact]
	public static void ACallerNamedParameter_BeatsAnObjectMember_OnEveryRun()
	{
		var rig = new Rig();
		var command = new DeclaredCommand(rig, Twice());
		command
			.AddParams(new { OrderId = 1 })
			.AddParam("@order_id", 9);

		Slot first = rig.Use("a");
		command.ExecuteNonQuery();
		Slot second = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Equal(["@order_id"], Sent(first));
		Assert.Equal([9], first.Command!.Sent.Select(static p => p.Value));
		Assert.Equal(["@order_id"], Sent(second));
		Assert.Equal([9], second.Command!.Sent.Select(static p => p.Value));
		Assert.Equal(2, command.Params.Count);
	}

	[Fact]
	public static void ACollision_Throws_ThenTheNextRunIsFine()
	{
		var rig = new Rig();
		var command = new HookCommand(rig, Twice());
		int orderId = 5;
		var other = new Args();
		command
			.AddParam(orderId)
			.AddParam(other.OrderId);
		command.Callers = command.Params;

		rig.Use("a");
		var ex = Assert.Throws<InvalidOperationException>(
			() => command.ExecuteNonQuery());
		Assert.Contains("Two parameters resolve to", ex.Message);

		// Nothing is left behind: Params is still the caller's, as added.
		Assert.Same(command.Callers, command.Params);
		Assert.Equal(["orderId", "OrderId"], Added(command));

		command.Params.RemoveAt(1);
		Slot slot = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Equal(["@order_id"], Sent(slot));
		Assert.Equal(["@order_id"], command.Seen!);
	}

	[Fact]
	public static void StaleNames_AreReadAgain_AndTheRunSendsTheNewOnes()
	{
		var rig = new Rig();
		var declared = new Dictionary<string, string[]>
		{
			["a"] = ["@order_id"],
		};
		var command = new DeclaredCommand(rig, declared);
		int orderId = 5;
		command.AddParam(orderId);

		Slot first = rig.Use("a");
		command.ExecuteNonQuery();
		Assert.Equal(["@order_id"], Sent(first));
		Assert.Equal(1, command.Lookups);

		// The procedure changed: it now takes @customer_id as well.
		declared["a"] = ["@order_id", "@customer_id"];
		int customerId = 7;
		command.AddParam(customerId);

		Slot second = rig.Use("a");
		command.ExecuteNonQuery();

		Assert.Equal(["@order_id", "@customer_id"], Sent(second));
		Assert.Equal(2, command.Lookups);
		Assert.Equal(["orderId", "customerId"], Added(command));
	}

	[Fact]
	public static async Task StaleNames_AreReadAgain_Async()
	{
		var rig = new Rig();
		var declared = new Dictionary<string, string[]>
		{
			["a"] = ["@order_id"],
		};
		var command = new DeclaredCommand(rig, declared);
		int orderId = 5;
		command.AddParam(orderId);

		rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);

		declared["a"] = ["@order_id", "@customer_id"];
		int customerId = 7;
		command.AddParam(customerId);

		Slot second = rig.Use("a");
		await command.ExecuteNonQueryAsync(Token);

		Assert.Equal(["@order_id", "@customer_id"], Sent(second));
		Assert.Equal(2, command.Lookups);
	}

	[Fact]
	public static void AThrowWhileSettingUp_DisposesTheCommand_AndResetsTheState()
	{
		var rig = new Rig();
		var command = new HookCommand(rig, Twice());
		int orderId = 5;
		int customerId = 7;
		command
			.AddParam(orderId)
			.AddParam(customerId);
		command.Callers = command.Params;

		bool fail = true;
		command.Before = () =>
		{
			if (fail) throw new InvalidOperationException("setup failed");
		};

		Slot failed = rig.Use("a");
		Assert.Throws<InvalidOperationException>(
			() => command.ExecuteNonQuery());
		Assert.Equal(1, failed.Disposals);
		Assert.Same(command.Callers, command.Params);

		fail = false;
		Slot next = rig.Use("b");
		command.ExecuteNonQuery();

		Assert.Equal(["@p_order_id", "@p_customer_id"], Sent(next));
		Assert.Same(command.Callers, command.Params);
		Assert.Equal(["orderId", "customerId"], Added(command));
	}
}
