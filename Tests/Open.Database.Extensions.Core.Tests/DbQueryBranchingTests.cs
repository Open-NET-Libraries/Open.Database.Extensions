using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// A call keeps its values in one shared array and every call made by adding a value sees only the values it was given.
// What it pins: two calls made from one kept call each carry exactly their own values, run one after the other and at the
// same moment (the second writer of an entry copies instead of writing over the first); a call that has run can be run again,
// added to and branched; the same holds when the values are added before the source is given and when they are the members of
// an object; and an array that is full grows without changing any call that sees part of it.
[ExcludeFromCodeCoverage]
public static class DbQueryBranchingTests
{
	sealed record Echo(int A, int B, int C);

	const string EchoSql = "select @a as A, @b as B, @c as C";

	static readonly DbQuery Q = DbQuery.Text(EchoSql);

	static readonly MappedDbQuery<Echo> M = Q.Map<Echo>();

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A connection that remembers the commands made on it, to read the parameters each one was sent with.
	sealed class Tracked() : SqliteConnection("Data Source=:memory:")
	{
		public List<DbCommand> Commands { get; } = [];

		protected override DbCommand CreateDbCommand()
		{
			DbCommand command = base.CreateDbCommand();
			Commands.Add(command);
			return command;
		}

		public string[] SentTo(int command)
			=> [.. Commands[command].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName + "=" + p.Value)];
	}

	// Each run makes its own in-memory connection, so a call can be run by any number of threads at once.
	sealed class Factory : IDbConnectionFactory<SqliteConnection>
	{
		public SqliteConnection Create() => new("Data Source=:memory:");

		IDbConnection IDbConnectionFactory.Create() => Create();
	}

	static readonly Factory Source = new();

	static Echo One(IExecuteReader call)
		=> Assert.Single(call.Map<Echo>().ToList());

	static Echo One(MappedQuery<Echo> query)
		=> Assert.Single(query.ToList());

	[Fact]
	public static void TwoBranchesOfAKeptCall_EachSendOnlyTheirOwnValues()
	{
		using var c = new Tracked();
		DbQueryCall held = Q.Using(c).AddParam("a", 1);

		DbQueryCall x = held.AddParam("b", 10);
		DbQueryCall y = held.AddParam("b", 20);
		DbQueryCall z = held.AddParam("b", 30).AddParam("c", 300);
		DbQueryCall x2 = x.AddParam("c", 100);
		DbQueryCall y2 = y.AddParam("c", 200);

		// Run in an order other than the one they were made in.
		Assert.Equal(new Echo(1, 20, 200), One(y2));
		Assert.Equal(new Echo(1, 10, 100), One(x2));
		Assert.Equal(new Echo(1, 30, 300), One(z));
		Assert.Equal(new Echo(1, 10, 100), One(x2));

		Assert.Equal(["@a=1", "@b=20", "@c=200"], c.SentTo(0));
		Assert.Equal(["@a=1", "@b=10", "@c=100"], c.SentTo(1));
		Assert.Equal(["@a=1", "@b=30", "@c=300"], c.SentTo(2));
		Assert.Equal(["@a=1", "@b=10", "@c=100"], c.SentTo(3));

		// The kept call still has its one value: the provider reports the others as missing.
		Assert.Throws<InvalidOperationException>(() => One(held));
		Assert.Equal(["@a=1"], c.SentTo(4));
	}

	[Fact]
	public static void ACallThatHasRun_CanBeRunAgain_AddedToAndBranched()
	{
		using var c = new Tracked();
		DbQueryCall full = Q.Using(c).AddParam("a", 1).AddParam("b", 2).AddParam("c", 3);

		Assert.Equal(new Echo(1, 2, 3), One(full));
		Assert.Equal(new Echo(1, 2, 3), One(full));

		// A complete call is added to: the extra value is sent, and the call it came from is unchanged.
		DbQueryCall extra = full.AddParam("d", 4);
		Assert.Equal(new Echo(1, 2, 3), One(extra));
		Assert.Equal(new Echo(1, 2, 3), One(full));
		Assert.Equal(["@a=1", "@b=2", "@c=3"], c.SentTo(3));
		Assert.Equal(["@a=1", "@b=2", "@c=3", "d=4"], c.SentTo(2));

		// A branch of the first value, made after the later ones ran.
		DbQueryCall first = Q.Using(c).AddParam("a", 9);
		DbQueryCall late = first.AddParam("b", 8).AddParam("c", 7);
		Assert.Equal(new Echo(9, 8, 7), One(late));
		DbQueryCall other = first.AddParam("b", 6).AddParam("c", 5);
		Assert.Equal(new Echo(9, 6, 5), One(other));
		Assert.Equal(new Echo(9, 8, 7), One(late));
	}

	[Fact]
	public static void ABranchedCall_EqualsItsTwin_AndNotItsSibling()
	{
		using var c = new Tracked();
		DbQueryCall held = Q.Using(c).AddParam("a", 1);
		DbQueryCall x = held.AddParam("b", 10);
		DbQueryCall y = held.AddParam("b", 20);
		DbQueryCall twinOfX = Q.Using(c).AddParam("a", 1).AddParam("b", 10);

		Assert.NotEqual(x, y);
		Assert.Equal(x, twinOfX);
		Assert.Equal(x.GetHashCode(), twinOfX.GetHashCode());
		Assert.NotEqual(held, x);
	}

	[Fact]
	public static void ValuesAddedBeforeTheSource_BranchTheSameWay()
	{
		UnboundCall<Echo> held = M.AddParam("a", 1);
		UnboundCall<Echo> x = held.AddParam("b", 10);
		UnboundCall<Echo> y = held.AddParam("b", 20);

		using var c1 = new Tracked();
		using var c2 = new Tracked();
		Assert.Equal(new Echo(1, 20, 2), One(y.AddParam("c", 2).Using(c1)));
		Assert.Equal(new Echo(1, 10, 3), One(x.AddParam("c", 3).Using(c2)));
		Assert.Equal(["@a=1", "@b=20", "@c=2"], c1.SentTo(0));
		Assert.Equal(["@a=1", "@b=10", "@c=3"], c2.SentTo(0));

		// The source is given to the same values twice.
		using var c3 = new Tracked();
		UnboundCall<Echo> full = x.AddParam("c", 4);
		Assert.Equal(new Echo(1, 10, 4), One(full.Using(c3)));
		Assert.Equal(new Echo(1, 10, 4), One(full.Using(Source)));
		Assert.Equal(["@a=1", "@b=10", "@c=4"], c3.SentTo(0));
	}

	[Fact]
	public static void MembersOfAnObject_BranchWithTheValuesAddedAfterThem()
	{
		using var c = new Tracked();
		DbQueryCall members = Q.Using(c).AddParams(new { a = 1 });
		DbQueryCall x = members.AddParam("b", 2).AddParam("c", 3);
		DbQueryCall y = members.AddParam("b", 5).AddParam("c", 6);
		DbQueryCall before = Q.Using(c).AddParam("a", 7).AddParams(new { b = 8, c = 9 });

		Assert.Equal(new Echo(1, 5, 6), One(y));
		Assert.Equal(new Echo(1, 2, 3), One(x));
		Assert.Equal(new Echo(7, 8, 9), One(before));
		// A name written by hand, with or without its prefix, and a member of an object are sent under the name the text declares.
		Assert.Equal(["@a=1", "@b=5", "@c=6"], c.SentTo(0));
		Assert.Equal(["@a=1", "@b=2", "@c=3"], c.SentTo(1));
		Assert.Equal(["@a=7", "@b=8", "@c=9"], c.SentTo(2));
		Assert.Throws<InvalidOperationException>(() => One(members));
		Assert.Equal(["@a=1"], c.SentTo(3));
	}

	// The same chain made again, and a second member on top of the first, are the same call.
	[Fact]
	public static void AnArrayThatIsFull_GrowsWithoutChangingTheCallsThatSeePartOfIt()
	{
		// A procedure declares no names, so the first array holds four values.
		DbQuery procedure = DbQuery.Procedure("Echo");
		using var c = new SqliteConnection();

		DbQueryCall Chain(int count)
		{
			DbQueryCall call = procedure.Using(c).AddParam("p0", 0);
			for (int i = 1; i < count; i++) call = call.AddParam("p" + i, i);
			return call;
		}

		DbQueryCall four = Chain(4);
		DbQueryCall six = four.AddParam("p4", 4).AddParam("p5", 5);
		DbQueryCall tenA = six.AddParam("p6", 6).AddParam("p7", 7).AddParam("p8", 8).AddParam("p9", 9);
		DbQueryCall tenB = six.AddParam("p6", 6).AddParam("p7", 7).AddParam("p8", 8).AddParam("p9", 9);
		DbQueryCall seven = six.AddParam("x", 1);
		DbQueryCall otherSeven = six.AddParam("y", 2);

		Assert.Equal(Chain(4), four);
		Assert.Equal(Chain(6), six);
		Assert.Equal(Chain(10), tenA);
		Assert.Equal(tenA, tenB);
		Assert.NotEqual(seven, otherSeven);
		Assert.NotEqual(four, six);
		Assert.NotEqual(six, tenA);

		// A kept call stays what it was after every branch off it.
		Assert.Equal(Chain(6), six);
		Assert.Equal(Chain(4), four);
	}

	[Fact]
	public static void TheSizeOfTheArray_FollowsTheNamesTheDefinitionDeclares()
	{
		Assert.Equal(3, DbQuery.Text(EchoSql).ParamCapacity);
		Assert.Equal(1, DbQuery.Text("select @only").ParamCapacity);
		Assert.Equal(4, DbQuery.Text("select 1").ParamCapacity);

		// A procedure declares none: it keeps the most a call has bound, so the next call is sized to fit.
		DbQuery procedure = DbQuery.Procedure("Echo");
		Assert.Equal(4, procedure.ParamCapacity);
		procedure.NoteParams(7);
		Assert.Equal(7, procedure.ParamCapacity);
		procedure.NoteParams(2);
		Assert.Equal(7, procedure.ParamCapacity);
	}

	const int Threads = 16;

	const int Rounds = 150;

	// Every thread adds its own value to the same kept call at the same moment, so they all reach for the same entry:
	// one writes it in place and the rest copy. Each then runs what it made, and must read its own values back.
	[Fact]
	public static void ManyThreadsBranchingOneKeptCall_EachSeeOnlyTheirOwnValues()
	{
		var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();

		for (int round = 0; round < Rounds; round++)
		{
			DbQueryCall held = Q.Using(Source).AddParam("a", 1);
			using var gate = new Barrier(Threads);
			Thread[] threads = new Thread[Threads];
			for (int t = 0; t < Threads; t++)
			{
				int mine = t;
				threads[t] = new Thread(() =>
				{
					try
					{
						gate.SignalAndWait();
						DbQueryCall b = held.AddParam("b", mine);
						gate.SignalAndWait();
						DbQueryCall full = b.AddParam("c", mine * 10);
						Echo echo = One(full);
						if (echo != new Echo(1, mine, mine * 10))
							failures.Enqueue($"thread {mine}: {echo}");

						// Run again, and run the kept call, while the others still branch off it.
						if (One(full) != echo) failures.Enqueue($"thread {mine}: second run differed");
						Assert.Throws<InvalidOperationException>(() => One(held));
					}
					catch (Exception ex)
					{
						failures.Enqueue($"thread {mine}: {ex.GetType().Name}: {ex.Message}");
					}
				});
				threads[t].Start();
			}

			foreach (Thread thread in threads) thread.Join();
			Assert.Empty(failures);
		}
	}

	// The same race with the values added before the source is given.
	[Fact]
	public static void ManyThreadsBranchingValuesAddedBeforeTheSource_EachSeeOnlyTheirOwnValues()
	{
		var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();

		for (int round = 0; round < Rounds; round++)
		{
			UnboundCall<Echo> held = M.AddParam("a", 1);
			using var gate = new Barrier(Threads);
			Thread[] threads = new Thread[Threads];
			for (int t = 0; t < Threads; t++)
			{
				int mine = t;
				threads[t] = new Thread(() =>
				{
					try
					{
						gate.SignalAndWait();
						UnboundCall<Echo> b = held.AddParam("b", mine);
						gate.SignalAndWait();
						Echo echo = One(b.AddParam("c", mine * 10).Using(Source));
						if (echo != new Echo(1, mine, mine * 10))
							failures.Enqueue($"thread {mine}: {echo}");
					}
					catch (Exception ex)
					{
						failures.Enqueue($"thread {mine}: {ex.GetType().Name}: {ex.Message}");
					}
				});
				threads[t].Start();
			}

			foreach (Thread thread in threads) thread.Join();
			Assert.Empty(failures);
		}
	}

	// The members of an object are the first entry of a call that gets an array when a value follows them: many threads do that
	// to one kept call at once.
	[Fact]
	public static void ManyThreadsAddingAfterTheMembersOfAnObject_EachSeeOnlyTheirOwnValues()
	{
		var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();

		for (int round = 0; round < Rounds; round++)
		{
			DbQueryCall held = Q.Using(Source).AddParams(new { a = 1 });
			using var gate = new Barrier(Threads);
			Thread[] threads = new Thread[Threads];
			for (int t = 0; t < Threads; t++)
			{
				int mine = t;
				threads[t] = new Thread(() =>
				{
					try
					{
						gate.SignalAndWait();
						DbQueryCall b = held.AddParam("b", mine).AddParam("c", mine * 10);
						Echo echo = One(b);
						if (echo != new Echo(1, mine, mine * 10))
							failures.Enqueue($"thread {mine}: {echo}");
					}
					catch (Exception ex)
					{
						failures.Enqueue($"thread {mine}: {ex.GetType().Name}: {ex.Message}");
					}
				});
				threads[t].Start();
			}

			foreach (Thread thread in threads) thread.Join();
			Assert.Empty(failures);
		}
	}

	// Many threads run one complete call at the same moment: running never writes the array.
	[Fact]
	public static void ManyThreadsRunningOneCompleteCall_AllReadTheSameValues()
	{
		DbQueryCall full = Q.Using(Source).AddParam("a", 1).AddParam("b", 2).AddParam("c", 3);
		var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
		using var gate = new Barrier(Threads);
		Thread[] threads = new Thread[Threads];
		for (int t = 0; t < Threads; t++)
		{
			threads[t] = new Thread(() =>
			{
				try
				{
					gate.SignalAndWait();
					for (int i = 0; i < Rounds; i++)
					{
						Echo echo = One(full);
						if (echo != new Echo(1, 2, 3)) failures.Enqueue(echo.ToString());
					}
				}
				catch (Exception ex)
				{
					failures.Enqueue($"{ex.GetType().Name}: {ex.Message}");
				}
			});
			threads[t].Start();
		}

		foreach (Thread thread in threads) thread.Join();
		Assert.Empty(failures);
	}
}
