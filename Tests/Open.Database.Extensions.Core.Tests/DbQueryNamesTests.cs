using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Open.Database.Extensions.Tests;

#nullable enable

// A name written by hand is the name the text declares: with or without its prefix, in any case, it is sent as the text writes it,
// so the provider has no name to build. A name the text does not declare is sent as written. A name is used once however it is
// spelled, whichever way the parameters are added, and a text with more names than the bookkeeping holds in one word behaves the same.
[ExcludeFromCodeCoverage]
public static class DbQueryNamesTests
{
	sealed record Echo(long A, long B);

	sealed class Tracked() : SqliteConnection("Data Source=:memory:")
	{
		public List<DbCommand> Commands { get; } = [];

		protected override DbCommand CreateDbCommand()
		{
			DbCommand command = base.CreateDbCommand();
			Commands.Add(command);
			return command;
		}

		public string[] Sent(int command)
			=> [.. Commands[command].Parameters.Cast<DbParameter>().Select(static p => p.ParameterName)];
	}

	static readonly DbQuery Q = DbQuery.Text("select @a as A, @b as B");

	static readonly MappedDbQuery<Echo> M = Q.Map<Echo>();

	[Theory]
	[InlineData("a", "@a")]
	[InlineData("@a", "@a")]
	[InlineData("A", "@a")]
	[InlineData("@A", "@a")]
	public static void AWrittenName_IsSentAsTheTextDeclaresIt(string written, string sent)
	{
		using var c = new Tracked();

		Echo echo = M.Using(c).AddParam(written, 3).AddParam("b", 4).FirstOrDefault()!;

		Assert.Equal(new Echo(3, 4), echo);
		Assert.Equal([sent, "@b"], c.Sent(0));
	}

	[Fact]
	public static void AWrittenName_TheTextDoesNotDeclare_IsSentAsWritten()
	{
		using var c = new Tracked();

		Echo echo = M.Using(c).AddParam("a", 3).AddParam("b", 4).AddParam("extra", 5).AddParam("@other", 6).FirstOrDefault()!;

		Assert.Equal(new Echo(3, 4), echo);
		Assert.Equal(["@a", "@b", "extra", "@other"], c.Sent(0));
	}

	[Fact]
	public static void TheSameNameSpelledTwoWays_IsRefused_WhateverWayItIsAdded()
	{
		using var c = new Tracked();
		int a = 1;

		Assert.Throws<InvalidOperationException>(() => M.Using(c).AddParam("a", 1).AddParam("@a", 2).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => M.Using(c).AddParam("@A", 1).AddParam("a", 2).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => M.Using(c).AddParam("a", 1).AddParam(a).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => M.AddParam("a", 1).AddParam("@a", 2).Using(c).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => M.Using(c).AddParam("extra", 1).AddParam("extra", 2).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => M.Using(c).AddParam("@extra", 1).AddParam("extra", 2).FirstOrDefault());
	}

	[Fact]
	public static void AWrittenName_WinsOverAMemberOfTheSameName()
	{
		using var c = new Tracked();

		// The member a written name claims is left to it, whichever was added first.
		Assert.Equal(new Echo(1, 3), M.Using(c).AddParam("a", 1).AddParams(new { a = 2, b = 3 }).FirstOrDefault());
		Echo echo = M.Using(c).AddParams(new { a = 2, b = 3 }).AddParam("@a", 7).FirstOrDefault()!;
		Assert.Equal(new Echo(7, 3), echo);
	}

	[Fact]
	public static void ANullValue_IsSentWithTheTypeOfItsArgument_UnderTheDeclaredName()
	{
		using var c = new Tracked();
		MappedDbQuery<long?> nullable = DbQuery.Text("select @a").Map<long?>();

		Assert.Null(nullable.Using(c).AddParam("a", (int?)null).FirstOrDefault());

		Assert.Equal(["@a"], c.Sent(0));
		Assert.Equal(System.Data.DbType.Int32, c.Commands[0].Parameters[0].DbType);
	}

	// More names than one word of bookkeeping holds: the names past the first sixty-four are told apart by looking at what the command has.
	static string ManyNamesSql(int count)
		=> "select " + string.Join(" + ", Enumerable.Range(0, count).Select(static i => "@n" + i));

	[Fact]
	public static void ATextWithManyNames_BindsEachOnce_AndRefusesARepeat()
	{
		const int Count = 80;
		MappedDbQuery<long> many = DbQuery.Text(ManyNamesSql(Count)).Map<long>();
		using var c = new Tracked();

		MappedQuery<long> all = many.Using(c);
		for (int i = 0; i < Count; i++) all = all.AddParam(i % 2 == 0 ? "n" + i : "@n" + i, 1);

		Assert.Equal(Count, all.FirstOrDefault());
		Assert.Equal(Enumerable.Range(0, Count).Select(static i => "@n" + i), c.Sent(0));

		// A repeat among the first sixty-four, and past them.
		Assert.Throws<InvalidOperationException>(() => many.Using(c).AddParam("n3", 1).AddParam("@N3", 1).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => many.Using(c).AddParam("n70", 1).AddParam("@N70", 1).FirstOrDefault());
		Assert.Throws<InvalidOperationException>(() => many.Using(c).AddParam("@n70", 1).AddParams(new { n70 = 2 }).FirstOrDefault());
	}
}
