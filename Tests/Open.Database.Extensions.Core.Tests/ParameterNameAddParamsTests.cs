using System.Reflection;

namespace Open.Database.Extensions.Tests;

#nullable enable

// AddParams(object) takes the members of the STATIC type of its argument, reads every public getter when it is called, and sends only the members the procedure declares.
// Beside them: what a caller can see of the names that were sent, a few typed NULLs, and the helper rows (candidates, match) that no call site can pin one by one.
[ExcludeFromCodeCoverage]
public static class ParameterNameAddParamsTests
{
	interface IHasRegion
	{
		string Region { get; }
	}

	sealed class WithRegion : IHasRegion
	{
		public string Region { get; } = "west";
		public string Secret = "not in the interface";
	}

	class Base
	{
		public int Id { get; set; } = 1;
		public int Inherited = 5;
	}

	[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Public instance members on purpose: AddParams reads the instance members of a type through reflection.")]
	sealed class Hiding : Base
	{
		public new string Id { get; set; } = "derived";
		public int WriteOnly { set => _ = value; }
		public int this[int index] => index;
	}

	[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Public instance members on purpose: AddParams reads the instance members of a type through reflection.")]
	sealed class LazyMember
	{
		public int Id => 1;
		public int Navigation => throw new InvalidOperationException("lazy");
	}

	readonly struct Point
	{
		public int X { get; init; }
	}

	sealed class NoPublicMembers
	{
		internal int Hidden { get; } = 1;
	}

	static SyncNameRun TextCommand() => new(CommandType.Text, "@never");

	// ---- the static type counts, except for object: then the members are those of the runtime type ----

	[Fact]
	public static void AnObjectVariable_SendsTheMembersOfItsRuntimeType()
	{
		object boxed = new { Region = "west", Secret = "x" };

		Assert.Equal(["Region", "Secret"], TextCommand().Run(c => c.AddParams(boxed)));

		var procedure = new SyncNameRun("@region");
		Assert.Equal(["@region"], procedure.Run(c => c.AddParams(boxed))); // matched like the members of any other type: the undeclared one is left out
		Assert.Equal("west", procedure.Values["@region"]);
		Assert.Equal(1, procedure.Lookups);
	}

	[Fact]
	public static void TheResultOfAFuncOfObject_SendsItsMembers_ReadAtEveryCall()
	{
		static object Make(int id) => new { Id = id, Name = "n" + id };
		var run = TextCommand();

		Assert.Equal(["Id", "Name"], run.Run(c => c.AddParams(Make(3))));
		Assert.Equal(3, run.Values["Id"]);
		Assert.Equal("n3", run.Values["Name"]);

		Assert.Equal(["Id", "Name"], run.Run(c => c.AddParams(Make(4)))); // the members are found once per type, the values are read every time
		Assert.Equal(4, run.Values["Id"]);
		Assert.Equal("n4", run.Values["Name"]);
	}

	[Fact]
	public static void DifferentRuntimeTypesInARow_EachSendItsOwnMembers()
	{
		object[] sources = [new { A = 1 }, new { B = "b", C = 2.5 }, new { A = 3 }, new WithRegion(), new Hiding()];
		string[][] expected = [["A"], ["B", "C"], ["A"], ["Region", "Secret"], ["Id", "Inherited"]];

		for (int i = 0; i < sources.Length; i++)
			Assert.Equal(expected[i], TextCommand().Run(c => c.AddParams(sources[i])).Order());

		var both = TextCommand();
		Assert.Equal(["A", "B", "C"], both.Run(c => c.AddParams(sources[0]).AddParams(sources[1])));
		Assert.Equal(1, both.Values["A"]);
		Assert.Equal("b", both.Values["B"]);
		Assert.Equal(2.5, both.Values["C"]);
	}

	[Fact]
	public static void AHiddenMember_OfARuntimeType_KeepsTheMostDerivedOne_AsItDoesForTheStaticType()
	{
		object boxed = new Hiding();
		var run = TextCommand();

		Assert.Equal(["Id", "Inherited"], run.Run(c => c.AddParams(boxed)).Order());
		Assert.Equal("derived", run.Values["Id"]);
	}

	[Fact]
	public static void ABoxedStruct_SendsItsMembers()
	{
		object boxed = new Point { X = 7 };
		var run = TextCommand();

		Assert.Equal(["X"], run.Run(c => c.AddParams(boxed)));
		Assert.Equal(7, run.Values["X"]);
	}

	[Fact]
	public static void ATypeWithNoMembers_AddsNothing_AndNobodyIsAsked()
	{
		object nothing = new();
		object noPublic = new NoPublicMembers();

		Assert.Empty(TextCommand().Run(c => c.AddParams(nothing)));
		Assert.Empty(TextCommand().Run(c => c.AddParams(noPublic)));

		var procedure = new SyncNameRun("@region");
		Assert.Empty(procedure.Run(c => c.AddParams(nothing)));
		Assert.Equal(0, procedure.Lookups); // nothing was added, so there is nothing to match
	}

	[Fact]
	public static void ANullObject_StillThrowsArgumentNullException()
	{
		var command = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		var ex = Assert.Throws<ArgumentNullException>(() => command.AddParams<object>(null!));

		Assert.Equal("source", ex.ParamName);
	}

	[Fact]
	public static void EveryPublicGetter_OfTheRuntimeType_IsReadWhenAddParamsIsCalled()
	{
		var command = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");
		object boxed = new LazyMember();

		var ex = Assert.Throws<InvalidOperationException>(() => command.AddParams(boxed));

		Assert.Equal("lazy", ex.Message);
	}

	[Fact]
	public static void AStaticTypeOtherThanObject_KeepsItsOwnMembers_WhateverTheObjectIs()
	{
		IHasRegion region = new WithRegion();
		Base derived = new Hiding(); // the static type is Base: Id is the int, and the runtime type's own members are not sent

		Assert.Equal(["Region"], TextCommand().Run(c => c.AddParams(region)));

		var run = TextCommand();
		Assert.Equal(["Id", "Inherited"], run.Run(c => c.AddParams(derived)).Order());
		Assert.Equal(1, run.Values["Id"]);
	}

	[Fact]
	public static void AnInterfaceVariable_SendsOnlyTheMembersOfTheInterface()
	{
		IHasRegion region = new WithRegion();

		Assert.Equal(["Region"], TextCommand().Run(c => c.AddParams(region)));
		Assert.Equal(["@region"], new SyncNameRun("@region", "@secret").Run(c => c.AddParams(region)));
	}

	[Fact]
	public static void ADictionary_SendsItsOwnProperties_NotItsEntries()
	{
		var entries = new Dictionary<string, int> { ["a"] = 1 };

		string[] names = TextCommand().Run(c => c.AddParams(entries));

		Assert.Contains("Count", names);
		Assert.Contains("Keys", names);
		Assert.DoesNotContain("a", names);
	}

	[Fact]
	public static void AHiddenMember_KeepsTheMostDerivedOne_AndWriteOnlyMembersAndIndexersAreIgnored()
	{
		var run = TextCommand();

		string[] names = run.Run(c => c.AddParams(new Hiding()));

		Assert.Equal(["Id", "Inherited"], names.Order());
		Assert.Equal("derived", run.Values["Id"]);
		Assert.Equal(5, run.Values["Inherited"]);
	}

	[Fact]
	public static void EveryPublicGetter_IsReadWhenAddParamsIsCalled_EvenOneTheProcedureDoesNotDeclare()
	{
		var command = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		var ex = Assert.Throws<InvalidOperationException>(() => command.AddParams(new LazyMember()));

		Assert.Equal("lazy", ex.Message);
	}

	[Fact]
	public static void TwoObjectsWithTheSameMember_Collide_AndTheMessageSaysSo()
	{
		var named = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@id").Run(c => c.AddParams(new { Id = 1 }).AddParams(new { Id = 2 })));
		Assert.StartsWith("Two parameters resolve to '@id': the member 'Id' of the AddParams object and the member 'Id' of the AddParams object.", named.Message);

		var written = Assert.Throws<InvalidOperationException>(() => TextCommand().Run(c => c.AddParams(new { Id = 1 }).AddParams(new { Id = 2 })));
		Assert.StartsWith("Two parameters resolve to 'Id': the member 'Id' of the AddParams object and the member 'Id' of the AddParams object.", written.Message);
	}

	// ---- what a caller can see of the names that were sent ----

	[Fact]
	public static void BeforeATerminalTheNamesAreAsWritten_TheProviderCommandHasTheRealOnes_AndAfterwardsParamsHoldsThem()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var command = new DerivedParameterNameTests.ProcCommand(probe.Connection, CommandType.StoredProcedure, "dbo.what_was_sent", "@company_serial_num");
		command.AddParam(value: 42, expression: "filter.CompanySerialNum");

		Assert.Equal(["CompanySerialNum"], command.Params.Select(p => p.Name)); // nothing is matched before a terminal runs

		string[]? seen = null;
		command.Execute(_ =>
		{
			seen = [.. probe.Parameters.Select(p => p.ParameterName)]; // the provider's command, as the function is given it
			return 0;
		});

		Assert.NotNull(seen);
		Assert.Equal(["@company_serial_num"], seen);
		Assert.Equal(["@company_serial_num"], command.Params.Select(p => p.Name));
		Assert.Equal([42], command.Params.Select(p => p.Value));
	}

	[Fact]
	public static void SeveralParameters_AreSentInTheOrderTheyWereAdded_EachWithItsValue()
	{
		const int companySerialNum = 42;
		const string? region = "west";
		DateTime? endDate = null;
		var run = new SyncNameRun("@company_serial_num", "@mode", "@region", "@end_date");

		string[] names = run.Run(c => c.AddParam(companySerialNum).AddParam("@mode", 7).AddParam(region).AddParam(endDate));

		Assert.Equal(["@company_serial_num", "@mode", "@region", "@end_date"], names);
		Assert.Equal([42, 7, "west", DBNull.Value], run.Probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static async Task SeveralParameters_AreSentInTheOrderTheyWereAdded_EachWithItsValue_Async()
	{
		const int companySerialNum = 42;
		const string? region = "west";
		DateTime? endDate = null;
		var run = new AsyncNameRun("@company_serial_num", "@mode", "@region", "@end_date");

		string[] names = await run.RunAsync(c => c.AddParam(companySerialNum).AddParam("@mode", 7).AddParam(region).AddParam(endDate));

		Assert.Equal(["@company_serial_num", "@mode", "@region", "@end_date"], names);
		Assert.Equal(new Dictionary<string, object?> { ["@company_serial_num"] = 42, ["@mode"] = 7, ["@region"] = "west", ["@end_date"] = DBNull.Value }, run.Values);
	}

	// ---- a NULL whose type no database type stands for is left to the provider ----

	[Fact]
	public static void ANullOfATypeWithoutADatabaseType_IsSentWithoutOne()
	{
		object? any = null;
		IDisposable? disposable = null;
		DateOnly? day = null;
		Point? point = null;
		var run = TextCommand();

		run.Run(c => c.AddParam(any).AddParam(disposable).AddParam(day).AddParam(point));

		Assert.Equal(4, run.Probe.Parameters.Count);
		Assert.All(run.Probe.Parameters, p =>
		{
			Assert.Equal(DBNull.Value, p.Value);
			Assert.Equal(default, p.DbType); // left to the provider to infer, as it always was
		});
	}

	// ---- a member the expression tree cannot box ----

	[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Public instance members on purpose: AddParams reads the instance members of a type through reflection.")]
	sealed class WithSpan
	{
		public int Id => 1;
		public ReadOnlySpan<char> Text => "x";
	}

	// Characterization of a defect, for the owner to decide: a public member of a ref struct type cannot be boxed, so the getters of the type cannot be built, and AddParams throws
	// (the type initializer of the member table fails, for that type, for the rest of the process). The swarm's suggestion is to skip such members; until then this pins what happens.
	// That is the rule of the STATIC type. When the argument is typed object the members are found by the runtime-type path, which skips such members, so one that holds a span is not a problem there.
	[Fact]
	public static void AMemberOfARefStructType_MakesAddParamsThrowForThatType_UntilSuchMembersAreSkipped()
	{
		var command = new ConnectionProbe(ConnectionState.Open).Connection.Command("select 1");

		Assert.Throws<TypeInitializationException>(() => command.AddParams(new WithSpan()));
		Assert.Throws<TypeInitializationException>(() => command.AddParams(new WithSpan())); // and it keeps throwing
	}

	[Fact]
	public static void AMemberOfARefStructType_OfARuntimeType_IsSkipped_AndTheOthersAreSent()
	{
		object boxed = new WithSpan();
		var run = TextCommand();

		Assert.Equal(["Id"], run.Run(c => c.AddParams(boxed)));
		Assert.Equal(1, run.Values["Id"]);
		Assert.Equal(["Id"], TextCommand().Run(c => c.AddParams(boxed))); // and again: nothing was left half built
	}

	// ---- the helpers, row by row (internal: reached the way the allocation tests reach them) ----

	static readonly Type Names = typeof(DataRecordExtensions).Assembly.GetType("Open.Database.Extensions.ParameterNames", throwOnError: true)!;

	static readonly Func<string, ImmutableArray<string>> Candidates = Names
		.GetMethod("Candidates", BindingFlags.Static | BindingFlags.NonPublic)!
		.CreateDelegate<Func<string, ImmutableArray<string>>>();

	static readonly Func<ImmutableArray<string>, ImmutableArray<string>, string, string, string?> Match = Names
		.GetMethod("Match", BindingFlags.Static | BindingFlags.NonPublic)!
		.CreateDelegate<Func<ImmutableArray<string>, ImmutableArray<string>, string, string, string?>>();

	[Theory]
	[InlineData("x", "x")]
	[InlineData("@class", "class")]
	[InlineData("this.Name", "Name")]
	[InlineData("(filter.X)", "filterX,X")]
	[InlineData("a.b.c", "abc,bc,c")]
	[InlineData("order.Customer.Id", "orderCustomerId,CustomerId,Id")]
	[InlineData("_author", "_author,author")]
	[InlineData("m_author", "m_author,author")]
	[InlineData("s_author", "s_author,author")]
	[InlineData("_1", "_1,1")]
	[InlineData("filter.m_author", "filterm_author,m_author,author")]
	[InlineData("order.Customer._id", "orderCustomer_id,Customer_id,_id,id")]
	[InlineData("M_author", "M_author")]
	[InlineData("m__author", "m__author")]
	[InlineData("mauthor", "mauthor")]
	[InlineData("_", "_")]
	[InlineData("m_", "m_")]
	public static void Candidates_AreTheLongestEndingFirst_ThenTheLastMemberWithoutAFieldPrefix(string expression, string expected)
		=> Assert.Equal(expected, string.Join(",", Candidates(expression)));

	[Theory]
	[InlineData("a + b")]
	[InlineData("x()")]
	[InlineData("this")]
	[InlineData("")]
	public static void Candidates_OfAnExpressionThatIsNotAChain_Throw(string expression)
		=> Assert.Throws<ArgumentException>(() => Candidates(expression));

	[Fact]
	public static void Match_ReturnsTheNameAsReported_TriesTheCandidatesInOrder_AndSaysNothingWhenNothingFits()
	{
		Assert.Equal("customer_id", Match(["CustomerId"], ["customer_id"], "customerId", "dbo.p"));
		Assert.Equal("@Customer_Id", Match(["CustomerId"], ["@Customer_Id"], "customerId", "dbo.p"));

		// An earlier candidate wins even when a later one fits better.
		Assert.Equal("@order_customer_id", Match(["orderCustomerId", "Id"], ["@id", "@order_customer_id"], "order.Customer.Id", "dbo.p"));

		Assert.Null(Match(["x"], [], "x", "dbo.p"));
		Assert.Null(Match(["x"], ["@y"], "x", "dbo.p"));
	}

	[Fact]
	public static void Match_AnAmbiguity_QuotesWhatWasWritten_TheCandidate_TheTierAndTheProcedure()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => Match(["orderCustomerId", "CustomerId"], ["@customer_id", "@customer__id"], "order.Customer.Id", "dbo.p"));

		Assert.Equal(
			"The argument 'order.Customer.Id' matches both '@customer_id' and '@customer__id' of 'dbo.p'. "
			+ "Its candidate name 'CustomerId' fits each of them ignoring case and underscores. "
			+ "The procedure declares: @customer_id, @customer__id. Name the parameter explicitly: AddParam(\"name\", value).",
			ex.Message);
	}
}
