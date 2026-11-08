namespace Open.Database.Extensions.Tests;

#nullable enable

// AddParam(value) on a command of CommandType.Text: the names the SQL declares are the @name tokens of its text, matched with the same four tiers as the parameters of a stored
// procedure (exact, ignoring case, ignoring case and underscores, then without a p_/in_/... prefix). Each case runs on ExpressiveCommand, on ExpressiveDbCommand, and through
// the asynchronous path of ExpressiveDbCommand. ExpressiveSqlCommand shares this code with them (SqlConnection is sealed, so only the SQL Server probe runs it).
[ExcludeFromCodeCoverage]
public static class SqlTextDerivedNamesTests
{
	static string[] OnCommand(string sql, CommandType type, Action<ExpressiveCommand> add)
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		var command = new ExpressiveCommand(probe.Connection, type, sql);
		add(command);
		command.ExecuteNonQuery();
		return [.. probe.Parameters.Select(p => p.ParameterName)];
	}

	static string[] OnDbCommand(string sql, CommandType type, Action<ExpressiveDbCommand> add)
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var command = new ExpressiveDbCommand(new DerivedParameterNameTests.OpenDbConnection(db), type, sql);
		add(command);
		command.ExecuteNonQuery();
		return [.. db.Sent.Select(p => p.ParameterName)];
	}

	static async Task<string[]> OnDbCommandAsync(string sql, CommandType type, Action<ExpressiveDbCommand> add)
	{
		var db = new DerivedParameterNameTests.RecordingDbCommand();
		var command = new ExpressiveDbCommand(new DerivedParameterNameTests.OpenDbConnection(db), type, sql);
		add(command);
		await command.ExecuteNonQueryAsync();
		return [.. db.Sent.Select(p => p.ParameterName)];
	}

	// The names sent when each expression is added with AddParam(value), on the three ways to run a command.
	static void AddAll(ExpressiveCommand command, string[] expressions)
	{
		for (int i = 0; i < expressions.Length; i++) command.AddParam(value: i + 1, expression: expressions[i]);
	}

	static void AddAll(ExpressiveDbCommand command, string[] expressions)
	{
		for (int i = 0; i < expressions.Length; i++) command.AddParam(value: i + 1, expression: expressions[i]);
	}

	static async Task<string[][]> AllWays(string sql, CommandType type, params string[] expressions)
		=> [
			OnCommand(sql, type, c => AddAll(c, expressions)),
			OnDbCommand(sql, type, c => AddAll(c, expressions)),
			await OnDbCommandAsync(sql, type, c => AddAll(c, expressions)),
		];

	static Task<string[][]> AllWays(string sql, params string[] expressions) => AllWays(sql, CommandType.Text, expressions);

	static async Task<InvalidOperationException[]> AllFail(string sql, params string[] expressions)
		=> [
			Assert.Throws<InvalidOperationException>(() => OnCommand(sql, CommandType.Text, c => AddAll(c, expressions))),
			Assert.Throws<InvalidOperationException>(() => OnDbCommand(sql, CommandType.Text, c => AddAll(c, expressions))),
			await Assert.ThrowsAsync<InvalidOperationException>(() => OnDbCommandAsync(sql, CommandType.Text, c => AddAll(c, expressions))),
		];

	// ---- the four tiers, on the tokens of the text ----

	[Theory]
	[InlineData("select 1 where a = @RegionId", "filter.RegionId", "@RegionId")] // exact
	[InlineData("select 1 where a = @regionid", "filter.RegionId", "@regionid")] // ignoring case
	[InlineData("select 1 where a = @region_id", "filter.RegionId", "@region_id")] // ignoring case and underscores
	[InlineData("select 1 where a = @REGION_ID", "filter.RegionId", "@REGION_ID")]
	[InlineData("select 1 where a = @p_name", "customer.Name", "@p_name")] // the prefix tier
	[InlineData("insert t (a) values (@in_region_id)", "filter.RegionId", "@in_region_id")]
	[InlineData("select 1 where a = @region_id and b = @p_name and c = @other", "filter.RegionId", "@region_id")] // the right one of several
	[InlineData("select 1 where a = @region_id or b = @region_id", "filter.RegionId", "@region_id")] // a name used twice is one name
	[InlineData("select 1 where a = @regionid or b = @RegionId", "filter.RegionId", "@regionid")] // the same name in two cases is one name: the first spelling
	[InlineData("select '@region_id' /* @regionid */ where a = @RegionId", "filter.RegionId", "@RegionId")] // a string and a comment declare nothing
	[InlineData("select 1 where a = @region_id -- @RegionId", "filter.RegionId", "@region_id")]
	[InlineData("declare @region_id int = 1; select @region_id", "filter.RegionId", "@region_id")]
	public static async Task TheDerivedName_IsTheTokenOfTheText(string sql, string expression, string expected)
	{
		foreach (string[] names in await AllWays(sql, expression)) Assert.Equal([expected], names);
	}

	[Fact]
	public static async Task SeveralArguments_EachFindTheirOwnToken()
	{
		foreach (string[] names in await AllWays("select 1 where a = @region_id and b = @p_name and c = @Limit", "filter.RegionId", "customer.Name", "page.Limit"))
			Assert.Equal(["@region_id", "@p_name", "@Limit"], names);
	}

	// ---- the outcomes are those of a stored procedure ----

	[Fact]
	public static async Task Ambiguity_Throws()
	{
		foreach (InvalidOperationException ex in await AllFail("select 1 where a = @customer_id and b = @customer__id", "order.Customer.Id"))
		{
			Assert.StartsWith("The argument 'order.Customer.Id' matches both '@customer_id' and '@customer__id' of '", ex.Message);
			Assert.Contains("Its candidate name 'CustomerId' fits each of them ignoring case and underscores.", ex.Message);
		}
	}

	[Fact]
	public static async Task NoMatch_Throws()
	{
		foreach (InvalidOperationException ex in await AllFail("select 1 where a = @a and b = @b", "order.Customer.Id"))
		{
			Assert.StartsWith("The argument 'order.Customer.Id' matches none of the parameters of '", ex.Message);
			Assert.Contains("tried orderCustomerId, CustomerId, Id", ex.Message);
			Assert.Contains("@a, @b", ex.Message);
		}
	}

	[Fact]
	public static async Task ANameOnlyInAStringOrAComment_IsNotDeclared()
	{
		foreach (InvalidOperationException ex in await AllFail("select '@region_id' /* @region_id */ where a = @other -- @region_id", "filter.RegionId"))
			Assert.StartsWith("The argument 'filter.RegionId' matches none of the parameters of '", ex.Message);
	}

	[Fact]
	public static async Task TwoArgumentsThatFindTheSameToken_Collide()
	{
		foreach (InvalidOperationException ex in await AllFail("select 1 where a = @region_id", "filter.RegionId", "other.RegionId"))
			Assert.StartsWith("Two parameters resolve to '@region_id': the argument 'filter.RegionId' and the argument 'other.RegionId'.", ex.Message);
	}

	// ---- no token: nothing that works today changes ----

	[Theory]
	[InlineData("select 1 where a = 1")]
	[InlineData("select @@rowcount")]
	[InlineData("select * from orders where region_id = :region_id")] // another provider's marker is not recognised
	[InlineData("select * from orders where region_id = $1")]
	[InlineData("select * from orders where region_id = ?")]
	public static async Task WithoutAnyToken_TheLastMemberIsUsedAsWritten(string sql)
	{
		foreach (string[] names in await AllWays(sql, "filter.RegionId")) Assert.Equal(["RegionId"], names);
	}

	[Theory]
	[InlineData(CommandType.TableDirect)]
	[InlineData(CommandType.StoredProcedure)]
	public static async Task OtherCommandTypes_DoNotReadTheText(CommandType type)
	{
		// Not a stored procedure lookup (no names are known on these fakes) and not a text: the text is never searched for tokens.
		foreach (string[] names in await AllWays("orders @region_id", type, "filter.RegionId")) Assert.Equal(["RegionId"], names);
	}

	// ---- AddParams ----

	sealed class Args
	{
		public int OrderId { get; set; } = 5;
		public string Name { get; set; } = "ann";
		public int Extra { get; set; } = 1;
	}

	static string[] Sorted(string[] names) => [.. names.Order(StringComparer.Ordinal)];

	[Fact]
	public static async Task AddParams_SendsOnlyTheMembersTheTextDeclares()
	{
		const string sql = "select 1 where a = @order_id and n = @Name";
		var args = new Args();

		Assert.Equal(["@Name", "@order_id"], Sorted(OnCommand(sql, CommandType.Text, c => c.AddParams(args))));
		Assert.Equal(["@Name", "@order_id"], Sorted(OnDbCommand(sql, CommandType.Text, c => c.AddParams(args))));
		Assert.Equal(["@Name", "@order_id"], Sorted(await OnDbCommandAsync(sql, CommandType.Text, c => c.AddParams(args))));
	}

	[Fact]
	public static async Task AddParams_WithoutAnyToken_SendsEveryMemberAsWritten()
	{
		const string sql = "select 1";
		var args = new Args();

		Assert.Equal(["Extra", "Name", "OrderId"], Sorted(OnCommand(sql, CommandType.Text, c => c.AddParams(args))));
		Assert.Equal(["Extra", "Name", "OrderId"], Sorted(OnDbCommand(sql, CommandType.Text, c => c.AddParams(args))));
		Assert.Equal(["Extra", "Name", "OrderId"], Sorted(await OnDbCommandAsync(sql, CommandType.Text, c => c.AddParams(args))));
	}

	[Fact]
	public static void AnExplicitName_WinsOverAMemberOfAnObject()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		new ExpressiveCommand(probe.Connection, CommandType.Text, "select 1 where a = @order_id")
			.AddParam("@order_id", 1)
			.AddParams(new Args { OrderId = 2 })
			.ExecuteNonQuery();

		Assert.Equal(["@order_id"], probe.Parameters.Select(p => p.ParameterName));
		Assert.Equal([1], probe.Parameters.Select(p => p.Value));
	}

	[Fact]
	public static void AnExplicitName_IsNeverCheckedAgainstTheText()
	{
		var probe = new ConnectionProbe(ConnectionState.Open);

		new ExpressiveCommand(probe.Connection, CommandType.Text, "select 1 where a = @order_id")
			.AddParam("@whatever", 1)
			.ExecuteNonQuery();

		Assert.Equal(["@whatever"], probe.Parameters.Select(p => p.ParameterName));
	}
}
