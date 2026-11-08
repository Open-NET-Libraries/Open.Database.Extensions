namespace Open.Database.Extensions.Tests;

#nullable enable

// The prefix tier removes one common parameter prefix (p_, in_, i_, prm_, par_, v_, or a lone p before a capital) from the REAL name and compares the rest.
// A parameter whose name merely STARTS like a prefix (@in_stock: the words "in" and "stock") is therefore found by the argument that names what follows it.
// These tests write down what happens today, so a change is a decision and not an accident. The docs warn about it and name the way out: name the parameter yourself.
[ExcludeFromCodeCoverage]
public static class ParameterNamePrefixWordTests
{
	const string NoMatch = "matches none of the parameters";

	// The prefix tier binds: the argument is what is left of the real name once the prefix is gone.
	[Theory]
	[InlineData("Stock", "@in_stock")]
	[InlineData("Progress", "@in_progress")]
	[InlineData("Value", "@par_value")]
	[InlineData("Verified", "@v_verified")]
	[InlineData("Item", "@i_item")]
	[InlineData("Ram", "@prm_ram")]
	[InlineData("Mount", "@p_mount")]
	[InlineData("Rice", "@pRice")]
	[InlineData("Ersion", "@v_ersion")]
	[InlineData("stock", "@IN_STOCK")]
	public static void AWordThatIsWhatFollowsAPrefix_BindsThroughThePrefixTier(string expression, string real)
		=> Assert.Equal([real], new SyncNameRun(real).Add(expression));

	// A real name that is one word with an underscore after its first letters binds at the underscore tier, before the prefix tier is reached (tier 3 ignores underscores).
	[Theory]
	[InlineData("Version", "@v_ersion")]
	[InlineData("Items", "@i_tems")]
	[InlineData("Parameters", "@par_ameters")]
	[InlineData("Instock", "@in_stock")]
	public static void AWordSplitByAnUnderscore_BindsAtTheUnderscoreTier_NotThroughThePrefix(string expression, string real)
		=> Assert.Equal([real], new SyncNameRun(real).Add(expression));

	// No underscore, no prefix: only a lone lower-case p before a capital is a prefix without one.
	[Theory]
	[InlineData("Stock", "@instock")]
	[InlineData("Value", "@parvalue")]
	[InlineData("Progress", "@inprogress")]
	[InlineData("Rice", "@price")]
	[InlineData("Ages", "@pages")]
	[InlineData("Stock", "@in_in_stock")]  // one prefix is removed, not two
	[InlineData("Stock", "@out_stock")]    // out_ is not a prefix of the list
	public static void AWordThatOnlyLooksLikeItHasAPrefix_DoesNotBind(string expression, string real)
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun(real).Add(expression));

		Assert.Contains(NoMatch, ex.Message);
	}

	[Fact]
	public static void ThePlainName_WinsOverThePrefixedOne_AndIsNeverAmbiguousWithIt()
	{
		Assert.Equal(["@stock"], new SyncNameRun("@in_stock", "@stock").Add("Stock"));
		Assert.Equal(["@Stock"], new SyncNameRun("@par_stock", "@in_stock", "@Stock").Add("stock"));
	}

	[Fact]
	public static void TwoPrefixedSpellings_ForTheSameWord_AreAmbiguous()
	{
		var ex = Assert.Throws<InvalidOperationException>(() => new SyncNameRun("@in_stock", "@par_stock").Add("Stock"));

		Assert.Contains("matches both '@in_stock' and '@par_stock'", ex.Message);
	}

	[Fact]
	public static void AChain_DoesNotHelp_TheLastMemberAloneBindsThroughThePrefix()
		=> Assert.Equal(["@in_stock"], new SyncNameRun("@in_stock").Add("warehouse.Stock"));

	[Fact]
	public static void NamingTheParameterYourself_AvoidsThePrefixTier()
	{
		// An explicit name is sent as written and never matched, so the procedure that declares only @in_stock gets @stock: the way out the docs give.
		var run = new SyncNameRun("@in_stock");

		Assert.Equal(["stock"], run.Run(c => c.AddParam("stock", 1)));
	}

	[Fact]
	public static async Task AWordThatIsWhatFollowsAPrefix_BindsThroughThePrefixTier_Async()
	{
		Assert.Equal(["@in_stock"], await new AsyncNameRun("@in_stock").AddAsync("Stock"));
		Assert.Equal(["@par_value"], await new AsyncNameRun("@par_value").AddAsync("Value"));
	}
}
