namespace Open.Database.Extensions.Generator.Tests;

// Models that declare a NULL policy (NullPolicy, NullValue). The generated readers do not apply one, so such a model is never intercepted: its
// Map<T>() call stays the runtime's, and the policy is never silently ignored. They live in the tests' own assembly because the attributes are
// Core's and the models library references nothing of ODX. Each has an unmarked twin of the same shape (UnmarkedPost) that IS served, so the
// attribute is what makes the difference.

/// <summary>The control: the same shape as the policy models, no policy. Served by generated code.</summary>
public sealed class UnmarkedPost
{
	public int Id { get; set; }

	public string Title { get; set; } = "untitled";

	public int Qty { get; set; }
}

/// <summary>A policy on the type: every member's NULL leaves it as it is.</summary>
[NullPolicy(NullAction.Default)]
public sealed class PolicyOnType
{
	public int Id { get; set; }

	public string Title { get; set; } = "untitled";

	public int Qty { get; set; }
}

/// <summary>A policy on one member: its NULL throws, naming the member and the column.</summary>
public sealed class PolicyOnMember
{
	public int Id { get; set; }

	[NullPolicy(NullAction.Throw)]
	public string? Title { get; set; } = "untitled";

	public int Qty { get; set; }
}

/// <summary>A base type that declares the policy (NullPolicy is inherited): it covers the derived model's members.</summary>
[NullPolicy(NullAction.Default)]
public class PolicyBase
{
	public int Id { get; set; }
}

/// <summary>A model whose only policy is its base type's.</summary>
public sealed class PolicyDerived : PolicyBase
{
	public string Title { get; set; } = "untitled";

	public int Qty { get; set; }
}

/// <summary>A value for a member's NULL.</summary>
public sealed class ValueOnMember
{
	public int Id { get; set; }

	public string Title { get; set; } = "untitled";

	[NullValue(-1)]
	public int Qty { get; set; }
}

/// <summary>A policy on a constructor parameter: its NULL passes the declared default.</summary>
public sealed class PolicyOnParameter(int id, [NullPolicy(NullAction.Default)] string title = "none", int qty = 0)
{
	public int Id { get; } = id;

	public string Title { get; } = title;

	public int Qty { get; } = qty;
}

/// <summary>A value on a positional record's parameter.</summary>
public sealed record PolicyRecord(int Id, [NullValue("none")] string Title, int Qty);

public partial class ParityTests
{
	static readonly Col[] PolicyColumns = [Col.Of<int>("Id"), Col.Of<string>("Title"), Col.Of<int>("Qty")];

	static readonly object?[][] PolicyRows =
	[
		[1, "one", 2],
		[2, null, null],
		[null, "three", 4],
	];

	static TableQuery PolicyQuery() => new(PolicyColumns, PolicyRows);

	/// <summary>Not intercepted at all: no counter moved, so the call ran the runtime map.</summary>
	static void NotIntercepted(Counts c)
		=> Assert.True(c == default, $"expected no interception: {c}");

	[Fact]
	public void The_unmarked_twin_of_the_policy_models_is_served()
	{
		var q = new TableQuery(PolicyColumns, PolicyRows[0], [2, "two", 3]);

		var start = Counts.Now();
		var generated = q.Map<UnmarkedPost>().ToList();
		Counts c = Counts.Now().Since(start);

		Parity.SameRows(q.Map(RowMap<UnmarkedPost>.Default).ToList(), generated);
		Served(c);
	}

	[Fact]
	public void A_policy_on_the_type_is_not_intercepted_and_its_rows_are_the_runtime_rows()
	{
		var q = PolicyQuery();

		var start = Counts.Now();
		SameOutcome(() => q.Map(RowMap<PolicyOnType>.Default).ToList(), () => q.Map<PolicyOnType>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		// The policy applied: Default leaves the NULL members as the type made them.
		var rows = q.Map<PolicyOnType>().ToList();
		Assert.Equal("untitled", rows[1].Title);
		Assert.Equal(0, rows[2].Id);
	}

	[Fact]
	public void A_policy_on_a_member_is_not_intercepted_and_its_exception_is_the_runtime_exception()
	{
		var q = PolicyQuery();

		var start = Counts.Now();
		SameThrow(() => q.Map(RowMap<PolicyOnMember>.Default).ToList(), () => q.Map<PolicyOnMember>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		// The policy applied: Throw names the member and the column.
		var e = Assert.IsType<ValueMappingException>(Catch(() => q.Map<PolicyOnMember>().ToList()));
		Assert.Equal("Title", e.PropertyName);
		Assert.Equal("Title", e.ColumnName);
	}

	[Fact]
	public void A_policy_on_a_base_type_is_not_intercepted_and_its_rows_are_the_runtime_rows()
	{
		var q = PolicyQuery();

		var start = Counts.Now();
		SameOutcome(() => q.Map(RowMap<PolicyDerived>.Default).ToList(), () => q.Map<PolicyDerived>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		var rows = q.Map<PolicyDerived>().ToList();
		Assert.Equal("untitled", rows[1].Title);
		Assert.Equal(0, rows[2].Id);
	}

	[Fact]
	public void A_null_value_on_a_member_is_not_intercepted_and_its_rows_are_the_runtime_rows()
	{
		// Only Qty is NULL here (a NULL in Title or Id has no policy: the runtime's NULL report).
		var q = new TableQuery(PolicyColumns, PolicyRows[0], [2, "two", null]);

		var start = Counts.Now();
		SameOutcome(() => q.Map(RowMap<ValueOnMember>.Default).ToList(), () => q.Map<ValueOnMember>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		Assert.Equal(-1, q.Map<ValueOnMember>().ToList()[1].Qty);
	}

	[Fact]
	public void A_policy_on_a_constructor_parameter_is_not_intercepted_and_its_rows_are_the_runtime_rows()
	{
		var q = new TableQuery(PolicyColumns, PolicyRows[0], [2, null, 3]);

		var start = Counts.Now();
		SameOutcome(() => q.Map(RowMap<PolicyOnParameter>.Default).ToList(), () => q.Map<PolicyOnParameter>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		Assert.Equal("none", q.Map<PolicyOnParameter>().ToList()[1].Title);
	}

	[Fact]
	public void A_null_value_on_a_record_parameter_is_not_intercepted_and_its_rows_are_the_runtime_rows()
	{
		var q = new TableQuery(PolicyColumns, PolicyRows[0], [2, null, 3]);

		var start = Counts.Now();
		SameOutcome(() => q.Map(RowMap<PolicyRecord>.Default).ToList(), () => q.Map<PolicyRecord>().ToList());
		NotIntercepted(Counts.Now().Since(start));

		Assert.Equal("none", q.Map<PolicyRecord>().ToList()[1].Title);
	}

	[Fact]
	public void A_NULL_with_no_policy_on_a_policy_model_is_the_runtime_report()
	{
		// Id has no policy of its own and the member's policy covers only Title: the NULL in Id is the runtime's row report, unchanged.
		var q = new TableQuery(PolicyColumns, PolicyRows[0], [null, "two", 3]);

		var start = Counts.Now();
		SameRowError<InvalidCastException>(() => q.Map(RowMap<PolicyOnMember>.Default).ToList(), () => q.Map<PolicyOnMember>().ToList(), typeof(PolicyOnMember), 1);
		NotIntercepted(Counts.Now().Since(start));
	}
}
