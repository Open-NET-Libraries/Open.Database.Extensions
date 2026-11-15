using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The call shapes of RowMap<T>.From, against the overload set of the pairs form (shipped) and the factory form:
// each shape compiles (an ambiguity, CS0121, would break the build), and what it does shows which overload bound:
// the pairs form returns Default for no pairs and otherwise builds the model through its constructor, a factory form builds the row by the function (it adds 100 to the id).
// This project compiles at the SDK's default language version (C# 14 on net10.0); the language versions before it are LegacyCompiler.Tests (C# 12 on net10.0),
// and C# 13 was checked by a scratch probe of the same overload set, with and without the attribute (the A1 task output).
[ExcludeFromCodeCoverage]
public static class FromBindingTests
{
	static readonly KeyValuePair<string, string?>[] Pairs = [new("Id", "N")];
	static readonly ImmutableArray<ColumnDef> Columns = [("Id", typeof(int)), ("N", typeof(int))];

	static int IdOf(RowMap<Post> map) => Assert.Single(Read(Route.Typed, Columns, [1, 7], map)).Id;

	static Post Plus100(int id, string? title) => new(id + 100, title ?? "");

	// ---- the shipped shapes bind to the pairs form ----

	[Fact]
	public static void FromDefault_BindsThePairsForm()
		=> Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(default));

	[Fact]
	public static void FromNull_BindsThePairsForm()
		=> Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(null));

	[Fact]
	public static void FromAnEmptyCollectionExpression_BindsThePairsForm()
		=> Assert.Same(RowMap<Post>.Default, RowMap<Post>.From([]));

	[Fact]
	public static void FromACollectionExpression_BindsThePairsForm()
	{
		RowMap<Post> map = RowMap<Post>.From([new("Id", "N")]);
		Assert.NotSame(RowMap<Post>.Default, map);
		Assert.Equal(7, IdOf(map)); // the pair is the pairs form's: column N feeds Id
	}

	[Fact]
	public static void FromASpan_BindsThePairsForm()
	{
		ReadOnlySpan<KeyValuePair<string, string?>> span = Pairs;
		Assert.Equal(7, IdOf(RowMap<Post>.From(span)));
		ReadOnlySpan<KeyValuePair<string, string?>> none = default;
		Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(none));
		Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(ReadOnlySpan<KeyValuePair<string, string?>>.Empty));
	}

	[Fact]
	public static void FromAnArray_BindsThePairsForm()
	{
		Assert.Equal(7, IdOf(RowMap<Post>.From(Pairs)));
		Assert.Same(RowMap<Post>.Default, RowMap<Post>.From(Array.Empty<KeyValuePair<string, string?>>()));
		Assert.Equal(7, IdOf(RowMap<Post>.From(Pairs.AsSpan())));
		Assert.Equal(7, IdOf(RowMap<Post>.From(ImmutableArray.Create(Pairs).AsSpan())));
	}

	// ---- the factory forms bind to the factory form ----

	[Fact]
	public static void AFactory_WithAndWithoutPairs_BindsTheFactoryForm()
	{
		Func<int, string?, Post> func = Plus100;
		Delegate asDelegate = func;
		ReadOnlySpan<KeyValuePair<string, string?>> span = Pairs;

		// Each is read through the function (101), whatever else the type could do by name; the one with the pair reads column N as the parameter `id`.
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(Plus100))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(asDelegate))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(static (int id, string? title) => new Post(id + 100, title ?? "")))).Id);

		Assert.Equal(107, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, new KeyValuePair<string, string?>("id", "N")))).Id);
		Assert.Equal(107, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, new("id", "N"), new("title", null)))).Id);
		Assert.Equal(107, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, [new("id", "N")]))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, []))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, default))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, null))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, Array.Empty<KeyValuePair<string, string?>>()))).Id);
		Assert.Equal(101, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, span[..0]))).Id);
		Assert.Equal(107, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, Pairs))).Id);
		Assert.Equal(107, Assert.Single(Read(Route.Typed, Columns, [1, 7], RowMap<Post>.From(func, span))).Id);
	}
}
