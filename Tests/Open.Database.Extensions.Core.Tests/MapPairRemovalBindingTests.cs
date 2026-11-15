#nullable enable

namespace Open.Database.Extensions.Tests;

// The Map<T>(pair) and Map<T>(params pairs) overloads are removed (K26):
// a pair is declared in a RowMap<T> and passed to Map(RowMap<T>?).
// The binding is read from the IL of each call (the method it calls),
// not from whether it compiles, so a change of binding fails here.
// A call shape that must not compile after the removal
// (Map<T>(new KeyValuePair<string, string?>("a", "b")), Map<T>(pair, pair))
// cannot be a test; the compiler matrix is in the A3 notes of the GA briefs.
[ExcludeFromCodeCoverage]
public static class MapPairRemovalBindingTests
{
	sealed class Item { public int Id { get; set; } }

	// The type of the second parameter of the Map<T> the lambda calls;
	// "" for the one without an argument.
	static string Bound(Delegate lambda)
	{
		var method = lambda.Method;
		byte[] il = method.GetMethodBody()!.GetILAsByteArray()!;
		string found = "?";
		for (int i = 0; i + 4 < il.Length; i++)
		{
			if (il[i] != 0x28) continue; // call
			try
			{
				var called = method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1));
				if (called?.Name != "Map") continue;
				var parameters = called.GetParameters();
				found = parameters.Length == 1 ? "" : parameters[1].ParameterType.Name;
			}
			catch (ArgumentException) { }
		}

		return found;
	}

	static readonly RowMap<Item> Declared = new(new("Id", "id"));

	[Fact]
	public static void Null_Default_AndCollectionExpressions_BindTheMapOverload_OnEveryReceiver()
	{
		const string map = "RowMap`1";

		Assert.Equal(map, Bound((IExecuteReader r) => r.Map<Item>(null)));
		Assert.Equal(map, Bound((IDataReader r) => r.Map<Item>(null)));
		Assert.Equal(map, Bound((IDbCommand r) => r.Map<Item>(null)));
		Assert.Equal(map, Bound((DataTable r) => r.Map<Item>(null)));

		Assert.Equal(map, Bound((IExecuteReader r) => r.Map<Item>(default)));
		Assert.Equal(map, Bound((IDataReader r) => r.Map<Item>(default)));
		Assert.Equal(map, Bound((IDbCommand r) => r.Map<Item>(default)));
		Assert.Equal(map, Bound((DataTable r) => r.Map<Item>(default)));

		// A collection expression converts to a RowMap<T> (its CollectionBuilder),
		// so the shipped call shape Map<T>([...]) still compiles, and binds here.
		Assert.Equal(map, Bound((IExecuteReader r) => r.Map<Item>([])));
		Assert.Equal(map, Bound((IDataReader r) => r.Map<Item>([new("Id", "a")])));
		Assert.Equal(map, Bound((IDbCommand r) => r.Map<Item>([new("Id", "a")])));
		Assert.Equal(map, Bound((DataTable r) => r.Map<Item>([new("Id", "a")])));
	}

	[Fact]
	public static void ADeclaredMapAndANewMap_BindTheMapOverload_OnEveryReceiver()
	{
		const string map = "RowMap`1";

		Assert.Equal(map, Bound((IExecuteReader r) => r.Map(Declared)));
		Assert.Equal(map, Bound((IDataReader r) => r.Map(Declared)));
		Assert.Equal(map, Bound((IDbCommand r) => r.Map(Declared)));
		Assert.Equal(map, Bound((DataTable r) => r.Map(Declared)));

		Assert.Equal(map, Bound((IExecuteReader r) => r.Map(new RowMap<Item>(new("Id", "a")))));
		Assert.Equal(map, Bound((IDataReader r) => r.Map(new RowMap<Item>(new("Id", "a")))));
		Assert.Equal(map, Bound((IDbCommand r) => r.Map(new RowMap<Item>([new("Id", "a")]))));
		Assert.Equal(map, Bound((DataTable r) => r.Map(new RowMap<Item>(new("Id", "a"), new("Id", "b")))));
	}

	[Fact]
	public static void TheOtherShapes_StillBindWhatTheyDid()
	{
		Assert.Equal("", Bound((IExecuteReader r) => r.Map<Item>()));
		Assert.Equal("", Bound((IDataReader r) => r.Map<Item>()));
		Assert.Equal("", Bound((IDbCommand r) => r.Map<Item>()));
		Assert.Equal("", Bound((DataTable r) => r.Map<Item>()));

		Assert.Equal("Func`2", Bound((IExecuteReader r) => r.Map(x => new Item())));
		Assert.Equal("Func`2", Bound((IDataReader r) => r.Map(x => new Item())));
		Assert.Equal("Func`2", Bound((IDbCommand r) => r.Map(x => new Item())));
		Assert.Equal("Func`2", Bound((DataTable r) => r.Map(x => new Item())));
	}
}
