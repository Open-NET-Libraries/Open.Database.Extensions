using System;
using System.Collections.Generic;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// RowMap<T>.From(pairs) is shipped; From(Delegate factory, params ReadOnlySpan<pair>) is added beside it, and the shipped overload carries [OverloadResolutionPriority(1)].
	// Every call in this file must COMPILE at the language version of its target (7.3, 9, 12), so the build of this project is the guard: a call that becomes ambiguous (CS0121) fails the build.
	// What a call does says which overload bound: the pairs form returns RowMap<T>.Default for no pairs, and the factory form always makes a new map.
	// A compiler older than C# 13 does not know `params ReadOnlySpan<T>`, so for it the factory form needs its pairs argument written out, and a one-argument call is the pairs form's alone.
	public static class FromBindingTests
	{
		sealed class Item
		{
			public Item(int id) { Id = id; }

			public int Id { get; }
		}

		static Item Make(int id) { return new Item(id); }

		static readonly KeyValuePair<string, string>[] Pairs = { new KeyValuePair<string, string>("Id", "N") };

		[Fact]
		public static void TheShippedShapes_BindThePairsForm()
		{
			Assert.Same(RowMap<Item>.Default, RowMap<Item>.From(default));
			Assert.Same(RowMap<Item>.Default, RowMap<Item>.From(null));
			Assert.Same(RowMap<Item>.Default, RowMap<Item>.From(new KeyValuePair<string, string>[0]));
			Assert.Same(RowMap<Item>.Default, RowMap<Item>.From(default(ReadOnlySpan<KeyValuePair<string, string>>)));

			ReadOnlySpan<KeyValuePair<string, string>> span = Pairs;
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(span));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(Pairs));

#if NET10_0_OR_GREATER
			// Collection expressions are C# 12.
			Assert.Same(RowMap<Item>.Default, RowMap<Item>.From([]));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From([new KeyValuePair<string, string>("Id", "N")]));
#endif
		}

		[Fact]
		public static void TheFactoryForm_IsReachableWithItsPairsWrittenOut()
		{
			Func<int, Item> func = Make;
			Delegate asDelegate = func;
			ReadOnlySpan<KeyValuePair<string, string>> span = Pairs;

			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(func, Pairs));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(asDelegate, Pairs));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(func, span));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(func, default(ReadOnlySpan<KeyValuePair<string, string>>)));
			Assert.NotSame(RowMap<Item>.Default, RowMap<Item>.From(func, new KeyValuePair<string, string>[0]));
		}
	}
}
