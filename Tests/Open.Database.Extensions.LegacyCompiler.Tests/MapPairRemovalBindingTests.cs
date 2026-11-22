using System;
using System.Collections.Generic;
using System.Data;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// The Map<T>(pair) and Map<T>(params pairs) overloads are removed
	// (K26): a pair is declared in a RowMap<T>
	// and passed to Map(RowMap<T>).
	// The binding is read from the IL of each call,
	// so a change of binding fails here,
	// and a call that becomes ambiguous (CS0121) fails the build.
	// Every target runs the calls that must compile
	// at its language version:
	// C# 7.3 (net472), 9 (net8.0), 12 (net10.0) and 13 (net11.0).
	// Map<T>(null), Map<T>(default) and Map<T>([]) are written
	// for C# 13 only: below it null and default are ambiguous,
	// and there are no collection expressions before C# 12.
	// C# 12 (net10.0) ignores [OverloadResolutionPriority],
	// so Map<T>(null) is CS0121 there, before and after the removal:
	// it is not written here.
	// C# 13 (net11.0) honours it:
	// null, default and an empty collection expression
	// bind the RowMap<T> overload.
	public static class MapPairRemovalBindingTests
	{
		sealed class Item
		{
			public int Id { get; set; }
		}

		static string Bound(Delegate lambda)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				if (il[i] != 0x28) continue; // call
				try
				{
					var called = method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1));
					if (called == null || called.Name != "Map") continue;
					var parameters = called.GetParameters();
					found = parameters.Length == 1 ? "" : parameters[1].ParameterType.Name;
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		static readonly RowMap<Item> Declared = new RowMap<Item>((System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)new[] { new KeyValuePair<string, string>("Id", "id") });

		[Fact]
		public static void ADeclaredMapAndANewMap_BindTheMapOverload_OnEveryReceiver()
		{
			const string map = "RowMap`1";

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map(Declared))));
			Assert.Equal(map, Bound((Func<IDataReader, object>)(r => r.Map(Declared))));
			Assert.Equal(map, Bound((Func<IDbCommand, object>)(r => r.Map(Declared))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map(Declared))));

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map(new RowMap<Item>((System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)new[] { new KeyValuePair<string, string>("Id", "a") })))));
			Assert.Equal(map, Bound((Func<IDataReader, object>)(r => r.Map(new RowMap<Item>((System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)new[] { new KeyValuePair<string, string>("Id", "a") })))));
			Assert.Equal(map, Bound((Func<IDbCommand, object>)(r => r.Map(new RowMap<Item>((System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)new[] { new KeyValuePair<string, string>("Id", "a") })))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map(new RowMap<Item>((System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)new[] { new KeyValuePair<string, string>("Id", "a") })))));

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map((RowMap<Item>)null))));
			Assert.Equal(map, Bound((Func<IDataReader, object>)(r => r.Map((RowMap<Item>)null))));
			Assert.Equal(map, Bound((Func<IDbCommand, object>)(r => r.Map((RowMap<Item>)null))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map((RowMap<Item>)null))));
		}

		[Fact]
		public static void TheOtherShapes_StillBindWhatTheyDid()
		{
			Assert.Equal("", Bound((Func<IExecuteReader, object>)(r => r.Map<Item>())));
			Assert.Equal("", Bound((Func<IDataReader, object>)(r => r.Map<Item>())));
			Assert.Equal("", Bound((Func<IDbCommand, object>)(r => r.Map<Item>())));
			Assert.Equal("", Bound((Func<DataTable, object>)(r => r.Map<Item>())));

			Assert.Equal("Func`2", Bound((Func<IExecuteReader, object>)(r => r.Map(_ => new Item()))));
			Assert.Equal("Func`2", Bound((Func<IDataReader, object>)(r => r.Map(_ => new Item()))));
			Assert.Equal("Func`2", Bound((Func<IDbCommand, object>)(r => r.Map(_ => new Item()))));
			Assert.Equal("Func`2", Bound((Func<DataTable, object>)(r => r.Map(_ => new Item()))));
		}

#if NET11_0_OR_GREATER
		[Fact]
		public static void NullDefaultAndEmptyCollection_BindTheMapOverload_OnCSharp13()
		{
			const string map = "RowMap`1";

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map<Item>(null))));
			Assert.Equal(map, Bound((Func<IDataReader, object>)(r => r.Map<Item>(null))));
			Assert.Equal(map, Bound((Func<IDbCommand, object>)(r => r.Map<Item>(null))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map<Item>(null))));

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map<Item>(default))));
			Assert.Equal(map, Bound((Func<IDataReader, object>)(r => r.Map<Item>(default))));
			Assert.Equal(map, Bound((Func<IDbCommand, object>)(r => r.Map<Item>(default))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map<Item>(default))));

			Assert.Equal(map, Bound((Func<IExecuteReader, object>)(r => r.Map<Item>([]))));
			Assert.Equal(map, Bound((Func<DataTable, object>)(r => r.Map<Item>([]))));
		}
#endif
	}
}
