using System;
using System.Data;
using System.Data.Common;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// The definition-first query on older compilers. A compiler older than C# 13
	// ignores [OverloadResolutionPriority], so without the overload for a string
	// value AddParam("author", "ann") would bind the deriving overload
	// AddParam<T>(T value, string expression) and send the parameter "ann".
	// The binding is read from the IL of each call, at every language version of
	// this project (C# 7.3, 9, 12 and 13).
	// Nothing here is written with a record, a target-typed new or any other
	// later syntax; Map binds the one extension on IExecuteReader on a struct
	// (boxing receiver) and on a class.
	public static class DbQueryBindingTests
	{
		sealed class NoConnection : DbConnection
		{
			public override string ConnectionString { get; set; }
			public override string Database => "";
			public override string DataSource => "";
			public override string ServerVersion => "";
			public override ConnectionState State => ConnectionState.Closed;
			public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();
			public override void Close() { }
			public override void Open() => throw new NotSupportedException();
			protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
			protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
		}

		readonly struct AuthorViews : IEquatable<AuthorViews>
		{
			public readonly int AuthorId;
			public readonly int Views;

			public AuthorViews(int authorId, int views)
			{
				AuthorId = authorId;
				Views = views;
			}

			public bool Equals(AuthorViews other) => AuthorId == other.AuthorId && Views == other.Views;

			public override bool Equals(object obj) => obj is AuthorViews other && Equals(other);

			public override int GetHashCode() => (AuthorId * 31) + Views;
		}

		sealed class Post
		{
			public int Id { get; set; }
		}

		static readonly DbQuery Q = DbQuery.Text("select 1");

		// The parameter types of the last call to a method of this name.
		static string Bound(Delegate lambda, string name)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				// call or callvirt
				if (il[i] != 0x28 && il[i] != 0x6F) continue;
				try
				{
					var called = method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1));
					if (called == null || called.Name != name) continue;
					var definition = called is System.Reflection.MethodInfo generic && generic.IsGenericMethod
						? generic.GetGenericMethodDefinition()
						: called;
					found = (definition.IsGenericMethod ? "<T>" : "")
						+ string.Join(",", Array.ConvertAll(definition.GetParameters(), p => p.ParameterType.IsGenericParameter ? "T" : p.ParameterType.Name));
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		[Fact]
		public static void AddParam_WithAStringValue_BindsTheNamedStringOverload_OnEveryReceiver()
		{
			const string name = "author";
			string s = "ann";

			Assert.Equal("String,String", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", "ann")), "AddParam"));
			Assert.Equal("String,String", Bound((Func<DbQueryUsing, object>)(u => u.AddParam(name, s)), "AddParam"));
			Assert.Equal("String,String", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", null)), "AddParam"));
			Assert.Equal("String,String", Bound((Func<DbQueryCall, object>)(c => c.AddParam("author", "ann")), "AddParam"));
			Assert.Equal("String,String", Bound((Func<DbQueryCall, object>)(c => c.AddParam(name, s)), "AddParam"));
		}

		[Fact]
		public static void AddParam_WithAnyOtherValue_BindsTheNamedGenericOverload()
		{
			object o = "ann";

			Assert.Equal("<T>String,T", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", 5)), "AddParam"));
			Assert.Equal("<T>String,T", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", 5L)), "AddParam"));
			Assert.Equal("<T>String,T", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", (int?)5)), "AddParam"));
			Assert.Equal("<T>String,T", Bound((Func<DbQueryUsing, object>)(u => u.AddParam("author", o)), "AddParam"));
			Assert.Equal("<T>String,T", Bound((Func<DbQueryCall, object>)(c => c.AddParam("author", 5)), "AddParam"));
		}

		[Fact]
		public static void AddParam_WithOneArgument_BindsTheDerivingOverload()
		{
			string author = "ann";
			int limit = 5;

			Assert.Equal("<T>T,String", Bound((Func<DbQueryUsing, object>)(u => u.AddParam(author)), "AddParam"));
			Assert.Equal("<T>T,String", Bound((Func<DbQueryUsing, object>)(u => u.AddParam(limit)), "AddParam"));
			Assert.Equal("<T>T,String", Bound((Func<DbQueryCall, object>)(c => c.AddParam(author)), "AddParam"));
		}

		[Fact]
		public static void Map_OnEveryShape_BindsTheExtensionOnIExecuteReader()
		{
			var map = new RowMap<Post>(new System.Collections.Generic.KeyValuePair<string, string>("Id", "id"));

			Assert.Equal("", MapShape((Func<DbQueryUsing, object>)(u => u.Map<Post>())));
			Assert.Equal("", MapShape((Func<DbQueryCall, object>)(c => c.Map<Post>())));
			Assert.Equal("RowMap`1", MapShape((Func<DbQueryUsing, object>)(u => u.Map(map))));
			Assert.Equal("RowMap`1", MapShape((Func<DbQueryCall, object>)(c => c.Map(map))));
			Assert.Equal("Func`2", MapShape((Func<DbQueryUsing, object>)(u => u.Map(r => r.GetInt32(0)))));
			Assert.Equal("Func`2", MapShape((Func<DbQueryCall, object>)(c => c.Map(r => r.GetInt32(0)))));
		}

		static string MapShape(Delegate lambda)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				if (il[i] != 0x28) continue;
				try
				{
					var called = method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1));
					if (called == null || called.Name != "Map") continue;
					var parameters = called.GetParameters();
					Assert.Equal(typeof(IExecuteReader), parameters[0].ParameterType);
					found = parameters.Length == 1 ? "" : parameters[1].ParameterType.Name;
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		[Fact]
		public static void TheTypedForm_NeedsNoTypeArgumentAtTheCall()
		{
			DbQuery<AuthorViews> typed = Q.Params<AuthorViews>();
			var connection = new NoConnection();
			var values = new AuthorViews(7, 1);

			DbQueryCall call = typed.Using(connection).AddParams(values);

			Assert.Same(connection, call.Source);
			Assert.Equal(call, typed.Using(connection).AddParams(values));
		}

		[Fact]
		public static void ACallKeepsItsSourceAndBranches()
		{
			var connection = new NoConnection();
			DbQueryCall held = Q.Using(connection).AddParam("a", 1);
			DbQueryCall one = held.AddParam("b", 2);
			DbQueryCall other = held.AddParam("b", 3);

			Assert.Same(connection, one.Source);
			Assert.NotEqual(one, other);
			Assert.Equal(held.AddParam("b", 2), one);
		}
	}
}
