using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// The cancellation token is passable positionally after any prefix of the optional parameters:
	// where a comparer, a command behavior or a flag is optional, the call can put the token straight after
	// the parameters before it. Each family is a ladder of rungs, every one ending in a token with a default,
	// and the optional parameter in the middle is required on the rung that names it.
	// The binding is read from the IL of each call (the method it calls), so a change of binding fails here,
	// and a call that becomes ambiguous (CS0121) fails the build.
	// Every target runs the calls at its language version: C# 7.3 (net472), 9 (net8.0), 12 (net10.0) and 13 (net11.0).
	// A bare default must bind the rung that takes the token, a bare null the rung that takes the comparer,
	// with no help from [OverloadResolutionPriority] (which only C# 13 reads).
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2012:Use ValueTasks correctly", Justification = "The calls are compiled only so their IL shows which overload binds; they never run.")]
	public static class PositionalTokenBindingTests
	{
		static readonly CancellationToken Token = new CancellationToken();
		static readonly Func<int, int> Key = i => i;
		static readonly Func<int, string> Value = i => "";
		static readonly IEqualityComparer<int> IntComparer = EqualityComparer<int>.Default;
		static readonly Action<DbDataReader> DbHandler = r => { };
		static readonly Func<DbDataReader, ValueTask> DbAsyncHandler = r => default;
		static readonly Func<DbDataReader, int> DbTransform = r => 1;
		static readonly Func<DbDataReader, ValueTask<int>> DbAsyncTransform = r => default;
		static readonly Func<IDataReader, ValueTask> DataAsyncHandler = r => default;
		static readonly Func<IDataReader, ValueTask<int>> DataAsyncTransform = r => default;
		static readonly Action<IDataRecord> Record = r => { };

		static string Name(Type type)
		{
			if (!type.IsGenericType) return type.Name;

			var names = new List<string>();
			foreach (Type argument in type.GetGenericArguments())
				names.Add(Name(argument));

			return type.Name.Split('`')[0] + "<" + string.Join(",", names) + ">";
		}

		// The method a lambda calls by this name: the type that declares it and its parameter types.
		static string Bound(Delegate lambda, string name)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				if (il[i] != 0x28 && il[i] != 0x6F) continue; // call, callvirt
				try
				{
					var called = method.Module.ResolveMethod(BitConverter.ToInt32(il, i + 1));
					if (called == null || called.Name != name) continue;

					var types = new List<string>();
					foreach (ParameterInfo parameter in called.GetParameters())
						types.Add(Name(parameter.ParameterType));

					found = Name(called.DeclaringType) + "." + name + "(" + string.Join(",", types) + ")";
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		static void Binds<T>(string expected, string name, Action<T> call)
			=> Assert.Equal(expected, Bound(call, name));

		// ---- ToHashSetAsync: (token) and (comparer, token) ----

		static void HashSetLadder<T>(string type, Action<T> none, Action<T> token, Action<T> defaulted, Action<T> comparerNull, Action<T> comparer, Action<T> both, string extension = null)
		{
			string tokenRung = extension == null ? type + ".ToHashSetAsync(CancellationToken)" : extension + ".ToHashSetAsync(" + type + ",CancellationToken)";
			string comparerRung = extension == null
				? type + ".ToHashSetAsync(IEqualityComparer<Int32>,CancellationToken)"
				: extension + ".ToHashSetAsync(" + type + ",IEqualityComparer<Int32>,CancellationToken)";

			Binds(tokenRung, "ToHashSetAsync", none);
			Binds(tokenRung, "ToHashSetAsync", token);
			Binds(tokenRung, "ToHashSetAsync", defaulted);
			Binds(comparerRung, "ToHashSetAsync", comparerNull);
			Binds(comparerRung, "ToHashSetAsync", comparer);
			Binds(comparerRung, "ToHashSetAsync", both);
		}

		[Fact]
		public static void ToHashSetAsync_OnEveryView_BindsEachRung()
		{
			HashSetLadder<IDataReader>("MappedReader<Int32>",
				r => r.Map<int>().ToHashSetAsync(), r => r.Map<int>().ToHashSetAsync(Token), r => r.Map<int>().ToHashSetAsync(default),
				r => r.Map<int>().ToHashSetAsync(null), r => r.Map<int>().ToHashSetAsync(IntComparer), r => r.Map<int>().ToHashSetAsync(IntComparer, Token));
			HashSetLadder<IDbCommand>("MappedCommand<Int32>",
				c => c.Map<int>().ToHashSetAsync(), c => c.Map<int>().ToHashSetAsync(Token), c => c.Map<int>().ToHashSetAsync(default),
				c => c.Map<int>().ToHashSetAsync(null), c => c.Map<int>().ToHashSetAsync(IntComparer), c => c.Map<int>().ToHashSetAsync(IntComparer, Token));
			HashSetLadder<DataTable>("MappedTable<Int32>",
				t => t.Map<int>().ToHashSetAsync(), t => t.Map<int>().ToHashSetAsync(Token), t => t.Map<int>().ToHashSetAsync(default),
				t => t.Map<int>().ToHashSetAsync(null), t => t.Map<int>().ToHashSetAsync(IntComparer), t => t.Map<int>().ToHashSetAsync(IntComparer, Token));
			HashSetLadder<IExecuteReader>("MappedQuery<Int32>",
				e => e.Map<int>().ToHashSetAsync(), e => e.Map<int>().ToHashSetAsync(Token), e => e.Map<int>().ToHashSetAsync(default),
				e => e.Map<int>().ToHashSetAsync(null), e => e.Map<int>().ToHashSetAsync(IntComparer), e => e.Map<int>().ToHashSetAsync(IntComparer, Token));
			HashSetLadder<IExecuteReader>("MappedQueryWithSkip<Int32>",
				e => e.Map<int>().Skip(1).ToHashSetAsync(), e => e.Map<int>().Skip(1).ToHashSetAsync(Token), e => e.Map<int>().Skip(1).ToHashSetAsync(default),
				e => e.Map<int>().Skip(1).ToHashSetAsync(null), e => e.Map<int>().Skip(1).ToHashSetAsync(IntComparer), e => e.Map<int>().Skip(1).ToHashSetAsync(IntComparer, Token));
			HashSetLadder<IExecuteReader>("MappedQueryWithTake<Int32>",
				e => e.Map<int>().Take(1).ToHashSetAsync(), e => e.Map<int>().Take(1).ToHashSetAsync(Token), e => e.Map<int>().Take(1).ToHashSetAsync(default),
				e => e.Map<int>().Take(1).ToHashSetAsync(null), e => e.Map<int>().Take(1).ToHashSetAsync(IntComparer), e => e.Map<int>().Take(1).ToHashSetAsync(IntComparer, Token));
		}

		[Fact]
		public static void ToHashSetAsync_OnTheInterface_BindsEachRung()
			=> HashSetLadder<IMappedQuery<int>>("IMappedQuery<Int32>",
				q => q.ToHashSetAsync(), q => q.ToHashSetAsync(Token), q => q.ToHashSetAsync(default),
				q => q.ToHashSetAsync(null), q => q.ToHashSetAsync(IntComparer), q => q.ToHashSetAsync(IntComparer, Token),
				"MappedQueryExtensions");

		[Fact]
		public static void ToHashSetAsync_OnACommand_BindsEachRung()
		{
			const string Token1 = "IExecuteReaderExtensions.ToHashSetAsync(IExecuteReader,CancellationToken)";
			const string Comparer1 = "IExecuteReaderExtensions.ToHashSetAsync(IExecuteReader,IEqualityComparer<Int32>,CancellationToken)";

			Binds<IExecuteReader>(Token1, "ToHashSetAsync", e => e.ToHashSetAsync<int>());
			Binds<IExecuteReader>(Token1, "ToHashSetAsync", e => e.ToHashSetAsync<int>(Token));
			Binds<IExecuteReader>(Token1, "ToHashSetAsync", e => e.ToHashSetAsync<int>(default));
			Binds<IExecuteReader>(Comparer1, "ToHashSetAsync", e => e.ToHashSetAsync<int>(null));
			Binds<IExecuteReader>(Comparer1, "ToHashSetAsync", e => e.ToHashSetAsync(IntComparer));
			Binds<IExecuteReader>(Comparer1, "ToHashSetAsync", e => e.ToHashSetAsync(IntComparer, Token));
		}

		// ---- ToDictionaryAsync (key): (key, token) and (key, comparer, token) ----

		static void DictionaryLadder<T>(string type, Action<T> none, Action<T> token, Action<T> defaulted, Action<T> comparerNull, Action<T> comparer, Action<T> both, string extension = null)
		{
			string tokenRung = extension == null
				? type + ".ToDictionaryAsync(Func<Int32,Int32>,CancellationToken)"
				: extension + ".ToDictionaryAsync(" + type + ",Func<Int32,Int32>,CancellationToken)";
			string comparerRung = extension == null
				? type + ".ToDictionaryAsync(Func<Int32,Int32>,IEqualityComparer<Int32>,CancellationToken)"
				: extension + ".ToDictionaryAsync(" + type + ",Func<Int32,Int32>,IEqualityComparer<Int32>,CancellationToken)";

			Binds(tokenRung, "ToDictionaryAsync", none);
			Binds(tokenRung, "ToDictionaryAsync", token);
			Binds(tokenRung, "ToDictionaryAsync", defaulted);
			Binds(comparerRung, "ToDictionaryAsync", comparerNull);
			Binds(comparerRung, "ToDictionaryAsync", comparer);
			Binds(comparerRung, "ToDictionaryAsync", both);
		}

		[Fact]
		public static void ToDictionaryAsyncByKey_OnEveryView_BindsEachRung()
		{
			DictionaryLadder<IDataReader>("MappedReader<Int32>",
				r => r.Map<int>().ToDictionaryAsync(Key), r => r.Map<int>().ToDictionaryAsync(Key, Token), r => r.Map<int>().ToDictionaryAsync(Key, default),
				r => r.Map<int>().ToDictionaryAsync(Key, null), r => r.Map<int>().ToDictionaryAsync(Key, IntComparer), r => r.Map<int>().ToDictionaryAsync(Key, IntComparer, Token));
			DictionaryLadder<IDbCommand>("MappedCommand<Int32>",
				c => c.Map<int>().ToDictionaryAsync(Key), c => c.Map<int>().ToDictionaryAsync(Key, Token), c => c.Map<int>().ToDictionaryAsync(Key, default),
				c => c.Map<int>().ToDictionaryAsync(Key, null), c => c.Map<int>().ToDictionaryAsync(Key, IntComparer), c => c.Map<int>().ToDictionaryAsync(Key, IntComparer, Token));
			DictionaryLadder<DataTable>("MappedTable<Int32>",
				t => t.Map<int>().ToDictionaryAsync(Key), t => t.Map<int>().ToDictionaryAsync(Key, Token), t => t.Map<int>().ToDictionaryAsync(Key, default),
				t => t.Map<int>().ToDictionaryAsync(Key, null), t => t.Map<int>().ToDictionaryAsync(Key, IntComparer), t => t.Map<int>().ToDictionaryAsync(Key, IntComparer, Token));
			DictionaryLadder<IExecuteReader>("MappedQuery<Int32>",
				e => e.Map<int>().ToDictionaryAsync(Key), e => e.Map<int>().ToDictionaryAsync(Key, Token), e => e.Map<int>().ToDictionaryAsync(Key, default),
				e => e.Map<int>().ToDictionaryAsync(Key, null), e => e.Map<int>().ToDictionaryAsync(Key, IntComparer), e => e.Map<int>().ToDictionaryAsync(Key, IntComparer, Token));
			DictionaryLadder<IExecuteReader>("MappedQueryWithSkip<Int32>",
				e => e.Map<int>().Skip(1).ToDictionaryAsync(Key), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Token), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, default),
				e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, null), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, IntComparer), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, IntComparer, Token));
			DictionaryLadder<IExecuteReader>("MappedQueryWithTake<Int32>",
				e => e.Map<int>().Take(1).ToDictionaryAsync(Key), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Token), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, default),
				e => e.Map<int>().Take(1).ToDictionaryAsync(Key, null), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, IntComparer), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, IntComparer, Token));
		}

		[Fact]
		public static void ToDictionaryAsyncByKey_OnTheInterface_BindsEachRung()
			=> DictionaryLadder<IMappedQuery<int>>("IMappedQuery<Int32>",
				q => q.ToDictionaryAsync(Key), q => q.ToDictionaryAsync(Key, Token), q => q.ToDictionaryAsync(Key, default),
				q => q.ToDictionaryAsync(Key, null), q => q.ToDictionaryAsync(Key, IntComparer), q => q.ToDictionaryAsync(Key, IntComparer, Token),
				"MappedQueryExtensions");

		// ---- ToDictionaryAsync (key and value): (key, value, token) and (key, value, comparer, token) ----

		static void DictionaryValueLadder<T>(string type, Action<T> none, Action<T> token, Action<T> defaulted, Action<T> comparerNull, Action<T> comparer, Action<T> both, string extension = null)
		{
			string tokenRung = extension == null
				? type + ".ToDictionaryAsync(Func<Int32,Int32>,Func<Int32,String>,CancellationToken)"
				: extension + ".ToDictionaryAsync(" + type + ",Func<Int32,Int32>,Func<Int32,String>,CancellationToken)";
			string comparerRung = extension == null
				? type + ".ToDictionaryAsync(Func<Int32,Int32>,Func<Int32,String>,IEqualityComparer<Int32>,CancellationToken)"
				: extension + ".ToDictionaryAsync(" + type + ",Func<Int32,Int32>,Func<Int32,String>,IEqualityComparer<Int32>,CancellationToken)";

			Binds(tokenRung, "ToDictionaryAsync", none);
			Binds(tokenRung, "ToDictionaryAsync", token);
			Binds(tokenRung, "ToDictionaryAsync", defaulted);
			Binds(comparerRung, "ToDictionaryAsync", comparerNull);
			Binds(comparerRung, "ToDictionaryAsync", comparer);
			Binds(comparerRung, "ToDictionaryAsync", both);
		}

		[Fact]
		public static void ToDictionaryAsyncByKeyAndValue_OnEveryView_BindsEachRung()
		{
			DictionaryValueLadder<IDataReader>("MappedReader<Int32>",
				r => r.Map<int>().ToDictionaryAsync(Key, Value), r => r.Map<int>().ToDictionaryAsync(Key, Value, Token), r => r.Map<int>().ToDictionaryAsync(Key, Value, default),
				r => r.Map<int>().ToDictionaryAsync(Key, Value, null), r => r.Map<int>().ToDictionaryAsync(Key, Value, IntComparer), r => r.Map<int>().ToDictionaryAsync(Key, Value, IntComparer, Token));
			DictionaryValueLadder<IDbCommand>("MappedCommand<Int32>",
				c => c.Map<int>().ToDictionaryAsync(Key, Value), c => c.Map<int>().ToDictionaryAsync(Key, Value, Token), c => c.Map<int>().ToDictionaryAsync(Key, Value, default),
				c => c.Map<int>().ToDictionaryAsync(Key, Value, null), c => c.Map<int>().ToDictionaryAsync(Key, Value, IntComparer), c => c.Map<int>().ToDictionaryAsync(Key, Value, IntComparer, Token));
			DictionaryValueLadder<DataTable>("MappedTable<Int32>",
				t => t.Map<int>().ToDictionaryAsync(Key, Value), t => t.Map<int>().ToDictionaryAsync(Key, Value, Token), t => t.Map<int>().ToDictionaryAsync(Key, Value, default),
				t => t.Map<int>().ToDictionaryAsync(Key, Value, null), t => t.Map<int>().ToDictionaryAsync(Key, Value, IntComparer), t => t.Map<int>().ToDictionaryAsync(Key, Value, IntComparer, Token));
			DictionaryValueLadder<IExecuteReader>("MappedQuery<Int32>",
				e => e.Map<int>().ToDictionaryAsync(Key, Value), e => e.Map<int>().ToDictionaryAsync(Key, Value, Token), e => e.Map<int>().ToDictionaryAsync(Key, Value, default),
				e => e.Map<int>().ToDictionaryAsync(Key, Value, null), e => e.Map<int>().ToDictionaryAsync(Key, Value, IntComparer), e => e.Map<int>().ToDictionaryAsync(Key, Value, IntComparer, Token));
			DictionaryValueLadder<IExecuteReader>("MappedQueryWithSkip<Int32>",
				e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value, Token), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value, default),
				e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value, null), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value, IntComparer), e => e.Map<int>().Skip(1).ToDictionaryAsync(Key, Value, IntComparer, Token));
			DictionaryValueLadder<IExecuteReader>("MappedQueryWithTake<Int32>",
				e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value, Token), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value, default),
				e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value, null), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value, IntComparer), e => e.Map<int>().Take(1).ToDictionaryAsync(Key, Value, IntComparer, Token));
		}

		[Fact]
		public static void ToDictionaryAsyncByKeyAndValue_OnTheInterface_BindsEachRung()
			=> DictionaryValueLadder<IMappedQuery<int>>("IMappedQuery<Int32>",
				q => q.ToDictionaryAsync(Key, Value), q => q.ToDictionaryAsync(Key, Value, Token), q => q.ToDictionaryAsync(Key, Value, default),
				q => q.ToDictionaryAsync(Key, Value, null), q => q.ToDictionaryAsync(Key, Value, IntComparer), q => q.ToDictionaryAsync(Key, Value, IntComparer, Token),
				"MappedQueryExtensions");

		// ---- ExecuteReaderAsync: (command, handler, token) and (command, handler, behavior, token) ----

		const string Ext = "CommandExtensions.ExecuteReaderAsync";

		static void BehaviorLadder<T>(string withoutBehavior, string withBehavior, Action<T> none, Action<T> token, Action<T> defaulted, Action<T> behavior, Action<T> both)
		{
			Binds(withoutBehavior, "ExecuteReaderAsync", none);
			Binds(withoutBehavior, "ExecuteReaderAsync", token);
			Binds(withoutBehavior, "ExecuteReaderAsync", defaulted);
			Binds(withBehavior, "ExecuteReaderAsync", behavior);
			Binds(withBehavior, "ExecuteReaderAsync", both);
		}

		[Fact]
		public static void ExecuteReaderAsync_OnADbCommand_BindsEachRung()
		{
			BehaviorLadder<DbCommand>(
				Ext + "(DbCommand,Action<DbDataReader>,CancellationToken)", Ext + "(DbCommand,Action<DbDataReader>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DbHandler), c => c.ExecuteReaderAsync(DbHandler, Token), c => c.ExecuteReaderAsync(DbHandler, default),
				c => c.ExecuteReaderAsync(DbHandler, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DbHandler, CommandBehavior.SequentialAccess, Token));
			BehaviorLadder<DbCommand>(
				Ext + "(DbCommand,Func<DbDataReader,ValueTask>,CancellationToken)", Ext + "(DbCommand,Func<DbDataReader,ValueTask>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DbAsyncHandler), c => c.ExecuteReaderAsync(DbAsyncHandler, Token), c => c.ExecuteReaderAsync(DbAsyncHandler, default),
				c => c.ExecuteReaderAsync(DbAsyncHandler, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DbAsyncHandler, CommandBehavior.SequentialAccess, Token));
			BehaviorLadder<DbCommand>(
				Ext + "(DbCommand,Func<DbDataReader,Int32>,CancellationToken)", Ext + "(DbCommand,Func<DbDataReader,Int32>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DbTransform), c => c.ExecuteReaderAsync(DbTransform, Token), c => c.ExecuteReaderAsync(DbTransform, default),
				c => c.ExecuteReaderAsync(DbTransform, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DbTransform, CommandBehavior.SequentialAccess, Token));
			BehaviorLadder<DbCommand>(
				Ext + "(DbCommand,Func<DbDataReader,ValueTask<Int32>>,CancellationToken)", Ext + "(DbCommand,Func<DbDataReader,ValueTask<Int32>>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DbAsyncTransform), c => c.ExecuteReaderAsync(DbAsyncTransform, Token), c => c.ExecuteReaderAsync(DbAsyncTransform, default),
				c => c.ExecuteReaderAsync(DbAsyncTransform, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DbAsyncTransform, CommandBehavior.SequentialAccess, Token));
		}

		[Fact]
		public static void ExecuteReaderAsync_OnAnIDbCommand_BindsEachRung()
		{
			BehaviorLadder<IDbCommand>(
				Ext + "(IDbCommand,CancellationToken)", Ext + "(IDbCommand,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(), c => c.ExecuteReaderAsync(Token), c => c.ExecuteReaderAsync(default),
				c => c.ExecuteReaderAsync(CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(CommandBehavior.SequentialAccess, Token));
			BehaviorLadder<IDbCommand>(
				Ext + "(IDbCommand,Func<IDataReader,ValueTask>,CancellationToken)", Ext + "(IDbCommand,Func<IDataReader,ValueTask>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DataAsyncHandler), c => c.ExecuteReaderAsync(DataAsyncHandler, Token), c => c.ExecuteReaderAsync(DataAsyncHandler, default),
				c => c.ExecuteReaderAsync(DataAsyncHandler, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DataAsyncHandler, CommandBehavior.SequentialAccess, Token));
			BehaviorLadder<IDbCommand>(
				Ext + "(IDbCommand,Func<IDataReader,ValueTask<Int32>>,CancellationToken)", Ext + "(IDbCommand,Func<IDataReader,ValueTask<Int32>>,CommandBehavior,CancellationToken)",
				c => c.ExecuteReaderAsync(DataAsyncTransform), c => c.ExecuteReaderAsync(DataAsyncTransform, Token), c => c.ExecuteReaderAsync(DataAsyncTransform, default),
				c => c.ExecuteReaderAsync(DataAsyncTransform, CommandBehavior.SequentialAccess), c => c.ExecuteReaderAsync(DataAsyncTransform, CommandBehavior.SequentialAccess, Token));
		}

		// ---- ForEachAsync on a DbDataReader: (handler, token) next to the flag rung ----

		[Fact]
		public static void ForEachAsync_BindsTheTokenRung()
		{
			const string Token1 = "DataReaderExtensions.ForEachAsync(DbDataReader,Action<IDataRecord>,CancellationToken)";
			const string Flag = "DataReaderExtensions.ForEachAsync(DbDataReader,Action<IDataRecord>,Boolean,CancellationToken)";

			Binds<DbDataReader>(Flag, "ForEachAsync", r => r.ForEachAsync(Record));
			Binds<DbDataReader>(Token1, "ForEachAsync", r => r.ForEachAsync(Record, Token));
			Binds<DbDataReader>(Flag, "ForEachAsync", r => r.ForEachAsync(Record, false));
			Binds<DbDataReader>(Flag, "ForEachAsync", r => r.ForEachAsync(Record, true, Token));

			// A bare default in the second slot now binds the token rung (the flag rung is the one that takes a bool):
			// it reads with ReadAsync, as the call without the flag does, where default(bool) used to turn that off.
			Binds<DbDataReader>(Token1, "ForEachAsync", r => r.ForEachAsync(Record, default));
		}

		// A single bare default on a DbCommand receiver does not compile, with or without these rungs: it is ambiguous
		// between DbCommand's own ExecuteReaderAsync(CommandBehavior) and ExecuteReaderAsync(CancellationToken) (CS0121).
	}
}
