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
	// The bool throwOnCancellation overloads of
	// ForEach, IterateWhile, Select and SelectAsync are removed (A11).
	// The binding is read from the IL of each call
	// (the signature of the method it calls),
	// so a change of binding fails here,
	// and a call that becomes ambiguous (CS0121) fails the build.
	// Every target runs the calls at its language version:
	// C# 7.3 (net472), 9 (net8.0), 12 (net10.0) and 13 (net11.0).
	// A call that passed the flag must no longer compile
	// and cannot be a test.
	public static class ReaderCancellationBindingTests
	{
		static readonly Action<IDataRecord> Handler = _ => { };
		static readonly Func<IDataRecord, bool> Predicate = _ => true;
		static readonly Func<IDataRecord, int> Transform = _ => 1;
		static readonly Func<IDataRecord, ValueTask<int>> AsyncTransform
			= _ => new ValueTask<int>(1);
		static readonly CancellationToken Token = new CancellationToken();

		static string Name(Type type)
		{
			if (!type.IsGenericType) return type.Name;

			var names = new List<string>();
			foreach (Type argument in type.GetGenericArguments())
				names.Add(Name(argument));

			return type.Name.Split('`')[0]
				+ "<"
				+ string.Join(",", names)
				+ ">";
		}

		static string Rest(ParameterInfo[] parameters)
		{
			var names = new List<string>();
			for (int i = 1; i < parameters.Length; i++)
				names.Add(Name(parameters[i].ParameterType));

			return string.Join(",", names);
		}

		static string Bound(Delegate lambda, string name)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				if (il[i] != 0x28) continue; // call
				try
				{
					var called = method.Module.ResolveMethod(
						BitConverter.ToInt32(il, i + 1));
					if (called == null || called.Name != name) continue;
					var parameters = called.GetParameters();
					found = Name(parameters[0].ParameterType)
						+ ":"
						+ Rest(parameters);
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		static void Binds(
			string expected,
			string name,
			Action<IDataReader> call)
			=> Assert.Equal(expected, Bound(call, name));

		static void BindsDb(
			string expected,
			string name,
			Action<DbDataReader> call)
			=> Assert.Equal(expected, Bound(call, name));

		const string ForEachSignature
			= "IDataReader:Action<IDataRecord>,CancellationToken";

		const string IterateWhileSignature
			= "IDataReader:Func<IDataRecord,Boolean>,CancellationToken";

		const string SelectSignature
			= "IDataReader:Func<IDataRecord,Int32>";

		const string SelectTokenSignature
			= "IDataReader:Func<IDataRecord,Int32>,CancellationToken";

		const string SelectTokenFirstSignature
			= "IDataReader:CancellationToken,Func<IDataRecord,Int32>";

		const string SelectAsyncDbSignature
			= "DbDataReader:Func<IDataRecord,Int32>,CancellationToken";

		const string SelectAsyncValueTaskSignature
			= "IDataReader:Func<IDataRecord,ValueTask<Int32>>,"
			+ "CancellationToken";

		[Fact]
		public static void ForEach_BindsTheTokenOverload()
		{
			Binds(ForEachSignature, "ForEach", r => r.ForEach(Handler));
			Binds(
				ForEachSignature,
				"ForEach",
				r => r.ForEach(Handler, Token));
			Binds(
				ForEachSignature,
				"ForEach",
				r => r.ForEach(Handler, default));
		}

		[Fact]
		public static void IterateWhile_BindsTheTokenOverload()
		{
			Binds(
				IterateWhileSignature,
				"IterateWhile",
				r => r.IterateWhile(Predicate));
			Binds(
				IterateWhileSignature,
				"IterateWhile",
				r => r.IterateWhile(Predicate, Token));
			Binds(
				IterateWhileSignature,
				"IterateWhile",
				r => r.IterateWhile(Predicate, default));
		}

		[Fact]
		public static void Select_BindsTheOverloadWithTheTokenWhereItIs()
		{
			Binds(SelectSignature, "Select", r => r.Select(Transform));
			Binds(
				SelectTokenSignature,
				"Select",
				r => r.Select(Transform, Token));
			Binds(
				SelectTokenFirstSignature,
				"Select",
				r => r.Select(Token, Transform));
			Binds(
				SelectTokenSignature,
				"Select",
				r => r.Select(Transform, default));
		}

		[Fact]
		public static void SelectAsync_BindsTheOverloadOfItsReceiver()
		{
			BindsDb(
				SelectAsyncDbSignature,
				"SelectAsync",
				r => r.SelectAsync(Transform));
			BindsDb(
				SelectAsyncDbSignature,
				"SelectAsync",
				r => r.SelectAsync(Transform, Token));
			BindsDb(
				SelectAsyncDbSignature,
				"SelectAsync",
				r => r.SelectAsync(Transform, default));

			Binds(
				SelectAsyncValueTaskSignature,
				"SelectAsync",
				r => r.SelectAsync(AsyncTransform));
			Binds(
				SelectAsyncValueTaskSignature,
				"SelectAsync",
				r => r.SelectAsync(AsyncTransform, Token));
			Binds(
				SelectAsyncValueTaskSignature,
				"SelectAsync",
				r => r.SelectAsync(AsyncTransform, default));
		}
	}
}
