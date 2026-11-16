using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Channels;
using Xunit;

namespace Open.Database.Extensions.LegacyCompiler.Tests
{
	// The channel terminals of the four command-level typed views
	// are overload ladders: twelve rungs on each view, each ending in
	// CancellationToken cancellationToken = default.
	// The binding is read from the IL of each call (the parameter types
	// of the method it calls), at every language version this project
	// compiles with: C# 7.3 (net472), 9 (net8.0), 12 (net10.0)
	// and 13 (net11.0).
	// A call that becomes ambiguous (CS0121) fails the build.
	//
	// ToChannel(null) binds ToChannel(ChannelOptions, CancellationToken)
	// in this change, because the Channel<T> overload of the
	// owned-channel forms does not exist yet.
	// That change (A19b) makes ToChannel(null) CS0121 and must
	// update the expectation of that one call below.
	public static class MappedToChannelBindingTests
	{
		sealed class Item
		{
			public int Id { get; set; }
		}

		static bool IsRung(MethodBase method) => method.Name.StartsWith("ToChannel", StringComparison.Ordinal);

		static string Names(MethodBase method) => string.Join(
				",",
				method.GetParameters()
					.Select(p => p.ParameterType.Name)
					.ToArray());

		static string Bound<TView>(Func<TView, object> lambda)
		{
			var method = lambda.Method;
			byte[] il = method.GetMethodBody().GetILAsByteArray();
			string found = "?";
			for (int i = 0; i + 4 < il.Length; i++)
			{
				// call, callvirt
				if (il[i] != 0x28 && il[i] != 0x6F) continue;
				try
				{
					int token = BitConverter.ToInt32(il, i + 1);
					var called = method.Module.ResolveMethod(token);
					if (called == null || !IsRung(called)) continue;
					found = Names(called);
				}
				catch (ArgumentException) { }
			}

			return found;
		}

		static readonly ChannelOptions Options = new BoundedChannelOptions(2);
		static readonly CancellationToken Ct;

		const string R1 = "CancellationToken";
		const string R2 = "ChannelOptions,CancellationToken";
		const string R3 = "Boolean,CancellationToken";
		const string R4 = "ChannelOptions,Boolean,CancellationToken";
		const string X1 = "Task`1&,CancellationToken";
		const string X3 = "Task`1&,Boolean,CancellationToken";
		const string X4 = "Task`1&,ChannelOptions,Boolean,CancellationToken";

		static readonly string[] Expected =
		{
			R1, R1, R2, R3, R3, R4, R3, R2, R2, R4,
			R1, R1, R2, R3, R3, R4, R3, R2, R1,
			X1, X1, X3, X4,
		};

		static string[] Query() => new[]
			{
				Bound((MappedQuery<Item> q) => q.ToChannelReader()),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(Options)),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(true)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannelReader(startImmediately: true)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannelReader(Options, true, Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(true, Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(Options, Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannelReader(null)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannelReader(Options, true)),
				Bound((MappedQuery<Item> q) => q.ToChannel()),
				Bound((MappedQuery<Item> q) => q.ToChannel(Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannel(Options)),
				Bound((MappedQuery<Item> q) => q.ToChannel(true)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannel(startImmediately: true)),
				Bound((MappedQuery<Item> q) => q.ToChannel(Options, true, Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannel(true, Ct)),
				Bound((MappedQuery<Item> q) => q.ToChannel(null)),
				Bound((MappedQuery<Item> q) => q.ToChannel(default)),
				Bound((MappedQuery<Item> q) => q.ToChannel(out _)),
				Bound((MappedQuery<Item> q) => q.ToChannel(out _, Ct)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannel(out _, startImmediately: true)),
				Bound((MappedQuery<Item> q)
					=> q.ToChannel(out _, Options, true, Ct)),
			};

		static string[] WithSkip() => new[]
			{
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader()),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(Ct)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(Options)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(true)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(startImmediately: true)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(Options, true, Ct)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(true, Ct)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(Options, Ct)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(null)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannelReader(Options, true)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel()),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(Ct)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(Options)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(true)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannel(startImmediately: true)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannel(Options, true, Ct)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(true, Ct)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(null)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(default)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(out _)),
				Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(out _, Ct)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannel(out _, startImmediately: true)),
				Bound((MappedQueryWithSkip<Item> q)
					=> q.ToChannel(out _, Options, true, Ct)),
			};

		static string[] WithTake() => new[]
			{
				Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader()),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(Ct)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(Options)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(true)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(startImmediately: true)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(Options, true, Ct)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(true, Ct)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(Options, Ct)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(null)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannelReader(Options, true)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel()),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(Ct)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(Options)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(true)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannel(startImmediately: true)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannel(Options, true, Ct)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(true, Ct)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(null)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(default)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(out _)),
				Bound((MappedQueryWithTake<Item> q) => q.ToChannel(out _, Ct)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannel(out _, startImmediately: true)),
				Bound((MappedQueryWithTake<Item> q)
					=> q.ToChannel(out _, Options, true, Ct)),
			};

		static string[] Command() => new[]
			{
				Bound((MappedCommand<Item> q) => q.ToChannelReader()),
				Bound((MappedCommand<Item> q) => q.ToChannelReader(Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannelReader(Options)),
				Bound((MappedCommand<Item> q) => q.ToChannelReader(true)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannelReader(startImmediately: true)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannelReader(Options, true, Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannelReader(true, Ct)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannelReader(Options, Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannelReader(null)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannelReader(Options, true)),
				Bound((MappedCommand<Item> q) => q.ToChannel()),
				Bound((MappedCommand<Item> q) => q.ToChannel(Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannel(Options)),
				Bound((MappedCommand<Item> q) => q.ToChannel(true)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannel(startImmediately: true)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannel(Options, true, Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannel(true, Ct)),
				Bound((MappedCommand<Item> q) => q.ToChannel(null)),
				Bound((MappedCommand<Item> q) => q.ToChannel(default)),
				Bound((MappedCommand<Item> q) => q.ToChannel(out _)),
				Bound((MappedCommand<Item> q) => q.ToChannel(out _, Ct)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannel(out _, startImmediately: true)),
				Bound((MappedCommand<Item> q)
					=> q.ToChannel(out _, Options, true, Ct)),
			};

		[Fact]
		public static void Query_EveryCallBindsThePredictedRung(
			) => Assert.Equal(Expected, Query());

		[Fact]
		public static void Skip_EveryCallBindsThePredictedRung(
			) => Assert.Equal(Expected, WithSkip());

		[Fact]
		public static void Take_EveryCallBindsThePredictedRung(
			) => Assert.Equal(Expected, WithTake());

		[Fact]
		public static void Command_EveryCallBindsThePredictedRung(
			) => Assert.Equal(Expected, Command());
	}
}
