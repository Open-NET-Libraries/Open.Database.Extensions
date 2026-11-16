using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The channel terminals of the four command-level typed views are
// overload ladders (twelve rungs on each view), and every rung ends in
// CancellationToken cancellationToken = default.
// The binding is read from the IL of each call (the parameter types of
// the method it calls), as in MapPairRemovalBindingTests,
// not from whether it compiles, so a change of binding fails here.
// The shapes that must not compile cannot be a test; the scratch
// compile of the ladder recorded them (ToChannel.api.md, 2b).
//
// ToChannel(null) binds ToChannel(ChannelOptions?, CancellationToken)
// in this change, because the Channel<T> overload of the owned-channel
// forms does not exist yet.
// That change (A19b) makes ToChannel(null) CS0121 (ambiguous)
// and must update the expectation of that one call below.
[ExcludeFromCodeCoverage]
public static class MappedToChannelBindingTests
{
	sealed class Item { public int Id { get; set; } }

	// The parameter types of the ToChannel or ToChannelReader
	// the lambda calls, by name, joined by commas.
	static bool IsRung(MethodBase method)
		=> method.Name.StartsWith("ToChannel", StringComparison.Ordinal);

	static string Names(MethodBase method)
		=> string.Join(
			",",
			method.GetParameters().Select(p => p.ParameterType.Name));

	static string Bound(Delegate lambda)
	{
		var method = lambda.Method;
		byte[] il = method.GetMethodBody()!.GetILAsByteArray()!;
		string found = "?";
		for (int i = 0; i + 4 < il.Length; i++)
		{
			// call, callvirt
			if (il[i] is not 0x28 and not 0x6F) continue;
			try
			{
				int token = BitConverter.ToInt32(il, i + 1);
				var called = method.Module.ResolveMethod(token);
				if (called is null || !IsRung(called)) continue;
				found = Names(called);
			}
			catch (ArgumentException) { }
		}

		return found;
	}

	static readonly ChannelOptions Options = new BoundedChannelOptions(2);
	static readonly CancellationToken Ct = default;

	const string R1 = "CancellationToken";
	const string R2 = "ChannelOptions,CancellationToken";
	const string R3 = "Boolean,CancellationToken";
	const string R4 = "ChannelOptions,Boolean,CancellationToken";
	const string X1 = "Task`1&,CancellationToken";
	const string X3 = "Task`1&,Boolean,CancellationToken";
	const string X4 = "Task`1&,ChannelOptions,Boolean,CancellationToken";

	// The expectation of each call, in the order of the lists below.
	static readonly string[] Expected =
	[
		// ToChannelReader: (), (ct), (options), (true),
		// (startImmediately: true), (options, true, ct), (true, ct),
		// (options, ct), (null), (options, true)
		R1, R1, R2, R3, R3, R4, R3, R2, R2, R4,
		// ToChannel: (), (ct), (options), (true),
		// (startImmediately: true), (options, true, ct), (true, ct),
		// (null), (default)
		R1, R1, R2, R3, R3, R4, R3, R2, R1,
		// ToChannel(out ...): (out rows), (out rows, ct),
		// (out rows, startImmediately: true),
		// (out rows, options, true, ct)
		X1, X1, X3, X4,
	];

	static string[] Query()
	=> [
		Bound((MappedQuery<Item> q) => q.ToChannelReader()),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(Options)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(true)),
		Bound((MappedQuery<Item> q)
			=> q.ToChannelReader(startImmediately: true)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(Options, true, Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(true, Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(Options, Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(null)),
		Bound((MappedQuery<Item> q) => q.ToChannelReader(Options, true)),
		Bound((MappedQuery<Item> q) => q.ToChannel()),
		Bound((MappedQuery<Item> q) => q.ToChannel(Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannel(Options)),
		Bound((MappedQuery<Item> q) => q.ToChannel(true)),
		Bound((MappedQuery<Item> q) => q.ToChannel(startImmediately: true)),
		Bound((MappedQuery<Item> q) => q.ToChannel(Options, true, Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannel(true, Ct)),
		Bound((MappedQuery<Item> q) => q.ToChannel(null)),
		Bound((MappedQuery<Item> q) => q.ToChannel(default)),
		Bound((MappedQuery<Item> q) => q.ToChannel(out _)),
		Bound((MappedQuery<Item> q) => q.ToChannel(out _, Ct)),
		Bound((MappedQuery<Item> q)
			=> q.ToChannel(out _, startImmediately: true)),
		Bound((MappedQuery<Item> q) => q.ToChannel(out _, Options, true, Ct)),
	];

	static string[] WithSkip()
	=> [
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader()),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(Options)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(true)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannelReader(startImmediately: true)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannelReader(Options, true, Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(true, Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(Options, Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannelReader(null)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannelReader(Options, true)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel()),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(Options)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(true)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannel(startImmediately: true)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(Options, true, Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(true, Ct)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(null)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(default)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(out _)),
		Bound((MappedQueryWithSkip<Item> q) => q.ToChannel(out _, Ct)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannel(out _, startImmediately: true)),
		Bound((MappedQueryWithSkip<Item> q)
			=> q.ToChannel(out _, Options, true, Ct)),
	];

	static string[] WithTake()
	=> [
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader()),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(Options)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(true)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannelReader(startImmediately: true)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannelReader(Options, true, Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(true, Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(Options, Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannelReader(null)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannelReader(Options, true)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel()),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(Options)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(true)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannel(startImmediately: true)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(Options, true, Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(true, Ct)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(null)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(default)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(out _)),
		Bound((MappedQueryWithTake<Item> q) => q.ToChannel(out _, Ct)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannel(out _, startImmediately: true)),
		Bound((MappedQueryWithTake<Item> q)
			=> q.ToChannel(out _, Options, true, Ct)),
	];

	static string[] Command()
	=> [
		Bound((MappedCommand<Item> q) => q.ToChannelReader()),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(Options)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(true)),
		Bound((MappedCommand<Item> q)
			=> q.ToChannelReader(startImmediately: true)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(Options, true, Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(true, Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(Options, Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(null)),
		Bound((MappedCommand<Item> q) => q.ToChannelReader(Options, true)),
		Bound((MappedCommand<Item> q) => q.ToChannel()),
		Bound((MappedCommand<Item> q) => q.ToChannel(Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannel(Options)),
		Bound((MappedCommand<Item> q) => q.ToChannel(true)),
		Bound((MappedCommand<Item> q) => q.ToChannel(startImmediately: true)),
		Bound((MappedCommand<Item> q) => q.ToChannel(Options, true, Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannel(true, Ct)),
		Bound((MappedCommand<Item> q) => q.ToChannel(null)),
		Bound((MappedCommand<Item> q) => q.ToChannel(default)),
		Bound((MappedCommand<Item> q) => q.ToChannel(out _)),
		Bound((MappedCommand<Item> q) => q.ToChannel(out _, Ct)),
		Bound((MappedCommand<Item> q)
			=> q.ToChannel(out _, startImmediately: true)),
		Bound((MappedCommand<Item> q) => q.ToChannel(out _, Options, true, Ct)),
	];

	[Fact]
	public static void Query_EveryCallBindsThePredictedRung()
		=> Assert.Equal(Expected, Query());

	[Fact]
	public static void Skip_EveryCallBindsThePredictedRung(
		)
		=> Assert.Equal(Expected, WithSkip());

	[Fact]
	public static void Take_EveryCallBindsThePredictedRung(
		)
		=> Assert.Equal(Expected, WithTake());

	[Fact]
	public static void Command_EveryCallBindsThePredictedRung()
		=> Assert.Equal(Expected, Command());

	[Fact]
	public static void EveryRung_EndsInATokenWithADefault()
	{
		foreach (Type view in new[]
		{
			typeof(MappedQuery<Item>), typeof(MappedQueryWithSkip<Item>),
			typeof(MappedQueryWithTake<Item>), typeof(MappedCommand<Item>),
		})
		{
			var rungs = view.GetMethods()
				.Where(m => m.Name is "ToChannel" or "ToChannelReader")
				.ToArray();

			Assert.Equal(12, rungs.Length);
			foreach (var rung in rungs)
			{
				var last = rung.GetParameters()[^1];
				Assert.Equal(typeof(CancellationToken), last.ParameterType);
				Assert.Equal("cancellationToken", last.Name);
				Assert.True(last.HasDefaultValue);
			}
		}
	}

	[Fact]
	public static void TheViewsThatHaveNoRungs_AreTheReaderAndTheTable()
	{
		Assert.DoesNotContain(typeof(MappedReader<Item>).GetMethods(), IsRung);
		Assert.DoesNotContain(typeof(MappedTable<Item>).GetMethods(), IsRung);
	}
}
