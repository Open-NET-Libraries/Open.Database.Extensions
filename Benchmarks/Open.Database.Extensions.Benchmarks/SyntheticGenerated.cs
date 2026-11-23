using System.Collections;
using System.Reflection;
using Open.Database.Extensions.Benchmarks.Generated;

namespace Open.Database.Extensions.Benchmarks;

// Gen and GenD (SG.md 5f): the prototype source generator, measured beside V2 (the hand-written selector).
//
//   Gen   command.Map<GenRowN>().ToList(): an undecorated twin of SynRow's first N members, matched by name, read by the generated row reader.
//   GenD  command.Map<GenRowDN>().ToList(): the decorated twin ([Column(Name, Order)] on every member), read at constant ordinals.
//
// The intercepted call sites are in Open.Database.Extensions.Benchmarks.Generated (GeneratedReads): the generator intercepts every plain Map<T>()
// of the project it runs in, and this project's V1 and the other runtime variants must stay the runtime map. The command is built here, as V2 builds it.
public static class SyntheticGenerated
{
	public static object Gen(SyntheticConnection connection, int width)
		=> GeneratedReads.Gen(connection.Command(SyntheticVariants.Sql), width);

	public static object GenD(SyntheticConnection connection, int width)
		=> GeneratedReads.GenD(connection.Command(SyntheticVariants.Sql), width);

	/// <summary>The control of Gen at width 20: V2's selector copied into the generated project's assembly, on Gen's twin of the row.</summary>
	public static object Ctl(SyntheticConnection connection)
		=> GeneratedControl.Control20(connection.Command(SyntheticVariants.Sql));
}

public static partial class SyntheticChecks
{
	// Gen and GenD: the same rows as V2, member by member (the twins carry SynRow's member names and types), and the calls were intercepted.
	static void VerifyGenerated(Report report, SyntheticConnection conn, int width, List<SynRow> v2, string where)
	{
		int sites = GeneratedReads.InterceptedCallSites();
		report.Check(sites == 10, $"Gen/GenD: {sites} intercepted call sites, expected the 10 of GeneratedReads (is the generator referenced there?)");

		// And none here: V1 (the baseline) and every other runtime variant of this project stay the runtime map.
		bool interceptedHere = typeof(SyntheticChecks).Assembly.GetTypes().Any(static t => t.Name.EndsWith("OdxInterceptors", StringComparison.Ordinal));
		report.Check(!interceptedHere, "the Benchmarks assembly carries generated interceptors: its runtime variants would not be the runtime map");
		CompareGenerated(report, "Gen vs V2", where, v2, (IList)SyntheticGenerated.Gen(conn, width));
		CompareGenerated(report, "GenD vs V2", where, v2, (IList)SyntheticGenerated.GenD(conn, width));

		// The control and the decomposition (GenParity.cs) read the 20 catalog columns at fixed ordinals: width 20 only.
		if (width != 20) return;

		CompareGenerated(report, "Ctl vs V2", where, v2, (IList)SyntheticGenerated.Ctl(conn));
		KeyValuePair<string, Func<List<SynRow>>>[] parity =
		[
			new("V2n vs V2", () => SyntheticVariants.V2(conn, GenParity.V2n)),
			new("GP2 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP2)),
			new("GP3 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP3)),
			new("GP4 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP4())),
			new("GP5 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP5)),
		];
		foreach (var variant in parity)
			Compare(report, variant.Key, where, v2, variant.Value());
	}

	// The benchmark methods that read the 20 catalog columns at fixed ordinals (GenParity.cs, GeneratedControl.cs): they run at width 20 only.
	static bool WidthTwentyOnly(string method)
		=> method is "Ctl_Sync" or "V2n_Sync" || method.StartsWith("GP", StringComparison.Ordinal);

	static void CompareGenerated(Report report, string name, string where, List<SynRow> expected, IList actual)
	{
		if (actual.Count != expected.Count)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {expected.Count}");
			return;
		}

		for (int i = 0; i < expected.Count; i++)
		{
			object row = actual[i]!;
			foreach (PropertyInfo p in row.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
			{
				object? want = typeof(SynRow).GetProperty(p.Name)!.GetValue(expected[i]);
				object? got = p.GetValue(row);
				if (SynRow.Same(want, got)) continue;
				report.Fail($"{name} {where} row {i}, {p.Name}: expected {want ?? "null"}, actual {got ?? "null"}");
				return;
			}
		}

		report.Check(true, name);
	}
}
