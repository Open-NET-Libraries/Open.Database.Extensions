using System.Collections;
using System.Reflection;
using Open.Database.Extensions.Benchmarks.Generated;

namespace Open.Database.Extensions.Benchmarks;

// Gen and GenD (SG.md 5f): the prototype source generator, measured beside V2 (the hand-written selector).
//
//   Gen   command.Map<GenRowN>().ToList(): an undecorated twin of SynRow's first N members, matched by name, read by the generated row reader.
//   GenD  command.Map<GenRowDN>().ToList(): the decorated twin ([Column(Name, Order)] on every member), read at constant ordinals.
//
// Gen and GenD against V2: not like-for-like even at width 20. GenRow20 and GenRowD20 declare Title, Body, Tag and Blob nullable, so the generated reader
// tests those four for NULL where V2 reads them with no test. The like-for-like bars are Gen_T and Ctl_T (the non-nullable twin) against V2, and Gen against Ctl.
// The twins of widths 1, 3, 5 and 10 have only that many members, while V2's Hand(width) builds a full SynRow, so at those widths the twin also allocates less
// (fewer members) and nothing compares with V2 in time or in allocation.
//
// The intercepted call sites are in Open.Database.Extensions.Benchmarks.Generated (GeneratedReads): the generator intercepts every plain Map<T>()
// of the project it runs in, and this project's V1 and the other runtime variants must stay the runtime map. The command is built here, as V2 builds it.
public static class SyntheticGenerated
{
	public static object Gen(SyntheticConnection connection, int width)
		=> GeneratedReads.Gen(connection.Command(SyntheticVariants.Sql), width);

	public static object GenD(SyntheticConnection connection, int width)
		=> GeneratedReads.GenD(connection.Command(SyntheticVariants.Sql), width);

	/// <summary>The control of Gen at width 20: the selector the generator would write for Gen's twin (its NULL tests included), written by hand in the generated project's assembly.</summary>
	public static object Ctl(SyntheticConnection connection)
		=> GeneratedControl.Control20(connection.Command(SyntheticVariants.Sql));

	/// <summary>Gen_T (width 20): the twin whose Title, Body and Tag are non-nullable, as V2 assumes (read alone, as V2 reads them); its Blob is tested for NULL, which V2's is not.</summary>
	public static object GenT(SyntheticConnection connection)
		=> GeneratedReads.GenT20(connection.Command(SyntheticVariants.Sql));

	/// <summary>The control of Gen_T: the hand-written selector of Gen_T's twin (the same Blob test as the generator makes), compiled beside it.</summary>
	public static object CtlT(SyntheticConnection connection)
		=> GeneratedControl.ControlT20(connection.Command(SyntheticVariants.Sql));
}

public static partial class SyntheticChecks
{
	// Gen and GenD: the same rows as V2, member by member (the twins carry SynRow's member names and types), and the calls were intercepted.
	static void VerifyGenerated(Report report, SyntheticConnection conn, int width, List<SynRow> v2, string where)
	{
		int sites = GeneratedReads.InterceptedCallSites();
		report.Check(sites == 11, $"Gen/GenD: {sites} intercepted call sites, expected the 11 of GeneratedReads (is the generator referenced there?)");

		// And none here: V1 (the baseline) and every other runtime variant of this project stay the runtime map.
		bool interceptedHere = typeof(SyntheticChecks).Assembly.GetTypes().Any(static t => t.Name.EndsWith("OdxInterceptors", StringComparison.Ordinal));
		report.Check(!interceptedHere, "the Benchmarks assembly carries generated interceptors: its runtime variants would not be the runtime map");
		CompareGenerated(report, "Gen vs V2", where, v2, (IList)SyntheticGenerated.Gen(conn, width));
		CompareGenerated(report, "GenD vs V2", where, v2, (IList)SyntheticGenerated.GenD(conn, width));

		// The control and the decomposition (GenParity.cs) read the 20 catalog columns at fixed ordinals: width 20 only.
		if (width != 20) return;

		CompareGenerated(report, "Ctl vs V2", where, v2, (IList)SyntheticGenerated.Ctl(conn));
		CompareGenerated(report, "Gen_T vs V2", where, v2, (IList)SyntheticGenerated.GenT(conn));
		CompareGenerated(report, "Ctl_T vs V2", where, v2, (IList)SyntheticGenerated.CtlT(conn));
		KeyValuePair<string, Func<List<SynRow>>>[] parity =
		[
			new("V2n vs V2", () => SyntheticVariants.V2(conn, GenParity.V2n)),
			new("GP2 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP2)),
			new("GP3 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP3)),
			new("GP4 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP4())),
			new("GP5 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP5)),
			new("GP6 vs V2", () => SyntheticVariants.V2(conn, GenParity.GP6)),
			new("GP6r vs V2", () => SyntheticVariants.V2(conn, GenParity.GP6r)),
			new("GP7 vs V2", () => GenParity.GP7(conn)),
			new("GP7c vs V2", () => GenParity.GP7c(conn)),
		];
		foreach (var variant in parity)
			Compare(report, variant.Key, where, v2, variant.Value());
	}

	// The benchmark methods that read the 20 catalog columns at fixed ordinals (GenParity.cs, GeneratedControl.cs): they run at width 20 only.
	static bool WidthTwentyOnly(string method)
		=> method is "Ctl_Sync" or "Ctl_T_Sync" or "Gen_T_Sync" or "V2n_Sync" || method.StartsWith("GP", StringComparison.Ordinal);

	// The NULL policy twins (SynRowPolicy.cs) against V1, member by member. A narrower table has no column for some members: SynRow leaves a reference-typed
	// one null, a twin keeps its initializer ("" or an empty array). That tolerance holds ONLY for a member whose column is missing from the table (not in
	// table.Names) while V1 has null and the twin has exactly its fresh initializer. A member whose column is present is compared strictly at every width:
	// at width 20 (every column present) nothing is tolerated.
	static void ComparePolicyTwin(Report report, string name, string where, SyntheticTable table, List<SynRow> expected, IList actual)
	{
		if (actual.Count != expected.Count)
		{
			report.Fail($"{name} {where}: {actual.Count} rows, expected {expected.Count}");
			return;
		}

		for (int i = 0; i < expected.Count; i++)
		{
			object row = actual[i]!;
			object fresh = Activator.CreateInstance(row.GetType())!;
			foreach (PropertyInfo p in row.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
			{
				object? want = typeof(SynRow).GetProperty(p.Name)!.GetValue(expected[i]);
				object? got = p.GetValue(row);
				bool absent = !table.Names.Contains(p.Name, StringComparer.OrdinalIgnoreCase);
				if (SynRow.Same(want, got) || absent && want is null && SynRow.Same(p.GetValue(fresh), got)) continue;
				report.Fail($"{name} {where} row {i}, {p.Name}: expected {want ?? "null"}, actual {got ?? "null"}");
				return;
			}
		}

		report.Check(true, name);
	}

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
