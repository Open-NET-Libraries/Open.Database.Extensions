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
}

public static partial class SyntheticChecks
{
	// Gen and GenD: the same rows as V2, member by member (the twins carry SynRow's member names and types), and the calls were intercepted.
	static void VerifyGenerated(Report report, SyntheticConnection conn, int width, List<SynRow> v2, string where)
	{
		int sites = GeneratedReads.InterceptedCallSites();
		report.Check(sites == 10, $"Gen/GenD: {sites} intercepted call sites, expected the 10 of GeneratedReads (is the generator referenced there?)");
		CompareGenerated(report, "Gen vs V2", where, v2, (IList)SyntheticGenerated.Gen(conn, width));
		CompareGenerated(report, "GenD vs V2", where, v2, (IList)SyntheticGenerated.GenD(conn, width));
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
