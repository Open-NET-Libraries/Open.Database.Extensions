namespace Open.Database.Extensions.Benchmarks;

// The checks of the NULL-rule ladder: the streaming rungs (SyntheticVariantsStream.cs) and the hand-written policy twins (SyntheticVariantsNull.cs). None of them is a benchmark.
public static partial class SyntheticChecks
{
	const string NullSql = SyntheticVariants.Sql;

	static T Run<T>(Task<T> task) => task.GetAwaiter().GetResult();

	// Every rung's rows, member by member, against V1's (and so the table's): the four streams at every width, the hand-written twins at width 20.
	// The benchmark methods return a checksum (Id + Big) for a stream, compared in VerifyBenchmarkPath; here the same streams are collected whole.
	static void VerifyNullLadder(Report report, SyntheticConnection conn, SyntheticTable table, int width, List<SynRow> v1, string where)
	{
		Compare(report, "V1 stream vs V1", where, v1, Run(SyntheticVariantsStream.Collect(conn.Command(NullSql).Map<SynRow>().Stream(Timeout.InfiniteTimeSpan))));
		ComparePolicyTwin(report, "V1 Throw stream vs V1", where, table, v1, Run(SyntheticVariantsStream.Collect(conn.Command(NullSql).Map<SynRowThrow>().Stream(Timeout.InfiniteTimeSpan))));
		ComparePolicyTwin(report, "V1 Default stream vs V1", where, table, v1, Run(SyntheticVariantsStream.Collect(conn.Command(NullSql).Map<SynRowDefault>().Stream(Timeout.InfiniteTimeSpan))));
		Compare(report, "V2 stream vs V1", where, v1, Run(SyntheticVariantsStream.Collect(conn.Command(NullSql).Map(SyntheticVariants.Hand(width)).Stream(Timeout.InfiniteTimeSpan))));
		report.Check(Wait(SyntheticVariantsStream.V1(conn)) == SyntheticVariantsStream.Expected(v1), $"V1 stream checksum {where}");

		if (width != 20) return;
		ComparePolicyTwin(report, "V2 Throw (hand-written) vs V1", where, table, v1, SyntheticVariantsNull.V2Throw(conn));
		ComparePolicyTwin(report, "V2 Default (hand-written) vs V1", where, table, v1, SyntheticVariantsNull.V2Default(conn));
	}

	// A NULL in a column the model does not allow: Throw (runtime, hand-written, streamed) names the member; Default (runtime and hand-written) keeps the member's
	// default and reads every other member. The Title column of the catalog has no NULLs, so the table is changed here: the Title of row 1 is NULL.
	static void VerifyNullPolicyOnNull(Report report)
	{
		var plain = SyntheticTable.Create(20, 5);
		var nulls = (bool[]?[])plain.Nulls.Clone();
		nulls[2] = [false, true, false, false, false];
		var table = new SyntheticTable { Width = plain.Width, Rows = plain.Rows, Names = plain.Names, Types = plain.Types, Columns = plain.Columns, Nulls = nulls };
		var conn = new SyntheticConnection(table, new SyntheticOptions());

		KeyValuePair<string, Action>[] throwing =
		[
			new("runtime Throw", () => _ = conn.Command(NullSql).Map<SynRowThrow>().ToList()),
			new("hand-written Throw", () => _ = SyntheticVariantsNull.V2Throw(conn)),
			new("runtime Throw, streamed", () => _ = Run(SyntheticVariantsStream.Collect(conn.Command(NullSql).Map<SynRowThrow>().Stream(Timeout.InfiniteTimeSpan)))),
		];
		foreach (var form in throwing)
		{
			string thrown = "(nothing thrown)";
			try { form.Value(); }
			catch (Exception ex) { thrown = string.Join(" <- ", WalkChain(ex).Select(static e => e.GetType().Name + ": " + e.Message)); }
			report.Check(thrown.Contains("ValueMappingException", StringComparison.Ordinal) && thrown.Contains("Title", StringComparison.Ordinal), $"{form.Key} with a NULL Title: expected a ValueMappingException naming Title, got {thrown}");
		}

		var expected = new List<SynRow>(5);
		for (int i = 0; i < 5; i++)
		{
			var row = SynRow.FromTable(plain, i);
			if (i == 1) row.Title = null;
			expected.Add(row);
		}

		ComparePolicyTwin(report, "runtime Default with a NULL Title", "width=20 rows=5", table, expected, conn.Command(NullSql).Map<SynRowDefault>().ToList());
		ComparePolicyTwin(report, "hand-written Default with a NULL Title", "width=20 rows=5", table, expected, SyntheticVariantsNull.V2Default(conn));
		Console.WriteLine("  ok  NULL in a non-nullable column: Throw (runtime, hand-written, streamed) names Title; Default (runtime, hand-written) keeps the default and reads the other members");
	}
}
