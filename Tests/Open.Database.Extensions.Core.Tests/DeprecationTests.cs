using Open.Database.Extensions.Core;
using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The members that are obsolete in 11.0 (removed in 12) each say what to type instead.
// The first test is the guard that a member never ships obsolete without naming its alternative.
// The others show that the alternative gives the same rows as the old call for the common case, and pin the documented differences.
[ExcludeFromCodeCoverage]
public static class DeprecationTests
{
	sealed class Thing
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public string? Note { get; set; }
	}

	static FakeExecuteReader Command(int count)
		=> new(() => ReaderFixtures.Reader(ReaderFixtures.Rows(count)));

	static string Show(IEnumerable<Thing> things) => string.Join(",", things.Select(t => $"{t.Id}:{t.Name}"));

	static bool IsFunc(Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Func<,>);

	static IEnumerable<MethodInfo> Of(Type type, string name, Func<MethodInfo, bool>? where = null)
		=> type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly)
			.Where(m => m.Name == name && (where is null || where(m)));

	static IEnumerable<MethodInfo> OnReader(string name, bool selector)
		=> Of(typeof(IExecuteReaderExtensions), name, m =>
		{
			ParameterInfo[] p = m.GetParameters();
			return p[0].ParameterType == typeof(IExecuteReader)
				&& p.Any(x => IsFunc(x.ParameterType)) == selector;
		});

	// (group, members, text every message must contain)
	static IEnumerable<(string Group, MemberInfo[] Members, string[] Contains)> Table()
	{
		Type command = typeof(ExpressiveDbCommandBase<,,,,>);

		yield return ("1a Results<T>(IExecuteReader)", [.. OnReader("Results", false)], [".Map<T>().ToList()", ".Map(map).ToList()", ".SingleResult()"]);
		yield return ("1b/1c ResultsAsync<T>", [.. Of(command, "ResultsAsync")], [".Map<T>().ToListAsync()", ".Map(map).ToListAsync()", ".SingleResult()"]);
		yield return ("2 ResultsFromDataTable<T>", [.. Of(typeof(CoreExtensions), "ResultsFromDataTable")], [".ToDataTable()", ".To<T>(map, true)", "RowMap<T>.Default"]);
		yield return ("3a FirstOrDefault(selector)", [.. OnReader("FirstOrDefault", true)], [".Map(selector).FirstOrDefault()"]);
		yield return ("3b SingleOrDefault(selector)", [.. OnReader("SingleOrDefault", true)], [".Map(selector).SingleOrDefault()"]);
		yield return ("3c Take(selector)", [.. OnReader("Take", true)], [".Map(selector).Take(count).ToList()", "Take(0) now runs the command"]);
		yield return ("3d Skip(selector)", [.. OnReader("Skip", true)], [".Map(selector).Skip(count).ToList()", "does not run the selector on skipped rows"]);
		yield return ("3e SkipThenTake(selector)", [.. OnReader("SkipThenTake", true)], [".Map(selector).Skip(skip).Take(take).ToList()", "Take(0) now runs the command"]);
		yield return ("4a TakeAsync", [.. Of(command, "TakeAsync")], [".Map(selector).Take(count).ToListAsync(cancellationToken)", "IterateReaderWhileAsync"]);
		yield return ("4b ToListAsync(selector)", [.. Of(command, "ToListAsync", m => m.GetParameters().Any(p => p.ParameterType == typeof(CommandBehavior)))], [".Map(selector).ToListAsync(cancellationToken)", "IterateReaderAsync"]);
#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		yield return ("5 QueryResultCollection", [typeof(QueryResultCollection<,>), typeof(QueryResultCollection<>)], ["no replacement", ".Map<T>().ToList()"]);
#pragma warning restore CS0618
		yield return ("6a AsMappedDictionaries", [.. Of(typeof(QueryResultExtensions), "AsMappedDictionaries", m => m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(QueryResult<>))], [".Retrieve().DequeueAsMappedDictionaries()"]);
		yield return ("6b/6c AsMappedDictionaries (async)", [.. Of(typeof(QueryResultExtensions), "AsMappedDictionaries", m => m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() != typeof(QueryResult<>))], ["(await cmd.RetrieveAsync()).DequeueAsMappedDictionaries()"]);
		yield return ("7a DequeueAs<T>", [.. Of(typeof(QueryResultExtensions), "DequeueAs", m => m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(QueryResult<>))], ["cmd.Map<T>().ToList()", "command.Results<T>()", "reader.ResultsBuffered<T>()"]);
		yield return ("7b-7e DequeueAs<T> (async)", [.. Of(typeof(QueryResultExtensions), "DequeueAs", m => m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() != typeof(QueryResult<>))], ["await cmd.Map<T>().ToListAsync()", "(await cmd.RetrieveAsync()).DequeueAs<T>()", "await command.ResultsAsync<T>()"]);
	}

	[Fact]
	public static void EveryObsoleteMember_IsAWarning_NamesItsReplacement_AndSaysWhenItGoes()
	{
		int members = 0;
		foreach ((string group, MemberInfo[] list, string[] contains) in Table())
		{
			Assert.NotEmpty(list);
			foreach (MemberInfo member in list)
			{
				members++;
				ObsoleteAttribute obsolete = member.GetCustomAttribute<ObsoleteAttribute>()
					?? throw new Xunit.Sdk.XunitException($"{group}: {member} is not [Obsolete].");
				Assert.False(obsolete.IsError, $"{group}: {member} must be a warning, not an error.");
				string message = obsolete.Message ?? string.Empty;
				Assert.EndsWith("Removed in 12.", message);
				foreach (string text in contains)
					Assert.True(message.Contains(text, StringComparison.Ordinal), $"{group}: {member} does not say \"{text}\".");
			}
		}

		Assert.Equal(22, members); // 3 + 2 + 5 + 2 + 2 + 3 + 5
	}

	// ---- group 1 ----

	[Fact]
	public static void Results_AndMapToList_GiveTheSameRows()
	{
		FakeExecuteReader command = Command(3);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		string old = Show(command.Results<Thing>());
#pragma warning restore CS0618
		string replacement = Show(command.Map<Thing>().ToList());

		Assert.Equal("1:n1,2:n2,3:n3", old);
		Assert.Equal(old, replacement);
	}

	[Fact]
	public static void ANullMap_IsAcceptedByResults_ButNotByMap()
	{
		FakeExecuteReader command = Command(1);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Single(command.Results((RowMap<Thing>?)null));
#pragma warning restore CS0618
		Assert.Throws<ArgumentNullException>(() => command.Map((RowMap<Thing>)null!));
	}

	// ---- group 2 ----

	[Fact]
	public static void ResultsFromDataTable_AndToDataTableThenTo_GiveTheSameRows()
	{
		using var source = new DataTable();
		source.Columns.Add("Id", typeof(int));
		source.Columns.Add("Name", typeof(string));
		source.Rows.Add(1, "n1");
		source.Rows.Add(2, "n2");

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		string old = Show(source.CreateDataReader().ResultsFromDataTable<Thing>());
#pragma warning restore CS0618
		using var table = source.CreateDataReader().ToDataTable();
		string replacement = Show(table.To<Thing>(null, true).ToList());

		Assert.Equal("1:n1,2:n2", old);
		Assert.Equal(old, replacement);
	}

	// ---- group 3 ----

	[Fact]
	public static void TheSelectorForms_AndTheirMapForms_GiveTheSameRows()
	{
		static int Id(IDataRecord record) => record.GetInt32(0);
		FakeExecuteReader command = Command(5);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Equal(command.Map(Id).FirstOrDefault(), command.FirstOrDefault(Id));
		Assert.Equal(command.Map(Id).Take(2).ToList(), command.Take(2, Id));
		Assert.Equal(command.Map(Id).Skip(3).ToList(), command.Skip(3, Id));
		Assert.Equal(command.Map(Id).Skip(1).Take(3).ToList(), command.SkipThenTake(1, 3, Id));
		Assert.Equal(Command(1).Map(Id).SingleOrDefault(), Command(1).SingleOrDefault(Id));
#pragma warning restore CS0618
	}

	[Fact]
	public static void MapDoesNotRunTheSelectorOnASkippedRow_ButSkipDid()
	{
		int calls = 0;
		int Counting(IDataRecord record)
		{
			calls++;
			return record.GetInt32(0);
		}

		FakeExecuteReader command = Command(3);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		command.Skip(1, Counting);
#pragma warning restore CS0618
		int oldCalls = calls;
		calls = 0;
		command.Map(Counting).Skip(1).ToList();

		Assert.Equal((3, 2), (oldCalls, calls));

		// So a selector that throws on a skipped row failed the old call and no longer fails the new one.
		var seen = new List<int>();
		int ThrowsOnId1(IDataRecord record)
		{
			int id = record.GetInt32(0);
			seen.Add(id);
			return id == 1 ? throw new InvalidOperationException() : id;
		}

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Exception? thrown = Record.Exception(() => command.Skip(1, ThrowsOnId1));
#pragma warning restore CS0618
		Assert.IsType<InvalidOperationException>(thrown);
		Assert.Equal([1], seen);
		Assert.Equal([2, 3], command.Map(ThrowsOnId1).Skip(1).ToList());
	}

	[Fact]
	public static void ATakeOfZero_RunsTheCommandThroughMap_ButDidNotThroughTake()
	{
		static int Id(IDataRecord record) => record.GetInt32(0);
		var old = Command(3);
		var replacement = Command(3);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Empty(old.Take(0, Id));
#pragma warning restore CS0618
		Assert.Empty(replacement.Map(Id).Take(0).ToList());

		Assert.Equal(0, old.ExecuteCount);
		Assert.Equal(1, replacement.ExecuteCount);
	}

	// ---- group 6 ----

	[Fact]
	public static void AsMappedDictionaries_AndDequeueAsMappedDictionaries_GiveTheSameDictionaries()
	{
		static object?[][] Raw() => [[1, "a"], [2, DBNull.Value]];
		string[] names = ["Id", "Name"];

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		var old = new QueryResult<IEnumerable<object?[]>>([0, 1], names, Raw()).AsMappedDictionaries().ToList();
#pragma warning restore CS0618
		var replacement = new QueryResult<Queue<object[]>>([0, 1], names, new Queue<object[]>(Raw()!)).DequeueAsMappedDictionaries().ToList();

		Assert.Equal(2, old.Count);
		Assert.Equal(old.Count, replacement.Count);
		for (int i = 0; i < old.Count; i++)
			Assert.Equal(old[i].OrderBy(p => p.Key), replacement[i].OrderBy(p => p.Key));
		Assert.Null(replacement[1]["Name"]);
	}

	// ---- group 7 ----

	[Fact]
	public static void RetrieveThenDequeueAs_AndMapToList_GiveTheSameRows()
	{
		FakeExecuteReader command = Command(3);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		string old = Show(command.Retrieve().DequeueAs<Thing>());
#pragma warning restore CS0618
		string replacement = Show(command.Map<Thing>().ToList());

		Assert.Equal("1:n1,2:n2,3:n3", old);
		Assert.Equal(old, replacement);
	}

	[Fact]
	public static void ANullMap_IsAcceptedByDequeueAs_ButNotByMap()
	{
		FakeExecuteReader command = Command(1);

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		Assert.Single(command.Retrieve().DequeueAs<Thing>(null));
#pragma warning restore CS0618
		Assert.Throws<ArgumentNullException>(() => command.Map((RowMap<Thing>)null!));
	}
}
