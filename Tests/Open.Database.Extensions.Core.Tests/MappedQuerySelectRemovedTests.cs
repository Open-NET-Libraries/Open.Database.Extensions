using System.Reflection;

#nullable enable

namespace Open.Database.Extensions.Tests;

// IMappedQuery<T>.Select (and the ProjectedQuery behind it) is
// removed: a mapped view is mapped to its result type with
// Map(RowMap<TResult>.From(factory)), or projected after a terminal.
// The reader-level IDataReader.Select / SelectAsync stay
// (the caller opened the reader).
[ExcludeFromCodeCoverage]
public static class MappedQuerySelectRemovedTests
{
	// A test cannot assert a compile error (CS1929 on
	// `view.Select(...)`, see the A2 notes): the same fact is asserted
	// over the public surface by reflection.
	static readonly Type[] ViewTypes =
	[
		typeof(IMappedQuery<>),
		typeof(MappedQuery<>),
		typeof(MappedQueryWithSkip<>),
		typeof(MappedQueryWithTake<>),
		typeof(MappedReader<>),
		typeof(MappedCommand<>),
		typeof(MappedTable<>),
	];

	static bool IsView(Type type)
		=> type.IsGenericType
		&& Array.IndexOf(ViewTypes, type.GetGenericTypeDefinition()) >= 0;

	static IEnumerable<MethodInfo> PublicSelects(Assembly assembly)
		=> assembly.GetExportedTypes()
			.SelectMany(static t => t.GetMethods(
				BindingFlags.Public
				| BindingFlags.Static
				| BindingFlags.Instance
				| BindingFlags.DeclaredOnly))
			.Where(static m => m.Name == "Select");

	[Fact]
	public static void NoViewHasASelect_AsAnExtensionOrAMember()
	{
		Assembly core = typeof(IMappedQuery<>).Assembly;

		foreach (MethodInfo method in PublicSelects(core))
		{
			Type declaring = method.DeclaringType!;
			Assert.False(
				IsView(declaring),
				$"{declaring.FullName}.Select is a member of a mapped view");

			if (method.IsStatic
				&& method.GetParameters() is [{ } first, ..])
			{
				Assert.False(
					IsView(first.ParameterType),
					$"{declaring.FullName}.Select extends {first.ParameterType}");
			}
		}
	}

	[Fact]
	public static void TheReaderLevelSelect_StillExists_AndStillCompiles()
	{
		Assert.Contains(
			PublicSelects(typeof(IMappedQuery<>).Assembly),
			static m => m.IsStatic
				&& m.GetParameters() is [{ } first, ..]
				&& first.ParameterType == typeof(IDataReader));

		// Compiles, so the overload binds on a reader the caller opened.
		using DataTable table = TwoRows();
		using DataTableReader reader = table.CreateDataReader();
		Assert.Equal([1, 2], reader.Select(static r => r.GetInt32(0)).ToList());
	}

	// What the removal leaves as the migration forms
	// (see migration-lines.md): a factory map straight to the result
	// type, and projecting after the terminal.
	sealed record Label(int Id, string Text);

	static DataTable TwoRows()
	{
		DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		table.Rows.Add(1, "a");
		table.Rows.Add(2, "b");
		return table;
	}

	[Fact]
	public static void TheReplacementForms_GiveWhatSelectGave()
	{
		using DataTable table = TwoRows();
		var map = RowMap<Label>.From(
			static (int id, string name) => new Label(id, "n:" + name));

		Assert.Equal(
			[new Label(1, "n:a"), new Label(2, "n:b")],
			table.Map(map).ToList());

		Assert.Equal(
			["1:a", "2:b"],
			table.Map<Pair>()
				.ToList()
				.Select(static x => $"{x.Id}:{x.Name}")
				.ToList());
	}

	sealed record Pair(int Id, string Name);
}
