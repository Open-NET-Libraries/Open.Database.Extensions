using Open.Database.Extensions.Tests.NullPolicyMatrix;
using System.Collections;
using System.Reflection;
using static Open.Database.Extensions.Tests.ConstructorBindingTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Drives the NullPolicy parity matrix (Tests/Shared/NullPolicyMatrix) through the runtime reader, on a typed reader and on one that reports object.
// The generated reader runs the same cases through its own runner.
[ExcludeFromCodeCoverage]
public static class NullPolicyMatrixTests
{
	static readonly Dictionary<string, Case> ByName = Matrix.Cases().ToDictionary(c => c.Name);

	static readonly ImmutableArray<ColumnDef> Columns = [.. Matrix.Kinds.Select(k => new ColumnDef(k.Member, k.ColumnType))];

	static readonly MethodInfo ReadRowsMethod = typeof(ConstructorBindingTests).GetMethod(nameof(ReadRows), BindingFlags.Static | BindingFlags.NonPublic)!;

	public static TheoryData<Route, string> All
	{
		get
		{
			var data = new TheoryData<Route, string>();
			foreach (Case c in ByName.Values)
			{
				data.Add(Route.Typed, c.Name);
				data.Add(Route.Boxed, c.Name);
			}

			return data;
		}
	}

	// ReadRows<TModel>(route, columns, rows), with the reader's own exception unwrapped.
	static List<object> Read(Case c, Route route)
		=> [.. ((IEnumerable)ReadRowsMethod.MakeGenericMethod(c.Model).Invoke(null, BindingFlags.DoNotWrapExceptions, null, [route, Columns, c.Rows, null], null)!).Cast<object>()];

	[Theory]
	[MemberData(nameof(All))]
	public static void EachCase(Route route, string name)
	{
		Case c = ByName[name];

		if (c.Error is { } expected)
		{
			ValueMappingException ex = Assert.Throws<ValueMappingException>(() => Read(c, route));
			Assert.Equal(expected.Message, ex.Message);
			Assert.Equal(expected.PropertyName, ex.PropertyName);
			Assert.Equal(expected.ColumnName, ex.ColumnName);
			Assert.Equal(expected.TargetType, ex.TargetType);
			Assert.Equal(expected.InnerType, ex.InnerException?.GetType());
			return;
		}

		PropertyInfo member = c.Model.GetProperty(c.Kind.Member)!;
		Assert.Equal(c.Values, Read(c, route).Select(row => member.GetValue(row)));
	}

	public static TheoryData<Route, string> HierarchyNames
	{
		get
		{
			var data = new TheoryData<Route, string>();
			foreach (HierarchyCase c in Matrix.Hierarchy)
			{
				data.Add(Route.Typed, c.Name);
				data.Add(Route.Boxed, c.Name);
			}

			return data;
		}
	}

	// A NULL into one member of a derived model, alone in its row.
	[Theory]
	[MemberData(nameof(HierarchyNames))]
	public static void EachHierarchyCase(Route route, string name)
	{
		HierarchyCase c = Matrix.Hierarchy.Single(h => h.Name == name);
		ImmutableArray<ColumnDef> columns = [new(c.Member, Nullable.GetUnderlyingType(c.MemberType) ?? c.MemberType)];
		object?[][] rows = [[null]];
		List<object> ReadOne() => [.. ((IEnumerable)ReadRowsMethod.MakeGenericMethod(c.Model).Invoke(null, BindingFlags.DoNotWrapExceptions, null, [route, columns, rows, null], null)!).Cast<object>()];

		if (c.Throws)
		{
			ValueMappingException ex = Assert.Throws<ValueMappingException>(ReadOne);
			Assert.Equal(c.Message, ex.Message);
			Assert.Equal(c.MemberType, ex.TargetType);
			return;
		}

		Assert.Equal(c.Value, c.Model.GetProperty(c.Member)!.GetValue(Assert.Single(ReadOne())));
	}

	public static TheoryData<string> MisfitNames => [.. Matrix.Misfits.Select(m => m.Model.Name)];

	// A null value that does not fit: the map is not created (RowMap<T>.Default throws), whatever the rows.
	[Theory]
	[MemberData(nameof(MisfitNames))]
	public static void EachMisfit(string name)
	{
		Misfit misfit = Matrix.Misfits.Single(m => m.Model.Name == name);
		MethodInfo getDefault = typeof(RowMap<>).MakeGenericType(misfit.Model).GetProperty("Default")!.GetMethod!;
		ArgumentException ex = Assert.Throws<ArgumentException>(() => getDefault.Invoke(null, BindingFlags.DoNotWrapExceptions, null, null, null));
		Assert.Equal(misfit.Message, ex.Message);
	}

	// The table itself: every policy, target and kind has a model member, so no case is silently empty.
	[Fact]
	public static void TheMatrix_HasAModelForEveryPolicyAndTarget_AndEveryCase()
	{
		Assert.Equal(Matrix.Policies.Length * 4 * Matrix.Kinds.Length * 5, ByName.Count);
		foreach (Case c in ByName.Values)
			Assert.NotNull(c.Model.GetProperty(c.Kind.Member));
	}
}
