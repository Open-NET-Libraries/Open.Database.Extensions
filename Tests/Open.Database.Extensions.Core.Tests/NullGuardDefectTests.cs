using System.Data.Common;
using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A null argument is refused once, at the public entry point, by an ArgumentNullException that names that argument:
// never a NullReferenceException, never the name of an argument of something it calls, and before anything has been executed or yielded.
[ExcludeFromCodeCoverage]
public static class NullGuardDefectTests
{
	static readonly IEnumerable<string> Names = ["id"];

	// Defect 7: a null remainder means no more ordinals. It used to be a NullReferenceException while enumerated, after the first value.
	[Fact]
	public static void EnumerateValuesFromOrdinals_NullRemainder_YieldsOnlyTheFirstValue()
	{
		IDataRecord record = Substitute.For<IDataRecord>();
		record.GetValue(0).Returns("a");

		Assert.Equal(["a"], record.EnumerateValuesFromOrdinals(0, null!));
	}

	[Fact]
	public static void EnumerateValuesFromOrdinals_YieldsTheFirstOrdinalThenTheOthers()
	{
		IDataRecord record = Substitute.For<IDataRecord>();
		record.GetValue(0).Returns("a");
		record.GetValue(2).Returns("c");
		record.GetValue(1).Returns("b");

		Assert.Equal(["a", "c", "b"], record.EnumerateValuesFromOrdinals(0, 2, 1));
		Assert.Equal(["a"], record.EnumerateValuesFromOrdinals(0, []));
	}

	// ---- Defects found while the XML documentation was checked against the code. A null argument was a NullReferenceException,
	// a wrongly named ArgumentNullException (the name of something the method calls), or a failure only later (while enumerated, or when the first connection was taken). ----

	// The call itself refuses a null connection: the exception does not wait for the returned task to be awaited.
	[Fact]
	public static void OpenAsync_NullConnection_IsRefusedByTheCall_NamingTheConnection()
	{
		var ex = Assert.Throws<ArgumentNullException>(
#pragma warning disable RCS1021 // An expression body would bind to the Func<Task> overload of Throws, which xUnit refuses.
			() => { _ = ((IDbConnection)null!).OpenAsync(TestContext.Current.CancellationToken).AsTask(); });
#pragma warning restore RCS1021

		Assert.Equal("connection", ex.ParamName);
	}

	// A function or a factory that is null is refused where it is given, not when the first connection is taken.
	static readonly Dictionary<string, Action> AsPoolOnNull = new()
	{
		["AsPool(IDbConnectionFactory)"] = () => ((IDbConnectionFactory)null!).AsPool(),
		["AsPool(Func)"] = () => ((Func<IDbConnection>)null!).AsPool(),
		["AsPool(IDbConnectionFactory<T>)"] = () => ((IDbConnectionFactory<DbConnection>)null!).AsPool(),
		["AsPool(Func<T>)"] = () => ((Func<DbConnection>)null!).AsPool(),
	};

	public static TheoryData<string> AsPoolNames() => [.. AsPoolOnNull.Keys];

	[Theory]
	[MemberData(nameof(AsPoolNames))]
	public static void AsPool_NullSource_NamesTheSource(string call)
	{
		// The adapter constructor, which every AsPool ends in, is the one guard.
		var ex = Assert.Throws<ArgumentNullException>(AsPoolOnNull[call]);

		Assert.Equal("factory", ex.ParamName);
	}

	[Fact]
	public static void AsPool_NullFactory_NamesTheFactory()
	{
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionFactory)null!).AsPool()).ParamName);
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(
			() => ((IDbConnectionFactory<DbConnection>)null!).AsPool()).ParamName);
	}

	// The function constructors too: a null function is refused where it is given, not when the first connection is taken.
	[Fact]
	public static void AsPool_NullFunction_NamesTheFactory()
	{
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(
			() => ((Func<IDbConnection>)null!).AsPool()).ParamName);
		Assert.Equal("factory", Assert.Throws<ArgumentNullException>(
			() => ((Func<DbConnection>)null!).AsPool()).ParamName);
	}

	static readonly Dictionary<string, Action> CommandOnNullFunc = new()
	{
		["Command(Func<IDbConnection>)"] = () => ((Func<IDbConnection>)null!).Command("select 1"),
		["StoredProcedure(Func<IDbConnection>)"] = () => ((Func<IDbConnection>)null!).StoredProcedure("proc"),
		["Command(Func<T>)"] = () => ((Func<DbConnection>)null!).Command("select 1"),
		["StoredProcedure(Func<T>)"] = () => ((Func<DbConnection>)null!).StoredProcedure("proc"),
	};

	public static TheoryData<string> CommandOnNullFuncNames() => [.. CommandOnNullFunc.Keys];

	[Theory]
	[MemberData(nameof(CommandOnNullFuncNames))]
	public static void Command_NullConnectionFunction_NamesTheConnectionSource(string call)
	{
		var ex = Assert.Throws<ArgumentNullException>(CommandOnNullFunc[call]);

		// AsPool (or DbConnectionFactory.Create) and then the adapter or factory constructor is the one guard.
		Assert.Equal("factory", ex.ParamName);
	}

	[Fact]
	public static void GetNames_NullOrdinals_NamesTheOrdinals()
	{
		IDataRecord record = Substitute.For<IDataRecord>();

		Assert.Equal("source", Assert.Throws<ArgumentNullException>( // Select is the one guard.
			() => record.GetNames(null!)).ParamName);
	}

	// These are lazy: the refusal comes with the call, not while the result is enumerated.
	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public static void OrdinalMapping_NullArguments_AreRefusedByTheCall(bool sort)
	{
		IDataRecord record = Substitute.For<IDataRecord>();

		Assert.Equal("record", Assert.Throws<ArgumentNullException>(
			() => ((IDataRecord)null!).OrdinalMapping(Names, sort)).ParamName);
		Assert.Equal("source", Assert.Throws<ArgumentNullException>( // Select is the one guard.
			() => record.OrdinalMapping(null!, sort)).ParamName);
	}

	[Fact]
	public static void OrdinalMapping_NullArguments_AreRefusedByTheCall_WithoutSort()
	{
		IDataRecord record = Substitute.For<IDataRecord>();

		Assert.Equal("record", Assert.Throws<ArgumentNullException>(
			() => ((IDataRecord)null!).OrdinalMapping(Names)).ParamName);
		Assert.Equal("source", Assert.Throws<ArgumentNullException>( // Select is the one guard.
			() => record.OrdinalMapping(null!)).ParamName);
	}

	[Fact]
	public static void GetOrdinalMapping_NullArguments_AreRefusedByOrdinalMapping_ExceptForAnEmptyList()
	{
		IDataRecord record = Substitute.For<IDataRecord>();

		// An empty list is the one benign edge: nothing is read, so a null record is not looked at.
		Assert.Empty(((IDataRecord)null!).GetOrdinalMapping([]));
		Assert.Equal("record", Assert.Throws<ArgumentNullException>(
			() => ((IDataRecord)null!).GetOrdinalMapping(Names)).ParamName);
		Assert.Equal("source", Assert.Throws<ArgumentNullException>( // OrdinalMapping reaches Select, the one guard.
			() => record.GetOrdinalMapping(null!)).ParamName);
	}

	[Fact]
	public static void MatchingOrdinals_NullArguments_AreRefusedByTheCall()
	{
		IDataRecord record = Substitute.For<IDataRecord>();

		Assert.Equal("record", Assert.Throws<ArgumentNullException>(
			() => ((IDataRecord)null!).MatchingOrdinals(Names)).ParamName);
		Assert.Equal("columnNames", Assert.Throws<ArgumentNullException>(
			() => record.MatchingOrdinals(null!)).ParamName);
	}
}
