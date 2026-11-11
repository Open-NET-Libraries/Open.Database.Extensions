using System.Data.Common;
using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A null argument is refused once, at the public entry point, by an ArgumentNullException that names that argument:
// never a NullReferenceException, never the name of an argument of something it calls, and before anything has been executed or yielded.
[ExcludeFromCodeCoverage]
public static class NullGuardDefectTests
{
	static readonly IEnumerable<int> Ordinals = [0];
	static readonly IEnumerable<string> Names = ["id"];

	static readonly Dictionary<string, Action> RetrieveOnNullReader = new()
	{
		["Retrieve()"] = () => ((IDataReader)null!).Retrieve(),
		["Retrieve(ordinals)"] = () => ((IDataReader)null!).Retrieve(Ordinals),
		["Retrieve(n, others)"] = () => ((IDataReader)null!).Retrieve(0, 1),
		["Retrieve(names)"] = () => ((IDataReader)null!).Retrieve(Names),
		["Retrieve(c, others)"] = () => ((IDataReader)null!).Retrieve("id", "name"),
	};

	static readonly Dictionary<string, Func<Task>> RetrieveAsyncOnNullReader = new()
	{
		["RetrieveAsync()"] = () => ((DbDataReader)null!).RetrieveAsync(true, CancellationToken.None).AsTask(),
		["RetrieveAsync(token)"] = () => ((DbDataReader)null!).RetrieveAsync(CancellationToken.None).AsTask(),
		["RetrieveAsync(ordinals)"] = () => ((DbDataReader)null!).RetrieveAsync(Ordinals, true, CancellationToken.None).AsTask(),
		["RetrieveAsync(ordinals, token)"] = () => ((DbDataReader)null!).RetrieveAsync(Ordinals, CancellationToken.None).AsTask(),
		["RetrieveAsync(n, others)"] = () => ((DbDataReader)null!).RetrieveAsync(0, 1).AsTask(),
		["RetrieveAsync(token, n, others)"] = () => ((DbDataReader)null!).RetrieveAsync(CancellationToken.None, 0, 1).AsTask(),
		["RetrieveAsync(names)"] = () => ((DbDataReader)null!).RetrieveAsync(Names, false, true, CancellationToken.None).AsTask(),
		["RetrieveAsync(names, normalize, token)"] = () => ((DbDataReader)null!).RetrieveAsync(Names, false, CancellationToken.None).AsTask(),
		["RetrieveAsync(names, token)"] = () => ((DbDataReader)null!).RetrieveAsync(Names, CancellationToken.None).AsTask(),
		["RetrieveAsync(c, others)"] = () => ((DbDataReader)null!).RetrieveAsync("id", "name").AsTask(),
		["RetrieveAsync(token, c, others)"] = () => ((DbDataReader)null!).RetrieveAsync(CancellationToken.None, "id", "name").AsTask(),
	};

	public static TheoryData<string> RetrieveNames() => [.. RetrieveOnNullReader.Keys];

	public static TheoryData<string> RetrieveAsyncNames() => [.. RetrieveAsyncOnNullReader.Keys];

	// Defects 3 and 4: the argument that is wrong is called "reader"; it was reported as "record" or as a NullReferenceException.
	[Theory]
	[MemberData(nameof(RetrieveNames))]
	public static void Retrieve_NullReader_NamesTheReader(string call)
	{
		var ex = Assert.Throws<ArgumentNullException>(RetrieveOnNullReader[call]);

		// These have no guard of their own: GetNames guards the reader and names it "record".
		Assert.Equal(call is "Retrieve()" or "Retrieve(ordinals)" or "Retrieve(n, others)" ? "record" : "reader", ex.ParamName);
	}

	[Theory]
	[MemberData(nameof(RetrieveAsyncNames))]
	public static async Task RetrieveAsync_NullReader_NamesTheReader(string call)
	{
		var ex = await Assert.ThrowsAsync<ArgumentNullException>(RetrieveAsyncOnNullReader[call]);

		Assert.Equal("reader", ex.ParamName);
	}

	// Defect 5: the selector was only called after the command had been executed, and a null one was a NullReferenceException.
	[Fact]
	public static void IterateReader_NullSelector_IsRefusedBeforeTheCommandRuns()
	{
		var tally = new DbTally();
		ExpressiveDbCommand command = tally.Factory.Command("select 1");

#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		var ex = Assert.Throws<ArgumentNullException>(() => command.IterateReader(r => r.GetValue(0), (Func<IEnumerable<object>, int>)null!));
#pragma warning restore CS0618

		Assert.Equal("selector", ex.ParamName);
		Assert.Empty(tally.Connections);
		Assert.Empty(tally.Readers);
	}

	// Defect 6: a null result is an empty collection. It used to be refused by two of the constructors and a NullReferenceException when enumerated.
	[Fact]
	public static void QueryResultCollection_Sequences_NullResult_IsEmpty()
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		=> Assert.Empty(new QueryResultCollection<int, List<int>>(Ordinals, Names, null!));
		#pragma warning restore CS0618

	[Fact]
	public static void QueryResultCollection_OfT_Sequences_NullResult_IsEmpty()
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		=> Assert.Empty(new QueryResultCollection<int>(Ordinals, Names, null!));
		#pragma warning restore CS0618

	[Fact]
	public static void QueryResultCollection_ImmutableArray_NullResult_IsEmpty()
		#pragma warning disable CS0618 // Obsolete in 11.0, removed in 12: kept covered until then.
		=> Assert.Empty(new QueryResultCollection<int, List<int>>([0], ["id"], null!));
		#pragma warning restore CS0618

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
}
