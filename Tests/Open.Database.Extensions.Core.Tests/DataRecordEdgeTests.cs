using System.Buffers;
using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Edge cases of the extensions on IDataRecord that read values: a pooled array, and the order and the NULLs of values read by ordinal.
[ExcludeFromCodeCoverage]
public static class DataRecordEdgeTests
{
	sealed class SizedPool : ArrayPool<object>
	{
		public List<int> Requests { get; } = [];

		public override object[] Rent(int minimumLength)
		{
			Requests.Add(minimumLength);
			return new object[minimumLength];
		}

		public override void Return(object[] array, bool clearArray = false) { }
	}

	[Fact]
	public static void GetValues_WithAPool_RentsOneArrayAsLongAsTheRecord_AndKeepsNulls()
	{
		var pool = new SizedPool();
		TypedReader reader = Reader([1, null, "n1"]);
		Assert.True(reader.Read());

		object[] values = reader.GetValues(pool);

		Assert.Equal([3], pool.Requests);
		Assert.Equal([1, DBNull.Value, "n1"], values);
	}

	[Fact]
	public static void GetValues_ANullPool_IsRefusedByName()
	{
		TypedReader reader = Reader(TwoRows);

		ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => reader.GetValues((ArrayPool<object>)null!));

		Assert.Equal("arrayPool", ex.ParamName);
	}

	[Fact]
	public static void EnumerateValuesFromOrdinals_YieldsTheFirstOrdinalThenTheOthers_AndKeepsNulls()
	{
		TypedReader reader = Reader([1, null, "n1"]);
		Assert.True(reader.Read());

		object[] values = reader.EnumerateValuesFromOrdinals(2, 1, 0).ToArray();

		Assert.Equal(["n1", DBNull.Value, 1], values);
	}
}
