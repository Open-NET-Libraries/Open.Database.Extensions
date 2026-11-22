namespace Open.Database.Extensions.Tests;

#nullable enable

[ExcludeFromCodeCoverage]
public static class CoreExtensionsDBNullTests
{
	[Fact]
	public static void CopyToDBNullAsNull_WritesConvertedValues_AndReturnsTarget()
	{
		// Arrange
		ReadOnlySpan<object?> source = ["a", DBNull.Value, 3, DBNull.Value];
		Span<object?> target = new object?[source.Length];

		// Act
		Span<object?> returned = source.CopyToDBNullAsNull(target);

		// Assert: DBNull -> null in the target, and the returned span IS the populated target
		// (regression guard: it previously returned a fresh all-null array).
		Assert.Equal("a", target[0]);
		Assert.Null(target[1]);
		Assert.Equal(3, target[2]);
		Assert.Null(target[3]);

		Assert.Equal("a", returned[0]);
		Assert.Null(returned[1]);
		Assert.Equal(3, returned[2]);
		Assert.Null(returned[3]);
	}

	[Fact]
	public static void ReplaceDBNullWithNull_Span_ConvertsInPlace_AndReturnsSameContents()
	{
		// Arrange
		object?[] backing = [DBNull.Value, "x", DBNull.Value];
		Span<object?> span = backing;

		// Act
		Span<object?> returned = span.ReplaceDBNullWithNull();

		// Assert
		Assert.Null(backing[0]);
		Assert.Equal("x", backing[1]);
		Assert.Null(backing[2]);

		Assert.Null(returned[0]);
		Assert.Equal("x", returned[1]);
		Assert.Null(returned[2]);
	}

	[Fact]
	public static void ReplaceDBNullWithNull_Array_ConvertsInPlace_AndReturnsSameInstance()
	{
		// Arrange
		object?[] values = [DBNull.Value, 1, DBNull.Value];

		// Act
		object?[] returned = values.ReplaceDBNullWithNull();

		// Assert
		Assert.Same(values, returned);
		Assert.Null(values[0]);
		Assert.Equal(1, values[1]);
		Assert.Null(values[2]);
	}

	[Fact]
	public static void ReplaceDBNullWithNull_List_ConvertsInPlace()
	{
		// Arrange
		var values = new List<object?> { DBNull.Value, "y", DBNull.Value };

		// Act
		List<object?> returned = values.ReplaceDBNullWithNull();

		// Assert
		Assert.Same(values, returned);
		Assert.Null(values[0]);
		Assert.Equal("y", values[1]);
		Assert.Null(values[2]);
	}

	[Fact]
	public static void DBNullToNullCopy_Array_ReturnsNewConvertedArray()
	{
		// Arrange
		object?[] values = [DBNull.Value, "z"];

		// Act
		object?[] copy = values.DBNullToNullCopy();

		// Assert
		Assert.NotSame(values, copy);
		Assert.Null(copy[0]);
		Assert.Equal("z", copy[1]);
		// Source is untouched.
		Assert.Equal(DBNull.Value, values[0]);
	}

	[Fact]
	public static void DBNullToNull_Enumerable_YieldsConverted()
	{
		// Arrange
		object?[] values = [DBNull.Value, "a", DBNull.Value, 2];

		// Act
		object?[] result = [.. values.DBNullToNull()];

		// Assert
		Assert.Equal([null, "a", null, 2], result);
	}
}
