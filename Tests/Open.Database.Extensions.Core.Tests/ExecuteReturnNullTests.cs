#nullable enable

namespace Open.Database.Extensions.Tests;

// ExecuteReturn<T>() / ExecuteReturnAsync<T>() with a NULL return value (the provider leaves null or DBNull in the return parameter):
// default(T) for every T, the same rule as ExecuteScalar<T>(). A value is still the exact cast.
[ExcludeFromCodeCoverage]
public static class ExecuteReturnNullTests
{
	// What a provider can leave in a return parameter for NULL.
	static readonly object?[] NoValue = [null, DBNull.Value];

	// A command whose return parameter holds the given value once the command has run.
	static ExpressiveCommand Returning(object? value)
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		probe.Command.CreateParameter().Returns(_ =>
		{
			var parameter = Substitute.For<IDbDataParameter>();
			parameter.Value.Returns(value);
			return parameter;
		});
		return probe.Connection.Command("EXEC p");
	}

	// ---- sync ----

	[Fact]
	public static void ExecuteReturn_Null_ReturnsDefault()
	{
		foreach (object? none in NoValue)
		{
			Assert.Equal(0, Returning(none).ExecuteReturn<int>());
			Assert.Null(Returning(none).ExecuteReturn<int?>());
			Assert.Null(Returning(none).ExecuteReturn<string>());
			Assert.Null(Returning(none).ExecuteReturn<object>());
		}
	}

	[Fact]
	public static void ExecuteReturn_AValue_IsStillTheExactCast()
	{
		Assert.Equal(42, Returning(42).ExecuteReturn<int>());
		Assert.Equal(42, Returning(42).ExecuteReturn<int?>());
		Assert.Equal("v", Returning("v").ExecuteReturn<string>());
		Assert.Throws<InvalidCastException>(() => Returning(42).ExecuteReturn<long>()); // a cast, not a conversion
	}

	// ---- async ----

	[Fact]
	public static async Task ExecuteReturnAsync_Null_ReturnsDefault()
	{
		foreach (object? none in NoValue)
		{
			Assert.Equal(
				0,
				await Returning(none)
					.ExecuteReturnAsync<int>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Returning(none)
					.ExecuteReturnAsync<int?>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Returning(none)
					.ExecuteReturnAsync<string>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Returning(none)
					.ExecuteReturnAsync<object>(
						cancellationToken: TestContext.Current.CancellationToken));
		}
	}

	[Fact]
	public static async Task ExecuteReturnAsync_AValue_IsStillTheExactCast()
	{
		Assert.Equal(
			42,
			await Returning(42)
				.ExecuteReturnAsync<int>(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			42,
			await Returning(42)
				.ExecuteReturnAsync<int?>(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			"v",
			await Returning("v")
				.ExecuteReturnAsync<string>(
					cancellationToken: TestContext.Current.CancellationToken));
		await Assert.ThrowsAsync<InvalidCastException>(
			async () => await Returning(42)
				.ExecuteReturnAsync<long>(
					cancellationToken: TestContext.Current.CancellationToken));
	}
}
