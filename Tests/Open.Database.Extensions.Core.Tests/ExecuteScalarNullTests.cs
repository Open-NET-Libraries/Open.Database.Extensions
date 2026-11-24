using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ExecuteScalar<T>() / ExecuteScalarAsync<T>() with a NULL scalar or a query that returned no row (the provider hands back null or DBNull):
// no row (null) is null for a type that takes null and a ValueMappingException for a non-nullable value type (ask for int? and use ?? for a default); a NULL (DBNull) is null for a nullable type and a ValueMappingException for a non-nullable value type.
// A value converts as Map<T>() does (ExecuteScalarConversionTests).
// The transform overloads get null for both (never DBNull); the raw ExecuteScalar()/ExecuteScalarAsync() stay raw.
[ExcludeFromCodeCoverage]
public static class ExecuteScalarNullTests
{
	// What a provider returns for a NULL scalar (DBNull) and for no rows (null).
	static readonly object?[] NoValue = [null, DBNull.Value];

	internal static ExpressiveCommand Sync(object? scalar)
	{
		var probe = new ConnectionProbe(ConnectionState.Open);
		probe.Command.ExecuteScalar().Returns(_ => scalar);
		return probe.Connection.Command("SELECT 1");
	}

	// A connection that is already open and hands out the one command, whose scalar is the given value.
	sealed class ScalarConnection(DbCommand command) : DbConnection
	{
		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "fake";
		public override string DataSource => "fake";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() { }
		public override void Close() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
		protected override DbCommand CreateDbCommand() => command;
	}

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	internal static ExpressiveDbCommand Async(object? scalar)
	{
		DbCommand command = Substitute.For<DbCommand>();
		command.ExecuteScalarAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(scalar));
		return new ScalarConnection(command).Command("SELECT 1");
	}

	// ---- sync ----

	[Fact]
	public static void ExecuteScalar_NoRowIsAnErrorForANonNullableValueType_ANullIsToo_AndNullForATypeThatTakesIt()
	{
		NullAssert.NoRows<int>("ExecuteScalar", () => Sync(null).ExecuteScalar<int>());
		NullAssert.NoRows<long>("ExecuteScalar", () => Sync(null).ExecuteScalar<long>());
		NullAssert.NoRows<DateTime>("ExecuteScalar", () => Sync(null).ExecuteScalar<DateTime>());
		NullAssert.Scalar<int>(() => Sync(DBNull.Value).ExecuteScalar<int>());
		NullAssert.Scalar<long>(() => Sync(DBNull.Value).ExecuteScalar<long>());
		NullAssert.Scalar<DateTime>(() => Sync(DBNull.Value).ExecuteScalar<DateTime>());
		foreach (object? none in NoValue)
		{
			Assert.Null(Sync(none).ExecuteScalar<int?>());
			Assert.Null(Sync(none).ExecuteScalar<Guid?>());
			Assert.Null(Sync(none).ExecuteScalar<string>());
			Assert.Null(Sync(none).ExecuteScalar<byte[]>());
			Assert.Null(Sync(none).ExecuteScalar<object>());
		}
	}

	[Fact]
	public static void ExecuteScalar_AValue_IsConverted()
	{
		Assert.Equal(42, Sync(42).ExecuteScalar<int>());
		Assert.Equal(42, Sync(42).ExecuteScalar<int?>());
		Assert.Equal("v", Sync("v").ExecuteScalar<string>());
		Assert.Equal(42L, Sync(42).ExecuteScalar<long>()); // a conversion, not a cast
	}

	// ---- async ----

	[Fact]
	public static async Task ExecuteScalarAsync_NoRowIsAnErrorForANonNullableValueType_ANullIsToo_AndNullForATypeThatTakesIt()
	{
		await NullAssert.NoRowsAsync<int>("ExecuteScalarAsync", async () => await Async(null)
			.ExecuteScalarAsync<int>(
				cancellationToken: TestContext.Current.CancellationToken));
		await NullAssert.NoRowsAsync<DateTime>("ExecuteScalarAsync", async () => await Async(null)
			.ExecuteScalarAsync<DateTime>(
				cancellationToken: TestContext.Current.CancellationToken));
		await NullAssert.NoRowsAsync<long>("ExecuteScalarAsync", async () => await Async(null)
			.ExecuteScalarAsync<long>(
				cancellationToken: TestContext.Current.CancellationToken));
		await NullAssert.ScalarAsync<int>(async () => await Async(DBNull.Value)
			.ExecuteScalarAsync<int>(
				cancellationToken: TestContext.Current.CancellationToken));
		await NullAssert.ScalarAsync<DateTime>(async () => await Async(DBNull.Value)
			.ExecuteScalarAsync<DateTime>(
				cancellationToken: TestContext.Current.CancellationToken));
		foreach (object? none in NoValue)
		{
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync<int?>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync<Guid?>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync<string>(
						cancellationToken: TestContext.Current.CancellationToken));
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync<object>(
						cancellationToken: TestContext.Current.CancellationToken));
		}
	}

	[Fact]
	public static async Task ExecuteScalarAsync_AValue_IsConverted()
	{
		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync<int>(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync<int?>(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			"v",
			await Async("v")
				.ExecuteScalarAsync<string>(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			42L,
			await Async(42)
				.ExecuteScalarAsync<long>(
					cancellationToken: TestContext.Current.CancellationToken));
	}

	// ---- the raw scalar: the ADO.NET primitive, not normalized (the only way to tell NULL from no row) ----

	[Fact]
	public static void ExecuteScalar_Raw_ReturnsDBNullForNullAndNullForNoRow()
	{
		Assert.Same(DBNull.Value, Sync(DBNull.Value).ExecuteScalar());
		Assert.Null(Sync(null).ExecuteScalar());
		Assert.Equal(42, Sync(42).ExecuteScalar());
	}

	[Fact]
	public static async Task ExecuteScalarAsync_Raw_ReturnsDBNullForNullAndNullForNoRow()
	{
		Assert.Same(
			DBNull.Value,
			await Async(DBNull.Value)
				.ExecuteScalarAsync(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Null(
			await Async(null)
				.ExecuteScalarAsync(
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync(
					cancellationToken: TestContext.Current.CancellationToken));
	}

	// ---- the transform overloads: NULL and no row both reach the function as null, never DBNull ----

	[Fact]
	public static void ExecuteScalar_Transform_ReceivesNullForNullAndNoRow()
	{
		foreach (object? none in NoValue)
		{
			int calls = 0;
			object? received = DBNull.Value; // anything but null, so an uncalled transform fails the assertion
			int result = Sync(none).ExecuteScalar(o =>
			{
				calls++;
				received = o;
				return -1;
			});

			Assert.Equal(-1, result);
			Assert.Equal(1, calls);
			Assert.Null(received);
		}
	}

	[Fact]
	public static void ExecuteScalar_Transform_PassesAValueThroughUnchanged()
	{
		Assert.Equal(42, Sync(42).ExecuteScalar(o => o));
		Assert.Equal("v", Sync("v").ExecuteScalar(o => o));
		object boxed = 42;
		Assert.Same(boxed, Sync(boxed).ExecuteScalar(o => o));
	}

	[Fact]
	public static void ExecuteScalar_Transform_NullableCast_GivesNullForNullAndTheValueForAValue()
	{
		foreach (object? none in NoValue)
			Assert.Null(Sync(none).ExecuteScalar(o => (int?)o));

		Assert.Equal(42, Sync(42).ExecuteScalar(o => (int?)o));
	}

	[Fact]
	public static async Task ExecuteScalarAsync_Transform_ReceivesNullForNullAndNoRow()
	{
		foreach (object? none in NoValue)
		{
			int calls = 0;
			object? received = DBNull.Value; // anything but null, so an uncalled transform fails the assertion
			int result = await Async(none).ExecuteScalarAsync(o =>
			{
				calls++;
				received = o;
				return -1;
			}, cancellationToken: TestContext.Current.CancellationToken);

			Assert.Equal(-1, result);
			Assert.Equal(1, calls);
			Assert.Null(received);
		}
	}

	[Fact]
	public static async Task ExecuteScalarAsync_Transform_PassesAValueThroughUnchanged()
	{
		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync(
					o => o,
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			"v",
			await Async("v")
				.ExecuteScalarAsync(
					o => o,
					cancellationToken: TestContext.Current.CancellationToken));
		object boxed = 42;
		Assert.Same(
			boxed,
			await Async(boxed)
				.ExecuteScalarAsync(
					o => o,
					cancellationToken: TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ExecuteScalarAsync_Transform_NullableCast_GivesNullForNullAndTheValueForAValue()
	{
		foreach (object? none in NoValue)
		{
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync(
						o => (int?)o,
						cancellationToken: TestContext.Current.CancellationToken));
		}

		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync(
					o => (int?)o,
					cancellationToken: TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ExecuteScalarAsync_TaskTransform_ReceivesNullForNullAndNoRow()
	{
		foreach (object? none in NoValue)
		{
			int calls = 0;
			object? received = DBNull.Value; // anything but null, so an uncalled transform fails the assertion
			int result = await Async(none).ExecuteScalarAsync(o =>
			{
				calls++;
				received = o;
				return new ValueTask<int>(-1);
			}, cancellationToken: TestContext.Current.CancellationToken);

			Assert.Equal(-1, result);
			Assert.Equal(1, calls);
			Assert.Null(received);
		}
	}

	[Fact]
	public static async Task ExecuteScalarAsync_TaskTransform_PassesAValueThroughUnchanged()
	{
		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync(
					o => new ValueTask<object?>(o),
					cancellationToken: TestContext.Current.CancellationToken));
		Assert.Equal(
			"v",
			await Async("v")
				.ExecuteScalarAsync(
					o => new ValueTask<object?>(o),
					cancellationToken: TestContext.Current.CancellationToken));
		object boxed = 42;
		Assert.Same(
			boxed,
			await Async(boxed)
				.ExecuteScalarAsync(
					o => new ValueTask<object?>(o),
					cancellationToken: TestContext.Current.CancellationToken));
	}

	[Fact]
	public static async Task ExecuteScalarAsync_TaskTransform_NullableCast_GivesNullForNullAndTheValueForAValue()
	{
		foreach (object? none in NoValue)
		{
			Assert.Null(
				await Async(none)
					.ExecuteScalarAsync(
						o => new ValueTask<int?>((int?)o),
						cancellationToken: TestContext.Current.CancellationToken));
		}

		Assert.Equal(
			42,
			await Async(42)
				.ExecuteScalarAsync(
					o => new ValueTask<int?>((int?)o),
					cancellationToken: TestContext.Current.CancellationToken));
	}
}
