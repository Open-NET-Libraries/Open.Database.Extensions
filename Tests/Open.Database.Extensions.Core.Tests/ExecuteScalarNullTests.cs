using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ExecuteScalar<T>() / ExecuteScalarAsync<T>() with a NULL scalar or a query that returned no row (the provider hands back null or DBNull):
// default(T) for every T (0 for int, null for int? and reference types). A value is still the exact cast.
[ExcludeFromCodeCoverage]
public static class ExecuteScalarNullTests
{
	// What a provider returns for a NULL scalar (DBNull) and for no rows (null).
	static readonly object?[] NoValue = [null, DBNull.Value];

	static ExpressiveCommand Sync(object? scalar)
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
	static ExpressiveDbCommand Async(object? scalar)
	{
		DbCommand command = Substitute.For<DbCommand>();
		command.ExecuteScalarAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(scalar));
		return new ScalarConnection(command).Command("SELECT 1");
	}

	// ---- sync ----

	[Fact]
	public static void ExecuteScalar_NullOrNoRow_ReturnsDefault()
	{
		foreach (object? none in NoValue)
		{
			Assert.Equal(0, Sync(none).ExecuteScalar<int>());
			Assert.Equal(0L, Sync(none).ExecuteScalar<long>());
			Assert.Equal(default, Sync(none).ExecuteScalar<DateTime>());
			Assert.Null(Sync(none).ExecuteScalar<int?>());
			Assert.Null(Sync(none).ExecuteScalar<Guid?>());
			Assert.Null(Sync(none).ExecuteScalar<string>());
			Assert.Null(Sync(none).ExecuteScalar<byte[]>());
			Assert.Null(Sync(none).ExecuteScalar<object>());
		}
	}

	[Fact]
	public static void ExecuteScalar_AValue_IsStillTheExactCast()
	{
		Assert.Equal(42, Sync(42).ExecuteScalar<int>());
		Assert.Equal(42, Sync(42).ExecuteScalar<int?>());
		Assert.Equal("v", Sync("v").ExecuteScalar<string>());
		Assert.Throws<InvalidCastException>(() => Sync(42).ExecuteScalar<long>()); // a cast, not a conversion
	}

	[Fact]
	public static void ExecuteScalar_WithATransform_IsUntouched()
	{
		foreach (object? none in NoValue)
			Assert.Equal(-1, Sync(none).ExecuteScalar(o => o is null or DBNull ? -1 : 0));
	}

	// ---- async ----

	[Fact]
	public static async Task ExecuteScalarAsync_NullOrNoRow_ReturnsDefault()
	{
		foreach (object? none in NoValue)
		{
			Assert.Equal(0, await Async(none).ExecuteScalarAsync<int>());
			Assert.Equal(0L, await Async(none).ExecuteScalarAsync<long>());
			Assert.Equal(default, await Async(none).ExecuteScalarAsync<DateTime>());
			Assert.Null(await Async(none).ExecuteScalarAsync<int?>());
			Assert.Null(await Async(none).ExecuteScalarAsync<Guid?>());
			Assert.Null(await Async(none).ExecuteScalarAsync<string>());
			Assert.Null(await Async(none).ExecuteScalarAsync<object>());
		}
	}

	[Fact]
	public static async Task ExecuteScalarAsync_AValue_IsStillTheExactCast()
	{
		Assert.Equal(42, await Async(42).ExecuteScalarAsync<int>());
		Assert.Equal(42, await Async(42).ExecuteScalarAsync<int?>());
		Assert.Equal("v", await Async("v").ExecuteScalarAsync<string>());
		await Assert.ThrowsAsync<InvalidCastException>(async () => await Async(42).ExecuteScalarAsync<long>());
	}

	[Fact]
	public static async Task ExecuteScalarAsync_WithATransform_IsUntouched()
	{
		foreach (object? none in NoValue)
			Assert.Equal(-1, await Async(none).ExecuteScalarAsync(o => o is null or DBNull ? -1 : 0));
	}
}
