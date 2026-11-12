using static Open.Database.Extensions.Tests.ExecuteScalarNullTests;

#nullable enable

namespace Open.Database.Extensions.Tests;

// ExecuteScalar<T>() and ExecuteScalarAsync<T>() convert the value
// the way column 0 of Map<T>() does:
// a value that is already a T as it is,
// an integer into another integer type with a range check,
// any other value by the general conversion,
// NULL and no row to default(T);
// and a failure is the same ValueMappingException,
// naming the scalar rather than a column.
// The fixtures are those of ExecuteScalarNullTests.
[ExcludeFromCodeCoverage]
public static class ExecuteScalarConversionTests
{
	public enum Level { None = 0, One = 1, Two = 2 }
	public enum Small : byte { A = 0, B = 3 }

	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// Sync and async give the same value.
	static async Task Reads<T>(object? scalar, T expected)
	{
		Assert.Equal(expected, Sync(scalar).ExecuteScalar<T>());
		Assert.Equal(
			expected,
			await Async(scalar).ExecuteScalarAsync<T>(Ct));
	}

	// Sync and async give the same failure.
	static async Task<ValueMappingException> Fails<T>(object? scalar)
	{
		ValueMappingException syncFailure = Assert.Throws<ValueMappingException>(
			() => Sync(scalar).ExecuteScalar<T>());
		ValueMappingException asyncFailure
			= await Assert.ThrowsAsync<ValueMappingException>(
				async () => await Async(scalar).ExecuteScalarAsync<T>(Ct));

		Assert.Equal(syncFailure.Message, asyncFailure.Message);
		Assert.Equal(
			syncFailure.InnerException?.GetType(),
			asyncFailure.InnerException?.GetType());
		return syncFailure;
	}

	// ---- the values the consumer met ----

	[Fact]
	public static async Task ABigint_ReadsAsInt()
	{
		await Reads(5L, 5);
		await Reads(int.MaxValue + 0L, int.MaxValue);
		await Reads(int.MinValue + 0L, int.MinValue);
		await Reads(-5L, -5);
	}

	[Fact]
	public static async Task ANumeric_ReadsAsLong()
	{
		await Reads(123m, 123L);
		await Reads(42.0m, 42L);
		await Reads(2.6m, 3L); // Convert rounds to nearest, as for a property
		await Reads(5m, 5);
	}

	[Fact]
	public static async Task AnInt_ReadsAsLong()
	{
		await Reads(7, 7L);
		await Reads(int.MinValue, (long)int.MinValue);
	}

	[Fact]
	public static async Task ByteAndShort_Widen()
	{
		await Reads((byte)7, 7);
		await Reads((byte)7, 7L);
		await Reads((short)-7, -7);
		await Reads((short)7, 7L);
		await Reads((sbyte)-7, -7);
		await Reads((ushort)7, 7);
		await Reads(7u, 7L);
		await Reads(7ul, 7L);
	}

	[Fact]
	public static async Task ALongThatDoesNotFit_IsAValueMappingException()
	{
		ValueMappingException high = await Fails<int>(int.MaxValue + 1L);
		Assert.IsType<OverflowException>(high.InnerException);
		Assert.Equal(typeof(int), high.TargetType);
		Assert.Equal("Unable to read the scalar as Int32.", high.Message);
		Assert.Equal("", high.ColumnName);
		Assert.Equal("", high.PropertyName);

		await Fails<int>(int.MinValue - 1L);
		await Fails<int>(long.MaxValue);
		await Fails<short>(70_000);
		await Fails<byte>(-1);
		await Fails<uint>(-1);
		await Fails<ulong>(-1L);
		await Fails<sbyte>((byte)200);
		await Fails<long>(ulong.MaxValue);
		await Fails<int>(5_000_000_000m);
	}

	[Fact]
	public static async Task ANullableTarget_ConvertsToo()
	{
		await Reads<int?>(5L, 5);
		await Reads<long?>(5, 5L);
		await Reads<int?>(5, 5);
		await Reads<int?>(2.6m, 3);
		await Reads<double?>(5, 5d);
		await Reads<Level?>(2, Level.Two);

		ValueMappingException failure = await Fails<int?>(5_000_000_000L);
		Assert.Equal(typeof(int?), failure.TargetType);
		Assert.Equal("Unable to read the scalar as Int32?.", failure.Message);
	}

	[Fact]
	public static async Task AFractionalTarget_Widens()
	{
		await Reads(5L, 5m);
		await Reads(5, 5d);
		await Reads(5L, 5f);
		await Reads(1.5f, 1.5d);
		await Reads(1.5m, 1.5d);
		await Reads(2.5d, 2.5m);
		await Reads(2.6d, 3); // fractional into an integer rounds
	}

	// ---- a value that is already a T ----

	[Fact]
	public static async Task TheMatchingType_IsReturnedAsIs()
	{
		var guid = Guid.NewGuid();
		var moment = new DateTime(2026, 11, 9, 8, 7, 6, DateTimeKind.Utc);
		var offset = new DateTimeOffset(moment);

		await Reads(5, 5);
		await Reads(5L, 5L);
		await Reads(1.5d, 1.5d);
		await Reads(1.5m, 1.5m);
		await Reads(true, true);
		await Reads("text", "text");
		await Reads(guid, guid);
		await Reads(moment, moment);
		await Reads(offset, offset);
		await Reads(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
		await Reads(2, 2 as int?);
		await Reads(Level.Two, Level.Two);

		var bytes = new byte[] { 1, 2, 3 };
		Assert.Same(bytes, Sync(bytes).ExecuteScalar<byte[]>());
		Assert.Same(bytes, await Async(bytes).ExecuteScalarAsync<byte[]>(Ct));

		var boxed = new object();
		Assert.Same(boxed, Sync(boxed).ExecuteScalar<object>());
		Assert.Same(boxed, await Async(boxed).ExecuteScalarAsync<object>(Ct));
	}

	// ---- NULL ----

	[Fact]
	public static async Task NullDbNullAndNoRow_AreDefault_WhateverT()
	{
		foreach (object? none in new object?[] { null, DBNull.Value })
		{
			await Reads(none, 0);
			await Reads(none, 0L);
			await Reads<int?>(none, null);
			await Reads<long?>(none, null);
			await Reads(none, Level.None);
			await Reads(none, default(DateTime));
			await Reads<Guid?>(none, null);
			await Reads<string?>(none, null);
			await Reads<byte[]?>(none, null);
		}
	}

	// ---- text, identifiers, dates and enums ----

	[Fact]
	public static async Task TextAndDates_ConvertLikeAColumn()
	{
		var guid = Guid.NewGuid();
		var moment = new DateTime(2026, 11, 9, 1, 2, 3);

		await Reads(5, "5");
		await Reads(5L, "5");
		await Reads("42", 42);
		await Reads(guid.ToString(), guid);
		await Reads(guid.ToByteArray(), guid);
		await Reads(moment, new DateTimeOffset(moment));
		await Reads(moment, DateOnly.FromDateTime(moment));
		await Reads(TimeSpan.FromHours(1), new TimeOnly(1, 0));
	}

	[Fact]
	public static async Task AnEnum_IsReadFromItsNumberOrItsName()
	{
		await Reads(2, Level.Two);
		await Reads(2L, Level.Two);
		await Reads((byte)3, Small.B);
		await Reads("Two", Level.Two);
		await Reads("two", Level.Two);
		await Reads(5, (Level)5); // as a property, any number
	}

	[Fact]
	public static async Task AValueThatDoesNotConvert_IsAValueMappingException()
	{
		var guid = Guid.NewGuid();

		ValueMappingException text = await Fails<int>("abc");
		Assert.IsType<FormatException>(text.InnerException);
		Assert.Equal(typeof(int), text.TargetType);

		await Fails<Guid>("not a guid");
		await Fails<Guid>(5);
		await Fails<int>(guid);
		await Fails<DateTime>(5);
		await Fails<Level>("nope");

		ValueMappingException bytes = await Fails<string>(new byte[] { 1 });
		Assert.IsType<InvalidCastException>(bytes.InnerException);
	}

	[Fact]
	public static async Task TheFailure_NamesTheScalar_NotAColumn()
	{
		ValueMappingException failure = await Fails<int>("abc");

		Assert.Contains("scalar", failure.Message);
		Assert.DoesNotContain("column", failure.Message);
	}

	// ---- the same as Map<T>() ----

	// Map<T>() reads column 0 of one row of that column type;
	// ExecuteScalar<T>() converts the value that column holds.
	// The same value, or the same failure, sync and async.
	static FakeExecuteReader MapOf(Type column, object? value)
		=> new(() => TypedReader.Create(
			[("v", column)],
			[value]));

	static async Task SameAsMap<T>(
		Type column,
		object? value,
		bool? fails = null)
	{
		T? expected = default;
		ValueMappingException? mapFailure = null;
		try
		{
			expected = MapOf(column, value).Map<T>().ToList()[0];
		}
		catch (ValueMappingException ex)
		{
			mapFailure = ex;
		}

		if (fails is { } mustFail)
			Assert.Equal(mustFail, mapFailure is not null);

		if (mapFailure is null)
		{
			Assert.Equal(expected, Sync(value).ExecuteScalar<T>());
			Assert.Equal(
				expected,
				await Async(value).ExecuteScalarAsync<T>(Ct));
			return;
		}

		SameFailure(
			mapFailure,
			Assert.Throws<ValueMappingException>(
				() => Sync(value).ExecuteScalar<T>()));
		SameFailure(
			mapFailure,
			await Assert.ThrowsAsync<ValueMappingException>(
				async () => await Async(value).ExecuteScalarAsync<T>(Ct)));
	}

	static void SameFailure(
		ValueMappingException map,
		ValueMappingException scalar)
	{
		Assert.Equal(map.InnerException?.GetType(), scalar.InnerException?.GetType());
		Assert.Equal(map.InnerException?.Message, scalar.InnerException?.Message);
		Assert.Equal(map.TargetType, scalar.TargetType);
		Assert.Equal(map.PropertyName, scalar.PropertyName);
	}

	[Fact]
	public static async Task Integers_AreConvertedAsMapConvertsThem()
	{
		await SameAsMap<int>(typeof(long), 5L, fails: false);
		await SameAsMap<int>(typeof(long), int.MaxValue + 0L, fails: false);
		await SameAsMap<int>(typeof(long), int.MaxValue + 1L, fails: true);
		await SameAsMap<int>(typeof(long), int.MinValue - 1L, fails: true);
		await SameAsMap<long>(typeof(long), 5L, fails: false);
		await SameAsMap<long>(typeof(int), 5, fails: false);
		await SameAsMap<short>(typeof(int), 70_000, fails: true);
		await SameAsMap<byte>(typeof(int), -1, fails: true);
		await SameAsMap<uint>(typeof(int), -1, fails: true);
		await SameAsMap<ulong>(typeof(long), -1L, fails: true);
		await SameAsMap<int>(typeof(short), (short)5, fails: false);
		await SameAsMap<long>(typeof(byte), (byte)5, fails: false);
		await SameAsMap<sbyte>(typeof(byte), (byte)200, fails: true);
		await SameAsMap<int>(typeof(ulong), 5ul, fails: false); // no typed getter
		await SameAsMap<long>(typeof(ulong), ulong.MaxValue, fails: true);
		await SameAsMap<int>(typeof(sbyte), (sbyte)-5, fails: false);
		await SameAsMap<int?>(typeof(long), 5L, fails: false);
		await SameAsMap<int?>(typeof(long), 5_000_000_000L, fails: true);
		await SameAsMap<long?>(typeof(int), 5, fails: false);
		await SameAsMap<int?>(typeof(int), 5, fails: false);
	}

	[Fact]
	public static async Task Fractions_AreConvertedAsMapConvertsThem()
	{
		await SameAsMap<decimal>(typeof(long), 5L, fails: false);
		await SameAsMap<double>(typeof(int), 5, fails: false);
		await SameAsMap<float>(typeof(long), 5L, fails: false);
		await SameAsMap<double>(typeof(float), 1.5f, fails: false);
		await SameAsMap<double>(typeof(decimal), 1.5m, fails: false);
		await SameAsMap<decimal>(typeof(double), 2.5d, fails: false);
		await SameAsMap<double?>(typeof(int), 5, fails: false);
		await SameAsMap<int>(typeof(decimal), 2.6m, fails: false);
		await SameAsMap<int?>(typeof(decimal), 2.6m, fails: false);
		await SameAsMap<long>(typeof(decimal), 42m, fails: false);
		await SameAsMap<int>(typeof(decimal), 5_000_000_000m, fails: true);
		await SameAsMap<int>(typeof(double), 2.6d, fails: false);
		await SameAsMap<long>(typeof(double), double.MaxValue, fails: true);
	}

	[Fact]
	public static async Task TextDatesAndEnums_AreConvertedAsMapConvertsThem()
	{
		var guid = Guid.NewGuid();
		var moment = new DateTime(2026, 11, 9, 8, 7, 6, DateTimeKind.Utc);

		await SameAsMap<string>(typeof(int), 5, fails: false);
		await SameAsMap<string>(typeof(long), 5L, fails: false);
		await SameAsMap<string>(typeof(Guid), guid, fails: false);
		await SameAsMap<string>(typeof(DateTime), moment, fails: false);
		await SameAsMap<string>(typeof(byte[]), new byte[] { 1 }, fails: true);
		await SameAsMap<int>(typeof(string), "42", fails: false);
		await SameAsMap<int>(typeof(string), "abc", fails: true);
		await SameAsMap<Guid>(typeof(string), guid.ToString(), fails: false);
		await SameAsMap<Guid>(typeof(string), "x", fails: true);
		await SameAsMap<Guid>(typeof(byte[]), guid.ToByteArray(), fails: false);
		await SameAsMap<Guid>(typeof(int), 5, fails: true);
		await SameAsMap<int>(typeof(Guid), guid, fails: true);
		await SameAsMap<DateTime>(typeof(DateTime), moment, fails: false);
		await SameAsMap<DateTimeOffset>(typeof(DateTime), moment, fails: false);
		await SameAsMap<DateOnly>(typeof(DateTime), moment, fails: false);
		await SameAsMap<TimeOnly>(typeof(TimeSpan), TimeSpan.FromHours(1), fails: false);
		await SameAsMap<DateTime>(typeof(int), 5, fails: true);
		await SameAsMap<bool>(typeof(int), 1, fails: false);
		await SameAsMap<int>(typeof(bool), true, fails: false);
		await SameAsMap<int>(typeof(char), 'x', fails: false);
		await SameAsMap<byte[]>(typeof(byte[]), new byte[] { 1, 2 }, fails: false);
		await SameAsMap<Level>(typeof(int), 2, fails: false);
		await SameAsMap<Level>(typeof(int), 5, fails: false);
		await SameAsMap<Level>(typeof(long), 2L, fails: false);
		await SameAsMap<Level>(typeof(long), 5_000_000_000L); // whatever Map does
		await SameAsMap<Level>(typeof(string), "two", fails: false);
		await SameAsMap<Level>(typeof(string), "nope", fails: true);
		await SameAsMap<Small>(typeof(byte), (byte)3, fails: false);
		await SameAsMap<Small>(typeof(int), 300); // whatever Map does
		await SameAsMap<Level?>(typeof(int), 2, fails: false);
	}

	// ---- the call ----

	[Fact]
	public static async Task ACancelledToken_CancelsTheCall()
	{
		using CancellationTokenSource cancelled = new();
		await cancelled.CancelAsync();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Async(5L)
				.ExecuteScalarAsync<int>(cancelled.Token));

		// The command's own token cancels it too.
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await Async(5L)
				.UseCancellationToken(cancelled.Token)
				.ExecuteScalarAsync<int>(Ct));

		// A live token changes nothing.
		Assert.Equal(
			5,
			await Async(5L)
				.UseCancellationToken(Ct)
				.ExecuteScalarAsync<int>(Ct));
	}

	// The value is converted once the command has released the connection:
	// a conversion that fails leaves the connection as the call found it.
	[Fact]
	public static void AFailedConversion_LeavesTheConnectionAsTheCallFoundIt()
	{
		var probe = new ConnectionProbe(ConnectionState.Closed);
		probe.Command.ExecuteScalar().Returns(_ => 5_000_000_000L);

		Assert.Throws<ValueMappingException>(
			() => probe.Connection.Command("SELECT 1").ExecuteScalar<int>());

		Assert.Equal(ConnectionState.Closed, probe.State);
		Assert.Contains("Open", probe.Calls);
		Assert.Contains("Close", probe.Calls);
		Assert.DoesNotContain("DisposeConnection", probe.Calls);
	}

	// The transform overloads are not touched:
	// they receive the provider's value as it is.
	[Fact]
	public static async Task TheTransformOverloads_StillReceiveTheRawValue()
	{
		Assert.Equal(
			typeof(long),
			Sync(5L).ExecuteScalar(value => value!.GetType()));
		Assert.Equal(
			typeof(long),
			await Async(5L)
				.ExecuteScalarAsync(value => value!.GetType(), Ct));
	}
}
