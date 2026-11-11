using static Open.Database.Extensions.Tests.ReaderFixtures;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A command's ToList<T>()
// and the other terminals are Map<T>() then that terminal;
// a single value such as int is read from the first column.
// These tests pin which types, how the column is converted, what a NULL is,
// which column is read, and that every terminal equals its Map<T>() twin.
[ExcludeFromCodeCoverage]
public static class SingleValueTerminalTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static T Block<T>(ValueTask<T> task)
		=> task.AsTask().GetAwaiter().GetResult();

	public enum Level { None = 0, One = 1, Two = 2 }
	public enum Small : byte { A = 0, B = 3 }

	public sealed record PostRow
	{
		public int Id { get; set; }
		public string? Title { get; set; }
	}

	// One column named "v" of the given type,
	// one row per cell (a null is a NULL).
	static FakeExecuteReader Cmd(Type column, params object?[] cells)
		=> new(() => TypedReader.Create(
			[("v", column)],
			[.. cells.Select(c => new object?[] { c })]));

	static FakeExecuteReader Cmd(
		ImmutableArray<ColumnDef> columns,
		params object?[][] rows)
		=> new(() => TypedReader.Create(columns, rows));

	// Every terminal, async, gives the answer of the Map<T>() twin.
	static void Check<T>(
		Type column,
		object? cell,
		T expected)
	{
		FakeExecuteReader cmd = Cmd(column, cell);
		Assert.Equal([expected], cmd.Map<T>().ToList());
		Assert.Equal([expected], cmd.Map<T>().ToArray());
		Assert.Equal(expected, cmd.Map<T>().FirstOrDefault());
		Assert.Equal(expected, cmd.Map<T>().SingleOrDefault());
		Assert.Single(cmd.Map<T>().ToHashSet());
		Assert.Equal([expected], cmd.Map<T>().ToImmutableArray());
		Assert.Equal(
			[expected],
			cmd.ToListAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
	}

	// ---- 1. every single-value type, from a column of its own type ----

	[Fact]
	public static void EverySingleValueType_IsReadFromAColumnOfItsOwnType()
	{
		var guid = Guid.NewGuid();
		var moment = new DateTime(2026, 11, 9, 8, 7, 6, DateTimeKind.Utc);
		var offset = new DateTimeOffset(moment);

		Check(typeof(int), 5, 5);
		Check(typeof(long), 5L, 5L);
		Check(typeof(short), (short)5, (short)5);
		Check(typeof(byte), (byte)5, (byte)5);
		Check(typeof(sbyte), (sbyte)-5, (sbyte)-5);
		Check(typeof(ushort), (ushort)5, (ushort)5);
		Check(typeof(uint), 5u, 5u);
		Check(typeof(ulong), 5ul, 5ul);
		Check(typeof(bool), true, true);
		Check(typeof(char), 'x', 'x');
		Check(typeof(float), 1.5f, 1.5f);
		Check(typeof(double), 1.5d, 1.5d);
		Check(typeof(decimal), 1.5m, 1.5m);
		Check(typeof(string), "text", "text");
		Check(typeof(Guid), guid, guid);
		Check(typeof(DateTime), moment, moment);
		Check(typeof(DateTimeOffset), offset, offset);
		Check(
			typeof(TimeSpan),
			TimeSpan.FromMinutes(5),
			TimeSpan.FromMinutes(5));
		Check(typeof(byte[]), new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 });
		Check(
			typeof(DateOnly),
			new DateOnly(2026, 11, 9),
			new DateOnly(2026, 11, 9));
		Check(typeof(TimeOnly), new TimeOnly(8, 7, 6), new TimeOnly(8, 7, 6));
		Check(typeof(int), 2, Level.Two);
		Check(typeof(byte), (byte)3, Small.B);
	}

	[Fact]
	public static void ANullableOfEverySingleValue_IsReadFromAColumnOfTheUnderlyingType()
	{
		var guid = Guid.NewGuid();
		Check<int?>(typeof(int), 5, 5);
		Check<long?>(typeof(long), 5L, 5L);
		Check<bool?>(typeof(bool), true, true);
		Check<double?>(typeof(double), 1.5d, 1.5d);
		Check<decimal?>(typeof(decimal), 1.5m, 1.5m);
		Check<Guid?>(typeof(Guid), guid, guid);
		Check<DateTime?>(
			typeof(DateTime),
			new DateTime(2026, 1, 2),
			new DateTime(2026, 1, 2));
		Check<TimeSpan?>(
			typeof(TimeSpan),
			TimeSpan.FromSeconds(3),
			TimeSpan.FromSeconds(3));
		Check<Level?>(typeof(int), 1, Level.One);
		Check<DateOnly?>(
			typeof(DateOnly),
			new DateOnly(2026, 1, 2),
			new DateOnly(2026, 1, 2));
	}

	// ---- 2. NULL ----

	[Fact]
	public static void ANull_IsDefaultForANonNullableValueType_AndNullForTheRest()
	{
		Check(typeof(int), null, 0);
		Check(typeof(sbyte), null, (sbyte)0); // a column type with no typed getter: the general path
		Check(typeof(decimal), null, 0); // a column that has to be converted
		Check(typeof(Guid), null, Guid.Empty);
		Check(typeof(DateTime), null, default(DateTime));
		Check(typeof(int), null, Level.None);
		Check<int?>(typeof(int), null, null);
		Check<Guid?>(typeof(string), null, null);
		Check<string?>(typeof(string), null, null);
		Check<string?>(typeof(int), null, null);
		Check<byte[]?>(typeof(byte[]), null, null);
		Check<DateOnly>(typeof(DateOnly), null, default);
	}

	[Fact]
	public static void ANull_InTheFirstRowAndInALaterRow_IsHandledPerRow()
	{
		FakeExecuteReader cmd = Cmd(typeof(int), null, 5, null, 7);
		Assert.Equal([0, 5, 0, 7], cmd.Map<int>().ToList());
		Assert.Equal([null, 5, null, 7], cmd.Map<int?>().ToList());
		Assert.Equal([null, "5", null, "7"], cmd.Map<string?>().ToList());
	}

	// ---- 3. conversions: as a property of that type is read ----

	[Fact]
	public static void ALongColumn_ConvertsToInt_WithARangeCheck()
	{
		Check(typeof(long), 5L, 5);

		ValueMappingException failure = Assert.Throws<ValueMappingException>(
			() => Cmd(typeof(long), 5_000_000_000L).Map<int>().ToList());
		Assert.IsType<OverflowException>(failure.InnerException);
		Assert.Equal("v", failure.ColumnName);
		Assert.Equal(typeof(int), failure.TargetType);
		Assert.Equal("", failure.PropertyName);
		Assert.Contains("'v'", failure.Message);
	}

	[Fact]
	public static void Numbers_WidenAndConvert()
	{
		Check(typeof(int), 5, 5L);
		Check(typeof(int), 5, 5d);
		Check(typeof(decimal), 2.6m, 3); // Convert rounds to nearest
		Check(typeof(int), 5, "5");
		Check(typeof(short), (short)5, 5);
	}

	[Fact]
	public static void TextAndDates_ConvertLikeAProperty()
	{
		var guid = Guid.NewGuid();
		Check(typeof(string), guid.ToString(), guid);
		Check(typeof(string), "two", Level.Two);
		Check(typeof(string), "42", 42);
		var moment = new DateTime(2026, 11, 9, 1, 2, 3);
		Check(typeof(DateTime), moment, new DateTimeOffset(moment));
		Check(typeof(DateTime), moment, DateOnly.FromDateTime(moment));
		Check(typeof(TimeSpan), TimeSpan.FromHours(1), new TimeOnly(1, 0));
	}

	[Fact]
	public static void AValueThatDoesNotConvert_IsAValueMappingException_NamingTheColumn()
	{
		ValueMappingException text = Assert.Throws<ValueMappingException>(
			() => Cmd(typeof(string), "abc").Map<int>().ToList());
		Assert.IsType<FormatException>(text.InnerException);
		Assert.Equal("v", text.ColumnName);

		Assert.Throws<ValueMappingException>(
			() => Cmd(typeof(byte[]), new byte[] { 1 }).Map<string>().ToList()); // ConvertValue refuses a byte array into a string
		Assert.Throws<ValueMappingException>(
			() => Cmd(typeof(long), 5L).Map<nint>().ToList()); // nint has no typed getter and Convert.ChangeType cannot make one
	}

	// ---- 4. more than one column ----

	[Fact]
	public static void OnlyTheFirstColumn_IsRead_WhateverItIsNamed()
	{
		FakeExecuteReader a = Cmd(
			[("Id", typeof(int)), ("v", typeof(int)), ("Name", typeof(string))],
			[1, 2, "x"],
			[3, 4, "y"]);
		Assert.Equal([1, 3], a.Map<int>().ToList());

		FakeExecuteReader b = Cmd(
			[("v", typeof(int)), ("Id", typeof(int))],
			[2, 1],
			[4, 3]);
		Assert.Equal([2, 4], b.Map<int>().ToList()); // swapping the columns changes the result
		Assert.Equal(["1", "3"], a.Map<string>().ToList());
	}

	[Fact]
	public static void AReaderWithNoColumns_FailsAtTheFirstRow_NotHangs()
	{
		FakeExecuteReader none = Cmd([], [], []);
		Assert.Throws<IndexOutOfRangeException>(() => none.Map<int>().ToList());
		Assert.Equal([], Cmd([]).Map<int>().ToList()); // and with no rows nothing is read at all
	}

	// ---- 5. empty and single results ----

	[Fact]
	public static void AnEmptyResult_IsEmpty_AndFirstOrDefaultIsDefault()
	{
		FakeExecuteReader cmd = Cmd(typeof(int));
		Assert.Empty(cmd.Map<int>().ToList());
		Assert.Empty(cmd.Map<int>().ToArray());
		Assert.Empty(cmd.Map<int>().ToImmutableArray());
		Assert.Empty(cmd.Map<int>().ToHashSet());
		Assert.Equal(0, cmd.Map<int>().FirstOrDefault());
		Assert.Null(cmd.Map<int?>().FirstOrDefault());
		Assert.Equal(0, cmd.Map<int>().SingleOrDefault());
		Assert.Empty(Block(cmd.ToListAsync<int>(Ct)));
	}

	[Fact]
	public static void SingleOrDefault_WithTwoRows_Throws_AsMapDoes()
	{
		FakeExecuteReader cmd = Cmd(typeof(int), 1, 2);
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<int>().SingleOrDefault());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<int>().SingleOrDefault());
		Assert.Equal(1, cmd.Map<int>().FirstOrDefault());
		Assert.Equal([1, 2], cmd.Map<int>().ToHashSet().Order());
		Assert.Equal(
			["a", "A"],
			Cmd(typeof(string), "a", "A")
				.Map<string>()
				.ToHashSet(StringComparer.Ordinal)
				.Order(StringComparer.Ordinal)
				.Reverse());
		Assert.Single(
			Cmd(typeof(string), "a", "A")
				.Map<string>()
				.ToHashSet(StringComparer.OrdinalIgnoreCase));
	}

	// ---- 6. parity with Map<T>() for every terminal,
	// a single value and a row model ----

	static void Drop<T>(ValueTask<T> _) { }

	static void Parity<T>(FakeExecuteReader cmd)
	{
		IEqualityComparer<T> comparer = EqualityComparer<T>.Default;
		Assert.Equal(
			cmd.Map<T>().ToListAsync(Ct).AsTask().GetAwaiter().GetResult(),
			cmd.ToListAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
		Assert.Equal(
			cmd.Map<T>().ToArrayAsync(Ct).AsTask().GetAwaiter().GetResult(),
			cmd.ToArrayAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
		Assert.Equal(
			cmd
				.Map<T>()
				.ToImmutableArrayAsync(Ct)
				.AsTask()
				.GetAwaiter()
				.GetResult(),
			cmd.ToImmutableArrayAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
		Assert.Equal(
			cmd
				.Map<T>()
				.FirstOrDefaultAsync(Ct)
				.AsTask()
				.GetAwaiter()
				.GetResult(),
			cmd.FirstOrDefaultAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
		Assert.Equal(
			cmd
				.Map<T>()
				.SingleOrDefaultAsync(Ct)
				.AsTask()
				.GetAwaiter()
				.GetResult(),
			cmd.SingleOrDefaultAsync<T>(Ct).AsTask().GetAwaiter().GetResult());
		Assert.True(
			cmd
				.Map<T>()
				.ToHashSetAsync(comparer, Ct)
				.AsTask()
				.GetAwaiter()
				.GetResult()
				.SetEquals(
					cmd
						.ToHashSetAsync(comparer, Ct)
						.AsTask()
						.GetAwaiter()
						.GetResult()));
	}

	[Fact]
	public static void EveryTerminal_EqualsItsMapTwin()
	{
		ImmutableArray<ColumnDef> columns = [
			("Id", typeof(int)),
			("Title", typeof(string))
		];
		FakeExecuteReader one = Cmd(columns, [1, "a"]); // SingleOrDefault needs one row
		FakeExecuteReader many = Cmd(columns, [1, "a"], [2, null], [3, "c"]);

		Parity<int>(one);

		Parity<int?>(one);
		Parity<string>(one);
		Parity<PostRow>(one);
		Assert.Equal(
			[
				new() { Id = 1, Title = "a" },
				new() { Id = 2 },
				new() { Id = 3, Title = "c" }
			],
			many.Map<PostRow>().ToList());
	}

	// ---- 7. a row model maps by name;
	// a type that cannot be mapped fails at the call ----

	public interface IShapeless;

	[Fact]
	public static void ARowModel_MapsByColumnName()
	{
		FakeExecuteReader cmd = Cmd(
			[("Title", typeof(string)), ("Id", typeof(int))],
			["a", 1]);
		Assert.Equal(
			[new() { Id = 1, Title = "a" }],
			cmd.Map<PostRow>().ToList());
		Assert.Equal(
			new PostRow { Id = 1, Title = "a" },
			cmd.Map<PostRow>().FirstOrDefault());
	}

	[Fact]
	public static void AnUnmappableType_ThrowsAtTheCall_BeforeAnythingRuns()
	{
		FakeExecuteReader cmd = Cmd(typeof(int), 1);
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().ToList());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().ToArray());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().ToImmutableArray());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().FirstOrDefault());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().SingleOrDefault());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<IShapeless>().ToHashSet());
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.ToListAsync<IShapeless>(Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.ToArrayAsync<IShapeless>(Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.ToImmutableArrayAsync<IShapeless>(Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.FirstOrDefaultAsync<IShapeless>(Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.SingleOrDefaultAsync<IShapeless>(Ct)));
		Assert.Throws<InvalidOperationException>(
			() => Drop(cmd.ToHashSetAsync<IShapeless>(cancellationToken: Ct)));
		Assert.Equal(0, cmd.ExecuteCount);
	}

	[Fact]
	public static void ANullCommand_IsAnArgumentNullException_ForEveryType()
	{
		IExecuteReader? none = null;
		Assert.Equal(
			"command",
			Assert.Throws<ArgumentNullException>(
					() => none!.Map<int>().ToList())
				.ParamName);
		Assert.Equal(
			"command",
			Assert.Throws<ArgumentNullException>(
					() => Drop(none!.ToListAsync<PostRow>(Ct)))
				.ParamName);
		Assert.Equal(
			"command",
			Assert.Throws<ArgumentNullException>(
					() => none!.Map<int>())
				.ParamName);
		Assert.Equal(
			"command",
			Assert.Throws<ArgumentNullException>(
					() => none!.Map<IShapeless>())
				.ParamName);
	}

	// ---- 8. Map<int>() works; what takes a model still does not ----

	[Fact]
	public static void MapOfASingleValue_ReadsTheFirstColumn()
	{
		FakeExecuteReader cmd = Cmd(typeof(long), 1L, 2L);
		Assert.Equal([1, 2], cmd.Map<int>().ToList());
		Assert.Equal([2], cmd.Map<int>().Skip(1).Take(1).ToList());
		Assert.Equal(1, cmd.Map<int>().SingleResult().FirstOrDefault());
		Assert.Throws<InvalidOperationException>(
			() => cmd.Map<int>(new KeyValuePair<string, string?>("Id", "Id")));
	}

	// ---- 9. read-through ----

	[Fact]
	public static void AFailureInALaterResultSet_IsThrown_UnlessSingleResult()
	{
		static FakeExecuteReader failing()
			=> new(() => new MultiSetReader(Rows(2), 2, failAtCall: 2));

		Assert.Throws<LaterSetFailure>(() => failing().Map<int>().ToList());
		Assert.Throws<LaterSetFailure>(() => failing().Map<int>().ToList());
		Assert.Equal([1, 2], failing().Map<int>().SingleResult().ToList());
	}

	// ---- 10. tokens ----

	public static TheoryData<string> AsyncTerminals
		=> [
			"ToListAsync",
			"ToArrayAsync",
			"ToImmutableArrayAsync",
			"FirstOrDefaultAsync",
			"SingleOrDefaultAsync",
			"ToHashSetAsync"
		];

	static Task RunAsync(
		FakeExecuteReader cmd,
		string terminal,
		CancellationToken token)
		=> terminal switch
		{
			"ToListAsync" => cmd.ToListAsync<int>(token).AsTask(),
			"ToArrayAsync" => cmd.ToArrayAsync<int>(token).AsTask(),
			"ToImmutableArrayAsync"
				=> cmd.ToImmutableArrayAsync<int>(token).AsTask(),
			"FirstOrDefaultAsync"
				=> cmd.FirstOrDefaultAsync<int>(token).AsTask(),
			"SingleOrDefaultAsync"
				=> cmd.SingleOrDefaultAsync<int>(token).AsTask(),
			"ToHashSetAsync"
				=> cmd.ToHashSetAsync<int>(cancellationToken: token).AsTask(),
			_ => throw new ArgumentOutOfRangeException(nameof(terminal)),
		};

#pragma warning disable xUnit1051 // This test passes no token on purpose: it checks what the reader is handed with none.
	[Theory, MemberData(nameof(AsyncTerminals))]
	public static async Task EveryAsyncTerminal_GovernsTheCallByTheCallTokenAndTheCommandsTogether(
		string terminal)
	{
		// No token anywhere: the reader is handed none.
		FakeExecuteReader none = Cmd(typeof(int), 1);
		await RunAsync(none, terminal, default);
		Assert.Equal(default, Assert.Single(none.ReaderTokens));

		// Only the call's token, or only the command's: that token itself.
		using var own = new CancellationTokenSource();
		using var call = new CancellationTokenSource();
		FakeExecuteReader onlyCall = Cmd(typeof(int), 1);
		await RunAsync(onlyCall, terminal, call.Token);
		Assert.Equal(call.Token, Assert.Single(onlyCall.ReaderTokens));

		FakeExecuteReader onlyCommand = Cmd(typeof(int), 1);
		onlyCommand.CancellationToken = own.Token;
		await RunAsync(onlyCommand, terminal, default);
		Assert.Equal(own.Token, Assert.Single(onlyCommand.ReaderTokens));

		// Both live: one linked token, which is neither of them.
		FakeExecuteReader both = Cmd(typeof(int), 1);
		both.CancellationToken = own.Token;
		await RunAsync(both, terminal, call.Token);
		CancellationToken linked = Assert.Single(both.ReaderTokens);
		Assert.True(linked.CanBeCanceled);
		Assert.NotEqual(own.Token, linked);
		Assert.NotEqual(call.Token, linked);

		// Either one canceled before the call stops it.
		using var canceled = new CancellationTokenSource();
		canceled.Cancel();
		FakeExecuteReader callCanceled = Cmd(typeof(int), 1);
		callCanceled.CancellationToken = own.Token;
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => RunAsync(callCanceled, terminal, canceled.Token));

		FakeExecuteReader commandCanceled = Cmd(typeof(int), 1);
		commandCanceled.CancellationToken = canceled.Token;
		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			() => RunAsync(commandCanceled, terminal, call.Token));
	}
#pragma warning restore xUnit1051

	// ---- 11. both reader routes ----

	static IDataReader Plain(Type column, params object[] values)
	{
		IDataReader reader = Substitute.For<IDataReader>();
		reader.FieldCount.Returns(1);
		reader.GetName(0).Returns("v");
		reader.GetFieldType(0).Returns(column);
		bool[] more = [.. values.Select(_ => true), false];
		reader.Read().Returns(more[0], more[1..]);
		int i = -1;
		reader.When(r => r.Read()).Do(_ => i++);
		reader.IsDBNull(0).Returns(_ => values[i] is DBNull);
		reader.GetInt32(0).Returns(_ => (int)values[i]);
		reader.GetInt64(0).Returns(_ => (long)values[i]);
		reader.GetValue(0).Returns(_ => values[i]);
		return reader;
	}

	[Fact]
	public static void APlainIDataReader_GivesTheSameAnswersAsADbDataReader()
	{
		Assert.Equal(
			[1, 2],
			new PlainReaderCommand(() => Plain(typeof(int), 1, 2))
				.Map<int>()
				.ToList());
		Assert.Equal(
			[1, 2],
			new PlainReaderCommand(() => Plain(typeof(long), 1L, 2L))
				.Map<int>()
				.ToArray());
		Assert.Equal(
			[1, 0],
			new PlainReaderCommand(() => Plain(typeof(int), 1, DBNull.Value))
				.Map<int>()
				.ToList());
		Assert.Equal(
			[1, null],
			new PlainReaderCommand(() => Plain(typeof(int), 1, DBNull.Value))
				.Map<int?>()
				.ToList());
		Assert.Equal(
			["1", "2"],
			new PlainReaderCommand(() => Plain(typeof(int), 1, 2))
				.Map<string>()
				.ToList());
	}

	// ---- 12. the reader is chosen per result set, from the column's type ----

	[Fact]
	public static void OneT_OverColumnsOfDifferentTypes_ReadsEachCorrectly()
	{
		Assert.Equal([1], Cmd(typeof(int), 1).Map<int>().ToList());
		Assert.Equal([2], Cmd(typeof(long), 2L).Map<int>().ToList());
		Assert.Equal([3], Cmd(typeof(int), 3).Map<int>().ToList());
		Assert.Equal([4], Cmd(typeof(string), "4").Map<int>().ToList());
		Assert.Equal([5], Cmd(typeof(long), 5L).Map<int>().ToList());
	}

	// A provider such as SQLite reports the type of the current row's value:
	// the getter chosen for the first row serves the rest of the result set.
	sealed class ShiftingReader(
		Type first,
		object?[] cells)
		: TypedReader(
			[("v", typeof(object))],
			[.. cells.Select(c => new object?[] { c })])
	{
		int _reported;
		public override Type GetFieldType(int ordinal)
			=> _reported++ == 0 ? first : typeof(string);
		public override long GetInt64(int ordinal)
			=> Convert.ToInt64(
				GetValue(ordinal),
				System.Globalization.CultureInfo.InvariantCulture);
	}

	[Fact]
	public static void ALaterRowOfAnotherType_IsReadWithTheFirstRowsGetter()
	{
		// The provider coerces:
		// the long getter serves a text row holding digits.
		Assert.Equal(
			[1, 22],
			new FakeExecuteReader(
					() => new ShiftingReader(typeof(long), [1L, "22"]))
				.Map<int>()
				.ToList());

		// The provider throws: a ValueMappingException, as for a property.
		Assert.Throws<ValueMappingException>(
			() => new FakeExecuteReader(
					() => new ShiftingReader(typeof(int), [1, "x"]))
				.Map<int>()
				.ToList());
	}
}
