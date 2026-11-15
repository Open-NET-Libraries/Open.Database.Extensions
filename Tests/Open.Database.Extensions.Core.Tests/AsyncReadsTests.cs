using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>A reader that counts which way its rows were pulled: <c>Read</c> or <c>ReadAsync</c>.</summary>
[ExcludeFromCodeCoverage]
internal sealed class CountingReader(int rows)
	: TypedReader([("Id", typeof(int))], [.. Enumerable.Range(1, rows).Select(i => (object?[])[i])])
{
	public int Reads { get; private set; }

	public int ReadAsyncs { get; private set; }

	public override bool Read()
	{
		Reads++;
		return base.Read();
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		ReadAsyncs++;
		cancellationToken.ThrowIfCancellationRequested();
		return Task.FromResult(base.Read());
	}
}

/// <summary>
/// One read rule: a terminal reads its rows with <c>Read</c> by default, async terminals included,
/// and with <c>ReadAsync</c> only after <c>AsyncReads()</c>.
/// </summary>
[ExcludeFromCodeCoverage]
public static class AsyncReadsTests
{
	const int Rows = 5;

	public enum Path
	{
		FluentCommand,
		FluentCommandViewOptIn,
		DbCommandAsIDbCommand,
		DbCommandSingleResultFirst,
		FluentSingleResultFirst,
		DataReader,
	}

	// One probe per path: run the async terminal, then read how the rows were pulled.
	sealed record Probe(Func<Task<List<int>>> Run, Func<int> Reads, Func<int> ReadAsyncs);

	static Probe Build(Path path, bool optIn)
	{
		CancellationToken token = TestContext.Current.CancellationToken;
		switch (path)
		{
			case Path.FluentCommand:
			case Path.FluentCommandViewOptIn:
			case Path.FluentSingleResultFirst:
			{
				var rig = new TokenRig { Rows = Rows };
				int syncReads = 0;
				rig.BeforeRead = _ => syncReads++;
				var command = new ExpressiveDbCommand(
					new TokenConnection(rig, ConnectionState.Open), CommandType.Text, "SELECT Id");
				return new Probe(async () =>
				{
					MappedQuery<int> view = path switch
					{
						Path.FluentCommand => (optIn ? command.AsyncReads() : command).Map<int>(),
						Path.FluentCommandViewOptIn => optIn ? command.Map<int>().AsyncReads() : command.Map<int>(),
						_ => optIn ? command.SingleResult().Map<int>().AsyncReads() : command.SingleResult().Map<int>(),
					};
					return await view.ToListAsync(token);
				}, () => syncReads - rig.Read.Count, () => rig.Read.Count); // TokenReader.ReadAsync calls Read, so those are subtracted
			}

			case Path.DbCommandAsIDbCommand:
			case Path.DbCommandSingleResultFirst:
			{
				var reader = new CountingReader(Rows);
				var connection = new FakeDbConnection();
				connection.SetState(ConnectionState.Open);
				IDbCommand command = new FakeDbCommand(connection, () => reader);
				return new Probe(() =>
				{
					MappedCommand<int> view = path == Path.DbCommandSingleResultFirst
						? command.SingleResult().Map<int>()
						: command.Map<int>();
					if (optIn) view = view.AsyncReads();
					return view.ToListAsync(token).AsTask();
				}, () => reader.Reads, () => reader.ReadAsyncs);
			}

			case Path.DataReader:
			{
				var reader = new CountingReader(Rows);
				return new Probe(
					() => reader.Map<int>().ToListAsync(token).AsTask(),
					() => reader.Reads, () => reader.ReadAsyncs);
			}

			default:
				throw new ArgumentOutOfRangeException(nameof(path));
		}
	}

	// T-RM1: Read by default on every path.
	[Theory]
	[InlineData(Path.FluentCommand)]
	[InlineData(Path.FluentCommandViewOptIn)]
	[InlineData(Path.DbCommandAsIDbCommand)]
	[InlineData(Path.DbCommandSingleResultFirst)]
	[InlineData(Path.FluentSingleResultFirst)]
	[InlineData(Path.DataReader)]
	public static async Task AnAsyncTerminal_ReadsWithRead_ByDefault(Path path)
	{
		Probe probe = Build(path, optIn: false);
		List<int> rows = await probe.Run();

		Assert.Equal(Rows, rows.Count);
		Assert.True(probe.Reads() >= Rows, $"{path}: Read called {probe.Reads()} times");
		Assert.Equal(0, probe.ReadAsyncs());
	}

	// T-RM1: ReadAsync only with AsyncReads().
	[Theory]
	[InlineData(Path.FluentCommand)]
	[InlineData(Path.FluentCommandViewOptIn)]
	[InlineData(Path.DbCommandAsIDbCommand)]
	[InlineData(Path.DbCommandSingleResultFirst)]
	[InlineData(Path.FluentSingleResultFirst)]
	public static async Task AnAsyncTerminal_ReadsWithReadAsync_OnlyAfterAsyncReads(Path path)
	{
		Probe probe = Build(path, optIn: true);
		List<int> rows = await probe.Run();

		Assert.Equal(Rows, rows.Count);
		Assert.True(probe.ReadAsyncs() >= Rows, $"{path}: ReadAsync called {probe.ReadAsyncs()} times");
		Assert.Equal(0, probe.Reads());
	}

	// A sync terminal never reads async, whatever the opt-in.
	[Fact]
	public static void ASyncTerminal_ReadsWithRead_EvenAfterAsyncReads()
	{
		var reader = new CountingReader(Rows);
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		IDbCommand command = new FakeDbCommand(connection, () => reader);

		List<int> rows = command.Map<int>().AsyncReads().ToList();

		Assert.Equal(Rows, rows.Count);
		Assert.Equal(0, reader.ReadAsyncs);
		Assert.True(reader.Reads >= Rows);
	}

	// The opt-in composes with Skip, Take and SingleResult in any order, on every staging type.
	[Fact]
	public static async Task AsyncReads_OnceOnEachStagingType()
	{
		foreach (int shape in new[] { 0, 1, 2, 3 })
		{
			var reader = new CountingReader(Rows);
			var connection = new FakeDbConnection();
			connection.SetState(ConnectionState.Open);
			IDbCommand command = new FakeDbCommand(connection, () => reader);
			CancellationToken token = TestContext.Current.CancellationToken;

			List<int> rows = shape switch
			{
				0 => await command.Map<int>().AsyncReads().ToListAsync(token),
				1 => await command.Map<int>().Skip(1).AsyncReads().ToListAsync(token),
				2 => await command.Map<int>().Take(3).AsyncReads().ToListAsync(token),
				_ => await command.Map<int>().Skip(1).Take(2).AsyncReads().SingleResult().ToListAsync(token),
			};

			Assert.NotEmpty(rows);
			Assert.True(reader.ReadAsyncs > 0, $"shape {shape}");
			Assert.Equal(0, reader.Reads);
		}
	}

	// The call shapes compile on all five receivers and return the receiver's own type.
	[Fact]
	public static void AsyncReads_ReturnsEachReceiversOwnType()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		var rig = new TokenRig();
		var fluent = new ExpressiveDbCommand(new TokenConnection(rig), CommandType.Text, "SELECT Id");
		ExpressiveDbCommand a = fluent.AsyncReads();
		MappedQuery<int> b = fluent.Map<int>().AsyncReads();
		MappedQueryWithSkip<int> c = fluent.Map<int>().Skip(1).AsyncReads();
		MappedQueryWithTake<int> d = fluent.Map<int>().Take(1).AsyncReads();
		MappedCommand<int> e = ((IDbCommand)new FakeDbCommand(connection, () => new CountingReader(1))).Map<int>().AsyncReads();

		Assert.Same(fluent, a);
		Assert.NotNull(e.ToString());
		Assert.NotNull(b.ToString());
		Assert.NotNull(c.ToString());
		Assert.NotNull(d.ToString());
	}

	// The removed members no longer exist.
	[Fact]
	public static void TheRemovedMembers_NoLongerExist()
	{
		Type type = typeof(ExpressiveDbCommand);
		Assert.Null(type.GetMethod("EnableAsyncRead"));
		Assert.Null(type.GetProperty("UseAsyncRead"));
		Assert.Null(type.GetMethod("get_UseAsyncRead"));
		Assert.Null(type.GetMethod("set_UseAsyncRead"));
	}

	// The table view completes synchronously and reads with Read (pin).
	[Fact]
	public static async Task ADataTable_ReadsSynchronously_AsyncTerminalsIncluded()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		for (int i = 1; i <= Rows; i++) table.Rows.Add(i);

		ValueTask<List<int>> task = table.Map<int>().ToListAsync(TestContext.Current.CancellationToken);

		Assert.True(task.IsCompletedSuccessfully);
		Assert.Equal(Rows, (await task).Count);
	}

	// T-AM1 (pin): the access mode under an async terminal, with and without the opt-in.
	[Theory]
	[InlineData(false, false, true)]
	[InlineData(true, false, true)]
	[InlineData(false, true, true)]
	[InlineData(false, false, false)]
	[InlineData(true, false, false)]
	[InlineData(false, true, false)]
	public static async Task TheAccessMode_IsPinned_WithAndWithoutAsyncReads(bool optIn, bool singleResult, bool generated)
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		var command = new FakeDbCommand(connection, () => new CountingReader(2));
		IDbCommand asInterface = command;
		CancellationToken token = TestContext.Current.CancellationToken;

		MappedCommand<int> view = generated
			? asInterface.Map<int>()
			: asInterface.Map(static r => r.GetInt32(0));
		if (optIn) view = view.AsyncReads();
		if (singleResult) view = view.SingleResult();
		await view.ToListAsync(token);

		CommandBehavior used = Assert.Single(command.Behaviors);
		Assert.Equal(generated, used.HasFlag(CommandBehavior.SequentialAccess));
		Assert.Equal(singleResult, used.HasFlag(CommandBehavior.SingleResult));
	}

	// T-TOK1: a plain IDataReader (not a DbDataReader) under an async terminal checks the token per row.
	[Fact]
	public static async Task APlainReader_ChecksTheTokenPerRow_UnderAnAsyncTerminal()
	{
		using var cts = new CancellationTokenSource();
		int mapped = 0;
		var command = new PlainReaderCommand(() => FakeReader.Create(["id"], [.. Enumerable.Range(1, 20).Select(i => (object?[])[i])]));
		MappedQuery<int> query = command.Map(r =>
		{
			if (++mapped == 10) cts.Cancel();
			return mapped;
		});

		await Assert.ThrowsAnyAsync<OperationCanceledException>(
			async () => await query.ToListAsync(cts.Token));

		Assert.Equal(10, mapped);
	}
}
