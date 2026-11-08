using Open.Database.Extensions.Core;
using System.Collections.Concurrent;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Characterization tests of the cost (T11) and the thread safety (T12) of the mapping core, on a model type of their own (RowMapTests has Item).
// They pin what is true today so a refactor of the per-model-type tables and of the compiled readers can be proved to keep it:
// reading through Map<T>() and through a declared map allocates the same, and the caches stay correct under contention.
[ExcludeFromCodeCoverage]
public static class MappingCostAndRaceCharacterizationTests
{
	public sealed class Gadget
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public decimal Total { get; set; }
		public string? Note { get; set; }
	}

	static readonly ImmutableArray<ColumnDef> MatchingColumns = [("Id", typeof(int)), ("Name", typeof(string)), ("Total", typeof(decimal)), ("Note", typeof(string))];
	static readonly ImmutableArray<ColumnDef> RenamedColumns = [("gadget_id", typeof(int)), ("gadget_name", typeof(string)), ("gadget_total", typeof(decimal)), ("gadget_note", typeof(string))];

	static RowMap<Gadget> NewDeclared() => new(
		new(nameof(Gadget.Id), "gadget_id"), new(nameof(Gadget.Name), "gadget_name"), new(nameof(Gadget.Total), "gadget_total"), new(nameof(Gadget.Note), "gadget_note"));

	static readonly RowMap<Gadget> Declared = NewDeclared();

	// The same properties, named by pairs to their own names: a declared map whose result shape equals the one Map<T>() reads.
	static readonly RowMap<Gadget> DeclaredOwnNames = new(
		new(nameof(Gadget.Id), "Id"), new(nameof(Gadget.Name), "Name"), new(nameof(Gadget.Total), "Total"), new(nameof(Gadget.Note), "Note"));

	static object?[] Row(ImmutableArray<ColumnDef> columns, int i)
		=> [.. columns.Select(c => c.Name switch
		{
			"Id" or "gadget_id" => (object?)i,
			"Name" or "gadget_name" => "name" + i,
			"Total" or "gadget_total" => i * 1.5m,
			"Note" or "gadget_note" => i % 2 == 0 ? null : "note" + i,
			_ => "extra",
		})];

	static FakeExecuteReader Command(ImmutableArray<ColumnDef> columns, int rows)
	{
		object?[][] data = [.. Enumerable.Range(1, rows).Select(i => Row(columns, i))];
		return new FakeExecuteReader(() => TypedReader.Create(columns, data));
	}

	static void AssertGadgets(IReadOnlyList<Gadget> items, int rows, bool hasNote = true)
	{
		Assert.Equal(rows, items.Count);
		for (int n = 0; n < rows; n++)
		{
			int i = n + 1;
			Assert.Equal(i, items[n].Id);
			Assert.Equal("name" + i, items[n].Name);
			Assert.Equal(i * 1.5m, items[n].Total);
			Assert.Equal(hasNote && i % 2 != 0 ? "note" + i : null, items[n].Note);
		}
	}

	// ---- T11: allocation ----

	// The smallest allocation per call over several rounds, after warm-up (the layout is compiled and cached by then).
	static long BytesPerRead(Func<List<Gadget>> read)
	{
		for (int i = 0; i < 5; i++) read();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; i++) read();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 20);
		}

		return best;
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ReadingThroughMapOfT_AndThroughADeclaredMap_AllocateTheSame(int rows)
	{
		FakeExecuteReader matching = Command(MatchingColumns, rows);
		FakeExecuteReader renamed = Command(RenamedColumns, rows);

		long byName = BytesPerRead(() => matching.Map<Gadget>().ToList());
		long declaredOwnNames = BytesPerRead(() => matching.Map(DeclaredOwnNames).ToList());
		long declaredRenamed = BytesPerRead(() => renamed.Map(Declared).ToList());
		long perCall = BytesPerRead(() => renamed.Map<Gadget>(
			new(nameof(Gadget.Id), "gadget_id"),
			new(nameof(Gadget.Name), "gadget_name"),
			new(nameof(Gadget.Total), "gadget_total"),
			new(nameof(Gadget.Note), "gadget_note")
		).ToList());

		Assert.InRange(declaredOwnNames - byName, -16, 16);
		Assert.InRange(declaredRenamed - byName, -16, 16);
		Assert.True(perCall - byName > 100, $"Per-call overrides were expected to cost visibly more ({perCall} vs {byName} B); otherwise this test cannot tell the difference."); // the control
	}

	// Per extra row the read allocates the model object and the list's own growth, and nothing more (a boxed cell, a closure or a slot per row would show here).
	[Fact]
	public static void EachExtraRow_CostsTheModelObjectAndTheListGrowth_AndNothingMore()
	{
		FakeExecuteReader one = Command(MatchingColumns, 1), many = Command(MatchingColumns, 101);
		long perRowByName = (BytesPerRead(() => many.Map<Gadget>().ToList()) - BytesPerRead(() => one.Map<Gadget>().ToList())) / 100;
		long perRowDeclared = (BytesPerRead(() => many.Map(DeclaredOwnNames).ToList()) - BytesPerRead(() => one.Map(DeclaredOwnNames).ToList())) / 100;

		// A Gadget is 56 bytes (header and method table 16, an int, a decimal and two references); the list adds a few bytes per row as it doubles.
		Assert.InRange(perRowByName, 56, 90); // measured: 77
		Assert.InRange(perRowDeclared - perRowByName, -2, 2);
	}

	// ---- T12: thread safety ----

	static void Race(int threads, Action<int> body)
	{
		using var barrier = new Barrier(threads);
		var errors = new ConcurrentQueue<Exception>();
		Task[] tasks = [.. Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
		{
			try
			{
				barrier.SignalAndWait();
				body(t);
			}
			catch (Exception ex)
			{
				errors.Enqueue(ex);
			}
		}, TaskCreationOptions.LongRunning))];

		Task.WaitAll(tasks, TestContext.Current.CancellationToken);
		Assert.Empty(errors);
	}

	static readonly ImmutableArray<ColumnDef> ReversedColumns = [.. RenamedColumns.Reverse()];
	static readonly ImmutableArray<ColumnDef> NoNoteColumns = [("extra", typeof(string)), ("gadget_total", typeof(decimal)), ("gadget_id", typeof(int)), ("gadget_name", typeof(string))];

	static RowMap<Gadget> BuildMap(ref int builds)
	{
		Interlocked.Increment(ref builds);
		return NewDeclared();
	}

	// A plain null check on the compiler-generated backing field.
	static class CoalescingMaps
	{
		public static int Builds;
		public static RowMap<Gadget> Map => field ??= BuildMap(ref Builds);
	}

	// Exactly one instance is published; the factory may still run more than once.
	static class EnsuredMaps
	{
		public static int Builds;
		public static RowMap<Gadget> Map => LazyInitializer.EnsureInitialized(ref field, static () => BuildMap(ref Builds));
	}

	// Every thread is released from a barrier immediately before its first call, so first use is contended.
	// Returns the instance each racer got from that first call.
	static RowMap<Gadget>[] RaceOnFirstUseThenSustained(Func<RowMap<Gadget>> map)
	{
		ImmutableArray<ColumnDef>[] shapes = [RenamedColumns, ReversedColumns, NoNoteColumns];
		var firsts = new RowMap<Gadget>[16];
		Race(16, t =>
		{
			firsts[t] = map(); // the contended first call
			ImmutableArray<ColumnDef> shape = shapes[t % shapes.Length];
			for (int n = 0; n < 150; n++) // sustained use after the race
				AssertGadgets(Command(shape, 5).Map(map()).ToList(), 5, hasNote: shape.Any(c => c.Name == "gadget_note"));
		});

		return firsts;
	}

	[Fact]
	public static void FieldCoalescingMap_RacedOnFirstUse_GivesEquivalentMapsAndIdenticalRows()
	{
		RowMap<Gadget>[] got = RaceOnFirstUseThenSustained(() => CoalescingMaps.Map);
		Assert.All(got, Assert.NotNull);
		Assert.InRange(CoalescingMaps.Builds, 1, 16);
	}

	[Fact]
	public static void EnsureInitializedMap_RacedOnFirstUse_PublishesOneInstanceToEveryRacer()
	{
		RowMap<Gadget>[] got = RaceOnFirstUseThenSustained(() => EnsuredMaps.Map);
		Assert.All(got, m => Assert.Same(got[0], m));
		Assert.InRange(EnsuredMaps.Builds, 1, 16);
	}

	// A model type nothing else touches, so the per-model-type tables (its properties and their index) and its default map are first built under the race.
	public sealed class Newcomer
	{
		public int Id { get; set; }
		public string? Name { get; set; }
		public decimal Total { get; set; }
	}

	[Fact]
	public static void AModelTypeMetForTheFirstTimeByManyThreads_BuildsOneDefaultMapAndMapsCorrectly()
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Name", typeof(string)), ("Total", typeof(decimal))];
		ImmutableArray<ColumnDef> reversed = [.. columns.Reverse()];
		var firsts = new RowMap<Newcomer>[16];
		Race(16, t =>
		{
			firsts[t] = RowMap<Newcomer>.Default;
			ImmutableArray<ColumnDef> shape = t % 2 == 0 ? columns : reversed;
			for (int n = 1; n <= 100; n++)
			{
				object?[] row = [.. shape.Select(c => c.Name switch { "Id" => (object?)n, "Name" => "name" + n, _ => n * 1.5m })];
				Newcomer item = new FakeExecuteReader(() => TypedReader.Create(shape, [row])).Map<Newcomer>().ToList().Single();
				Assert.Equal((n, "name" + n, n * 1.5m), (item.Id, item.Name, item.Total));
			}
		});

		Assert.All(firsts, m => Assert.Same(RowMap<Newcomer>.Default, m));
	}

	// The layout cache holds 128 layouts and clears itself when full. Readers keep using one layout while other
	// threads push 240 distinct layouts through the same map, so the cache fills and clears under them.
	[Fact]
	public static void ReadingWhileTheLayoutCacheOverflowsAndClears_GivesCorrectRows()
	{
		RowMap<Gadget> shared = NewDeclared();
		using var stop = new CancellationTokenSource();
		var errors = new ConcurrentQueue<Exception>();
		Thread[] readers = [.. Enumerable.Range(0, 4).Select(_ => new Thread(() =>
		{
			try
			{
				while (!stop.IsCancellationRequested) AssertGadgets(Command(RenamedColumns, 3).Map(shared).ToList(), 3);
			}
			catch (Exception ex)
			{
				errors.Enqueue(ex);
			}
		}))];

		foreach (Thread reader in readers) reader.Start();
		try
		{
			Race(4, t =>
			{
				for (int n = 0; n < 60; n++) // 4 x 60 = 240 layouts that no other thread shares
					AssertGadgets(Command([.. RenamedColumns, new ColumnDef($"extra_{t}_{n}", typeof(string))], 3).Map(shared).ToList(), 3);
			});
		}
		finally
		{
			stop.Cancel();
			foreach (Thread reader in readers) reader.Join();
		}

		Assert.Empty(errors);
	}

	// The buffered route has its own layout cache (by column names); it is raced the same way.
	[Fact]
	public static void BufferedRows_MappedByManyThreadsAtOnce_GiveCorrectRows()
	{
		RowMap<Gadget> shared = NewDeclared();
		string[][] shapes = [[.. RenamedColumns.Select(c => c.Name)], [.. ReversedColumns.Select(c => c.Name)], [.. NoNoteColumns.Select(c => c.Name)]];
		Race(16, t =>
		{
			string[] names = shapes[t % shapes.Length];
			ImmutableArray<ColumnDef> columns = [.. names.Select(n => new ColumnDef(n, typeof(object)))];
			for (int n = 0; n < 100; n++)
			{
				var result = new QueryResult<Queue<object?[]>>([.. Enumerable.Range(0, names.Length)], [.. names], new Queue<object?[]>([Row(columns, 1)]));
				AssertGadgets([.. result.DequeueAs(shared)], 1, hasNote: names.Contains("gadget_note"));
			}
		});
	}

	[Fact]
	public static void ManyMapsBuiltAtOnce_EachKeepsItsOwnMapping()
		=> Race(16, t =>
		{
			string a = "a" + t, b = "b" + t;
			var own = new RowMap<Gadget>([new(nameof(Gadget.Name), a), new(nameof(Gadget.Note), b)]);
			for (int n = 0; n < 50; n++)
			{
				Gadget item = new FakeExecuteReader([a, b], ["name" + t, "note" + t]).Map(own).ToList().Single();
				Assert.Equal(("name" + t, "note" + t), (item.Name, item.Note));
			}
		});

	// ---- constructor binding: T11 (cost) and T12 (thread safety) over a positional record, the equivalent of Gadget ----

	public sealed record GadgetRecord(int Id, string? Name, decimal Total, string? Note);

	static RowMap<GadgetRecord> NewDeclaredRecord() => new(
		new(nameof(GadgetRecord.Id), "gadget_id"), new(nameof(GadgetRecord.Name), "gadget_name"), new(nameof(GadgetRecord.Total), "gadget_total"), new(nameof(GadgetRecord.Note), "gadget_note"));

	static readonly RowMap<GadgetRecord> DeclaredRecord = NewDeclaredRecord();

	static readonly RowMap<GadgetRecord> DeclaredRecordOwnNames = new(
		new(nameof(GadgetRecord.Id), "Id"), new(nameof(GadgetRecord.Name), "Name"), new(nameof(GadgetRecord.Total), "Total"), new(nameof(GadgetRecord.Note), "Note"));

	static void AssertGadgetRecords(IReadOnlyList<GadgetRecord> items, int rows, bool hasNote = true)
	{
		Assert.Equal(rows, items.Count);
		for (int n = 0; n < rows; n++)
		{
			int i = n + 1;
			Assert.Equal(new GadgetRecord(i, "name" + i, i * 1.5m, hasNote && i % 2 != 0 ? "note" + i : null), items[n]);
		}
	}

	// The same measure as BytesPerRead, for any model type.
	static long BytesPerReadOf<T>(Func<List<T>> read)
	{
		for (int i = 0; i < 5; i++) read();
		long best = long.MaxValue;
		for (int round = 0; round < 5; round++)
		{
			long before = GC.GetAllocatedBytesForCurrentThread();
			for (int i = 0; i < 20; i++) read();
			best = Math.Min(best, (GC.GetAllocatedBytesForCurrentThread() - before) / 20);
		}

		return best;
	}

	[Fact]
	public static void ARecord_IsReadCorrectly_ThroughMapOfT_AndThroughADeclaredMap()
	{
		AssertGadgetRecords(Command(MatchingColumns, 5).Map<GadgetRecord>().ToList(), 5);
		AssertGadgetRecords(Command(MatchingColumns, 5).Map(DeclaredRecordOwnNames).ToList(), 5);
		AssertGadgetRecords(Command(RenamedColumns, 5).Map(DeclaredRecord).ToList(), 5);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void ReadingARecord_AllocatesWhatTheMatchingClassAllocates(int rows)
	{
		FakeExecuteReader matching = Command(MatchingColumns, rows);

		long asClass = BytesPerRead(() => matching.Map<Gadget>().ToList());
		long asRecord = BytesPerReadOf(() => matching.Map<GadgetRecord>().ToList());

		Assert.InRange(asRecord - asClass, -16, 16);
	}

	[Theory]
	[InlineData(0)]
	[InlineData(1)]
	[InlineData(100)]
	public static void PassingAMap_AllocatesNothingExtra_ForARecord(int rows)
	{
		FakeExecuteReader matching = Command(MatchingColumns, rows);
		FakeExecuteReader renamed = Command(RenamedColumns, rows);

		long byName = BytesPerReadOf(() => matching.Map<GadgetRecord>().ToList());
		long declaredOwnNames = BytesPerReadOf(() => matching.Map(DeclaredRecordOwnNames).ToList());
		long declaredRenamed = BytesPerReadOf(() => renamed.Map(DeclaredRecord).ToList());
		long perCall = BytesPerReadOf(() => renamed.Map<GadgetRecord>(
			new(nameof(GadgetRecord.Id), "gadget_id"),
			new(nameof(GadgetRecord.Name), "gadget_name"),
			new(nameof(GadgetRecord.Total), "gadget_total"),
			new(nameof(GadgetRecord.Note), "gadget_note")
		).ToList());

		Assert.InRange(declaredOwnNames - byName, -16, 16);
		Assert.InRange(declaredRenamed - byName, -16, 16);
		Assert.True(perCall - byName > 100, $"Per-call overrides were expected to cost visibly more ({perCall} vs {byName} B); otherwise this test cannot tell the difference."); // the control
	}

	// Per extra row the read allocates the record and the list's own growth, and nothing more: no boxed cell, argument array, closure or local per row.
	[Fact]
	public static void EachExtraRow_OfARecord_CostsTheSameAsTheClass_AndNothingMore()
	{
		FakeExecuteReader one = Command(MatchingColumns, 1), many = Command(MatchingColumns, 101);
		long perRowClass = (BytesPerRead(() => many.Map<Gadget>().ToList()) - BytesPerRead(() => one.Map<Gadget>().ToList())) / 100;
		long perRowRecord = (BytesPerReadOf(() => many.Map<GadgetRecord>().ToList()) - BytesPerReadOf(() => one.Map<GadgetRecord>().ToList())) / 100;
		long perRowDeclared = (BytesPerReadOf(() => many.Map(DeclaredRecordOwnNames).ToList()) - BytesPerReadOf(() => one.Map(DeclaredRecordOwnNames).ToList())) / 100;

		Assert.InRange(perRowRecord, 56, 90); // the same 56-byte object and list growth as a Gadget
		Assert.InRange(perRowRecord - perRowClass, -2, 2);
		Assert.InRange(perRowDeclared - perRowRecord, -2, 2);
	}

	// A record type nothing else touches, so its per-model-type tables (the slots and their index) and its default map are first built under the race.
	public sealed record RecordNewcomer(int Id, string? Name, decimal Total);

	[Fact]
	public static void ARecordTypeMetForTheFirstTimeByManyThreads_BuildsOneDefaultMapAndMapsCorrectly()
	{
		ImmutableArray<ColumnDef> columns = [("Id", typeof(int)), ("Name", typeof(string)), ("Total", typeof(decimal))];
		ImmutableArray<ColumnDef> reversed = [.. columns.Reverse()];
		var firsts = new RowMap<RecordNewcomer>[16];
		Race(16, t =>
		{
			firsts[t] = RowMap<RecordNewcomer>.Default;
			ImmutableArray<ColumnDef> shape = t % 2 == 0 ? columns : reversed;
			for (int n = 1; n <= 100; n++)
			{
				object?[] row = [.. shape.Select(c => c.Name switch { "Id" => (object?)n, "Name" => "name" + n, _ => n * 1.5m })];
				RecordNewcomer item = new FakeExecuteReader(() => TypedReader.Create(shape, [row])).Map<RecordNewcomer>().ToList().Single();
				Assert.Equal(new RecordNewcomer(n, "name" + n, n * 1.5m), item);
			}
		});

		Assert.All(firsts, m => Assert.Same(RowMap<RecordNewcomer>.Default, m));
	}

	static class RecordCoalescingMaps
	{
		public static int Builds;
		public static RowMap<GadgetRecord> Map => field ??= BuildRecordMap(ref Builds);
	}

	static RowMap<GadgetRecord> BuildRecordMap(ref int builds)
	{
		Interlocked.Increment(ref builds);
		return NewDeclaredRecord();
	}

	// One map, built on first use under the race, read in three result shapes by sixteen threads: the layouts compile and cache while others read them.
	[Fact]
	public static void ADeclaredMapOverARecord_RacedOnFirstUse_GivesIdenticalRows()
	{
		ImmutableArray<ColumnDef>[] shapes = [RenamedColumns, ReversedColumns, NoNoteColumns];
		Race(16, t =>
		{
			ImmutableArray<ColumnDef> shape = shapes[t % shapes.Length];
			for (int n = 0; n < 150; n++)
				AssertGadgetRecords(Command(shape, 5).Map(RecordCoalescingMaps.Map).ToList(), 5, hasNote: shape.Any(c => c.Name == "gadget_note"));
		});

		Assert.InRange(RecordCoalescingMaps.Builds, 1, 16);
	}

	// The layout cache holds 128 layouts and clears itself when full: readers keep using one layout while other threads push 240 distinct layouts through the same map.
	[Fact]
	public static void ReadingARecord_WhileTheLayoutCacheOverflowsAndClears_GivesCorrectRows()
	{
		RowMap<GadgetRecord> shared = NewDeclaredRecord();
		using var stop = new CancellationTokenSource();
		var errors = new ConcurrentQueue<Exception>();
		Thread[] readers = [.. Enumerable.Range(0, 4).Select(_ => new Thread(() =>
		{
			try
			{
				while (!stop.IsCancellationRequested) AssertGadgetRecords(Command(RenamedColumns, 3).Map(shared).ToList(), 3);
			}
			catch (Exception ex)
			{
				errors.Enqueue(ex);
			}
		}))];

		foreach (Thread reader in readers) reader.Start();
		try
		{
			Race(4, t =>
			{
				for (int n = 0; n < 60; n++) // 4 x 60 = 240 layouts that no other thread shares
					AssertGadgetRecords(Command([.. RenamedColumns, new ColumnDef($"extra_{t}_{n}", typeof(string))], 3).Map(shared).ToList(), 3);
			});
		}
		finally
		{
			stop.Cancel();
			foreach (Thread reader in readers) reader.Join();
		}

		Assert.Empty(errors);
	}

	// The buffered route has its own layout cache (by column names); it is raced the same way, over a record.
	[Fact]
	public static void BufferedRows_OfARecord_MappedByManyThreadsAtOnce_GiveCorrectRows()
	{
		RowMap<GadgetRecord> shared = NewDeclaredRecord();
		string[][] shapes = [[.. RenamedColumns.Select(c => c.Name)], [.. ReversedColumns.Select(c => c.Name)], [.. NoNoteColumns.Select(c => c.Name)]];
		Race(16, t =>
		{
			string[] names = shapes[t % shapes.Length];
			ImmutableArray<ColumnDef> columns = [.. names.Select(n => new ColumnDef(n, typeof(object)))];
			for (int n = 0; n < 100; n++)
			{
				var result = new QueryResult<Queue<object?[]>>([.. Enumerable.Range(0, names.Length)], [.. names], new Queue<object?[]>([Row(columns, 1)]));
				AssertGadgetRecords([.. result.DequeueAs(shared)], 1, hasNote: names.Contains("gadget_note"));
			}
		});
	}
}
