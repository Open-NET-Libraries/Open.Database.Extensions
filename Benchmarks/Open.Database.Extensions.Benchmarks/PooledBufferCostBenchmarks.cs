using System.Data;
using BenchmarkDotNet.Attributes;
using Open.Database.Extensions.Core;

namespace Open.Database.Extensions.Benchmarks;

// What async-buffers.md "What it costs" reports, over the synthetic provider
// (width 1 and 10,000 rows in the page; refill 100 rows / 50 us, or none).
// Run it in process (--inprocess) so the ReaderOpen column is filled; Mean is
// the total. AsyncReads() is on unless ODX_SYN_ASYNCREADS=0.
//
//  ChunkStream_*  ToPooledBufferStream: the consumer works on each chunk
//                 after it has been handed over (the reader is closed by
//                 the producer, whatever the consumer does).
//  NoBuffer_*     the plain read (ReadAsync per row when AsyncReads is on):
//                 the consumer works on each row inside the read, so it
//                 holds the reader open. Hand-mapped rows, see NoBuffer.
//  Buffer_Dump    ToPooledBuffer(...).Items, then dispose; ToArray_Dump the
//                 same rows with ToArrayAsync.
//
// The consumers (a busy wait, as the provider's refill is):
//  Spin1   one Thread.SpinWait iteration per row (about 39 ns: the
//          calibration is printed in the // PBCOST line)
//  Us1     the iterations of one microsecond, per row
//  Wait20  20 us per 100 rows
// Every consumer also adds each row's Id to a checksum, checked at the end.
[MemoryDiagnoser]
[Config(typeof(SyntheticConfig))]
public class PooledBufferCostBenchmarks
{
	const string Sql = SyntheticVariants.Sql;
	const int WaitEvery = 100;
	const double WaitMicroseconds = 20;

	enum Work { Spin1, Us1, Wait20 }

	[ParamsSource(nameof(WidthsValues), Priority = 0)]
	public int Width { get; set; }

	[ParamsSource(nameof(RowsValues), Priority = 1)]
	public int Rows { get; set; }

	[ParamsSource(nameof(BufferRowsValues), Priority = 2)]
	public int BufferRows { get; set; }

	[ParamsSource(nameof(RefillUsValues), Priority = 3)]
	public int RefillUs { get; set; }

	[ParamsSource(nameof(AsyncReadsValues), Priority = 4)]
	public int AsyncReads { get; set; }

	public static IEnumerable<int> WidthsValues => SyntheticParams.Widths;
	public static IEnumerable<int> RowsValues => SyntheticParams.Rows;
	public static IEnumerable<int> BufferRowsValues => SyntheticParams.BufferRows;
	public static IEnumerable<int> RefillUsValues => SyntheticParams.RefillUs;
	public static IEnumerable<int> AsyncReadsValues
		=> SyntheticParams.Parse(
			Environment.GetEnvironmentVariable("ODX_SYN_ASYNCREADS"),
			[1]);

	SyntheticConnection _connection = null!;
	Func<IDataRecord, SynRow> _hand = null!;
	long _expectedSum;
	int _spinPerUs;
	string _label = "";

	[GlobalSetup]
	public void Setup()
	{
		// The pool does not inject threads mid-run: the producer, the
		// consumer and the refill continuations need a few from the start.
		ThreadPool.GetMinThreads(out int worker, out int io);
		ThreadPool.SetMinThreads(Math.Max(worker, 4), io);

		_connection = new SyntheticConnection(
			SyntheticTable.Create(Width, Rows),
			new SyntheticOptions
			{
				BufferRows = BufferRows,
				RefillCostUs = RefillUs,
			});

		_hand = SyntheticVariants.Hand(Width);
		_expectedSum = 0;
		foreach (int id in (int[])_connection.Table.Columns[0])
			_expectedSum += id;

		double perUs = SpinCalibration.Measure().Median;
		_spinPerUs = Math.Max(1, (int)Math.Round(perUs));
		Console.WriteLine(FormattableString.Invariant(
			$"// PBCOST Width={Width} Rows={Rows} BufferRows={BufferRows} RefillUs={RefillUs} AsyncReads={AsyncReads} spin iterations per us={perUs:F2} (Spin1 is about {1000 / perUs:F1} ns per row) Us1 uses {_spinPerUs} per row"));
		SyntheticIterationListener.Attach(_connection.Options.Recorder);
	}

	// The label is the method that ran: the recorder's numbers are filed
	// under that case, which is what the ReaderOpen column looks up.
	[GlobalCleanup]
	public void Cleanup()
	{
		if (_label.Length == 0) return;
		SyntheticBenchmarks.FileResults(
			SyntheticBenchmarks.Key(
				_label,
				[
					(nameof(Width), Width),
					(nameof(Rows), Rows),
					(nameof(BufferRows), BufferRows),
					(nameof(RefillUs), RefillUs),
					(nameof(AsyncReads), AsyncReads),
				]),
			_connection.Options.Recorder);
	}

	// ----- the chunk stream: the reader is closed by the producer -----

	[Benchmark]
	public ValueTask<long> ChunkStream_Spin1()
		=> ChunkStream(nameof(ChunkStream_Spin1), Work.Spin1);

	[Benchmark]
	public ValueTask<long> ChunkStream_Us1()
		=> ChunkStream(nameof(ChunkStream_Us1), Work.Us1);

	[Benchmark]
	public ValueTask<long> ChunkStream_Wait20()
		=> ChunkStream(nameof(ChunkStream_Wait20), Work.Wait20);

	// ----- no buffer: the consumer works inside the read -----

	[Benchmark]
	public ValueTask<long> NoBuffer_Spin1()
		=> NoBuffer(nameof(NoBuffer_Spin1), Work.Spin1);

	[Benchmark]
	public ValueTask<long> NoBuffer_Us1()
		=> NoBuffer(nameof(NoBuffer_Us1), Work.Us1);

	[Benchmark]
	public ValueTask<long> NoBuffer_Wait20()
		=> NoBuffer(nameof(NoBuffer_Wait20), Work.Wait20);

	// ----- the dump: the rows held, then given back -----

	[Benchmark]
	public async ValueTask<long> Buffer_Dump()
	{
		_label = nameof(Buffer_Dump);
		await using PooledBuffer<SynRow> buffer = Command()
			.Map<SynRow>()
			.ToPooledBuffer(CancellationToken.None);

		PooledArray<SynRow> items = await buffer
			.Items
			.ConfigureAwait(false);

		return VerifyCount(items.Count);
	}

	[Benchmark]
	public async ValueTask<long> ToArray_Dump()
	{
		_label = nameof(ToArray_Dump);
		SynRow[] rows = await Command()
			.Map<SynRow>()
			.ToArrayAsync(CancellationToken.None)
			.ConfigureAwait(false);

		return VerifyCount(rows.Length);
	}

	// ----- the shapes -----

	async ValueTask<long> ChunkStream(string label, Work work)
	{
		_label = label;
		long sum = 0;
		int pending = 0;
		await foreach (ReadOnlyMemory<SynRow> chunk in Command()
			.Map<SynRow>()
			.ToPooledBufferStream(CancellationToken.None)
			.ConfigureAwait(false))
		{
			sum += SumIds(chunk.Span);
			Pay(work, chunk.Length, ref pending);
		}

		return VerifySum(sum);
	}

	// The read itself, with the consumer inside it: the public Map terminals
	// have no inline read with ReadAsync, so this is the ADO loop with the
	// hand-written selector (as SyntheticVariants.V0Async is).
	async ValueTask<long> NoBuffer(string label, Work work)
	{
		_label = label;
		Func<IDataRecord, SynRow> hand = _hand;
		bool asyncReads = AsyncReads != 0;
		long sum = await _connection
			.Command(Sql)
			.ExecuteReaderAsync<long>(
				async reader =>
				{
					long total = 0;
					int pending = 0;
					while (asyncReads
						? await reader.ReadAsync().ConfigureAwait(false)
						: reader.Read())
					{
						total += hand(reader).Id;
						Pay(work, 1, ref pending);
					}

					// The drain the library makes before it closes the reader.
					while (reader.NextResult()) { }
					return total;
				},
				CommandBehavior.SequentialAccess)
			.ConfigureAwait(false);

		return VerifySum(sum);
	}

	ExpressiveDbCommand Command()
	{
		ExpressiveDbCommand command = _connection.Command(Sql);
		return AsyncReads != 0 ? command.AsyncReads() : command;
	}

	// ----- the consumers -----

	static long SumIds(ReadOnlySpan<SynRow> rows)
	{
		long sum = 0;
		for (int i = 0; i < rows.Length; i++)
			sum += rows[i].Id;

		return sum;
	}

	// A chunk consumer pays the work of all its rows at once, a row consumer
	// one row at a time: the total is the same. Wait20 carries the remainder
	// from chunk to chunk.
	void Pay(Work work, int rows, ref int pending)
	{
		switch (work)
		{
			case Work.Spin1:
				Spin.Iterations(rows);
				break;

			case Work.Us1:
				Spin.Iterations(_spinPerUs * rows);
				break;

			default:
				pending += rows;
				while (pending >= WaitEvery)
				{
					Spin.Microseconds(WaitMicroseconds);
					pending -= WaitEvery;
				}

				break;
		}
	}

	long VerifySum(long sum)
		=> sum == _expectedSum
			? sum
			: throw new InvalidOperationException(
				$"{_label}: checksum {sum}, expected {_expectedSum}.");

	long VerifyCount(int count)
		=> count == Rows
			? count
			: throw new InvalidOperationException(
				$"{_label}: {count} rows, expected {Rows}.");
}
