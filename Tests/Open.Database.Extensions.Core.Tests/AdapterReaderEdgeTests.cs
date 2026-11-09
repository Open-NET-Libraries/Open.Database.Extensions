using System.Buffers;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Tests;

#nullable enable

// Edge cases of the Dataflow and Channel adapters that read a data reader into a target: what they do when the target declines or is full, when the reader or the transform throws, when the token is cancelled while a send waits,
// and which connection behavior and completion the Channel transform overload gives. The mapped paths and the Channel object[] overload have their own tests (ToTargetBlockMappedCompletionTests, ToChannelCoreEdgeTests).
// Dataflow: the synchronous overloads Post and so stop at the first declined record (documented: the declined record was read and is not delivered); the asynchronous ones SendAsync and wait for room.
[ExcludeFromCodeCoverage]
public static class AdapterReaderEdgeTests
{
	[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Test Only")]
	sealed class ProbeException : Exception;

	static readonly ImmutableArray<ColumnDef> Columns = [("id", typeof(int)), ("name", typeof(string))];

	static readonly string[] Names = ["id", "name"];

	static object?[][] Rows(int count) => [.. Enumerable.Range(1, count).Select(i => new object?[] { i, $"row {i}" })];

	// ---- helpers ----

	// A target that is not a Dataflow block, so that what it was given, completed with and faulted with can be read back (a faulted BufferBlock drops what it holds).
	sealed class RecordingTarget<T> : ITargetBlock<T>
	{
		readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

		public List<T> Received { get; } = [];

		public bool Completed { get; private set; }

		public Exception? Failure { get; private set; }

		public Task Completion => _completion.Task;

		public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAcceptDecline)
		{
			Received.Add(messageValue);
			return DataflowMessageStatus.Accepted;
		}

		public void Complete()
		{
			Completed = true;
			_completion.TrySetResult();
		}

		public void Fault(Exception exception)
		{
			Failure = exception;
			_completion.TrySetException(exception);
			_ = _completion.Task.Exception; // observed here: nobody waits on it
		}
	}

	// Hands out arrays longer than asked for, as ArrayPool<T>.Shared may, and counts the rentals.
	sealed class CountingPool : ArrayPool<object>
	{
		public int Rented { get; private set; }

		public override object[] Rent(int minimumLength)
		{
			Rented++;
			return new object[minimumLength + 2];
		}

		public override void Return(object[] array, bool clearArray = false) { }
	}

	// Throws from the chosen Read call (1 is the first row), as a provider does when the connection drops part-way through a result.
	sealed class FailingReader(ImmutableArray<ColumnDef> columns, object?[][] rows, int failOnRead, Exception failure) : TypedReader(columns, rows)
	{
		int _reads;

		public override bool Read() => ++_reads == failOnRead ? throw failure : base.Read();
	}

	sealed class ProbeReader(ImmutableArray<ColumnDef> columns, object?[][] rows) : TypedReader(columns, rows)
	{
		public int Disposals { get; private set; }

		protected override void Dispose(bool disposing)
		{
			if (disposing) Disposals++;
			base.Dispose(disposing);
		}
	}

	static object[] Values(IDataRecord record) => record.GetValues();

	// The three unmapped shapes of the adapters: an array of the values, an array rented from a pool, and a transform.
	public enum Shape { Plain, Pooled, Transform }

	public static TheoryData<Shape> Shapes { get; } = new(Enum.GetValues<Shape>());

	static CancellationToken Token => TestContext.Current.CancellationToken;

static long ToTarget(Shape shape, IDataReader reader, ITargetBlock<object[]> target, bool complete, ArrayPool<object>? pool = null)
		=> shape switch
		{
			Shape.Plain => reader.ToTargetBlock(target, complete),
			Shape.Pooled => reader.ToTargetBlock(target, complete, pool ?? ArrayPool<object>.Shared),
			_ => reader.ToTargetBlock(target, complete, Values),
		};

	static async Task<long> ToTargetAsync(Shape shape, IDataReader reader, ITargetBlock<object[]> target, bool complete, CancellationToken cancellationToken)
		=> shape switch
		{
			Shape.Plain => await reader.ToTargetBlockAsync(target, complete, cancellationToken),
			Shape.Pooled => await reader.ToTargetBlockAsync(target, complete, ArrayPool<object>.Shared, cancellationToken),
			_ => await reader.ToTargetBlockAsync(target, complete, Values, cancellationToken),
		};

	// A block that waits for ever is a hung test, not a failed one: bound the wait.
	static Task Within(Task task) => task.WaitAsync(TimeSpan.FromSeconds(30));

	// ---- DataflowExtensions.ToTargetBlock (IDataReader) and AsSourceBlock: Post, so a decline stops the reading ----

	[Theory]
	[MemberData(nameof(Shapes))]
	public static void ToTargetBlock_ACompletedTarget_DeclinesTheFirstRecord_SoNothingIsDeliveredAndNothingThrows(Shape shape)
	{
		var target = new BufferBlock<object[]>();
		target.Complete();
		TypedReader reader = TypedReader.Create(Columns, Rows(3));

		Assert.Equal(0, ToTarget(shape, reader, target, complete: true));

		Assert.Equal(1, reader.RowsRead); // read, declined, dropped
		Assert.False(target.TryReceive(out _));
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static async Task ToTargetBlock_ABoundedTargetThatFills_StopsTheReadingAndDropsTheDeclinedRecord(Shape shape)
	{
		var target = new BufferBlock<object[]>(new DataflowBlockOptions { BoundedCapacity = 2 });
		TypedReader reader = TypedReader.Create(Columns, Rows(5));

		Assert.Equal(2, ToTarget(shape, reader, target, complete: true));

		Assert.Equal(3, reader.RowsRead); // the third row was read and declined
		Assert.True(target.TryReceiveAll(out IList<object[]>? items));
		Assert.Equal([1, 2], items.Select(item => item[0]));
		await Within(target.Completion); // complete: true also when the reading stopped early
	}

	[Theory]
	[MemberData(nameof(Shapes))]
	public static void ToTargetBlock_AReaderThatThrows_FaultsTheTargetOnlyWhenCompleteIsTrue_AndRethrows(Shape shape)
	{
		var failure = new ProbeException();

		var faulted = new RecordingTarget<object[]>();
		Assert.Same(failure, Assert.Throws<ProbeException>(() => ToTarget(shape, new FailingReader(Columns, Rows(5), 3, failure), faulted, complete: true)));
		Assert.Equal(2, faulted.Received.Count); // what was read before the failure was delivered
		Assert.Same(failure, faulted.Failure);
		Assert.False(faulted.Completed);

		var untouched = new RecordingTarget<object[]>();
		Assert.Same(failure, Assert.Throws<ProbeException>(() => ToTarget(shape, new FailingReader(Columns, Rows(5), 3, failure), untouched, complete: false)));
		Assert.Equal(2, untouched.Received.Count);
		Assert.Null(untouched.Failure);
		Assert.False(untouched.Completed);
	}

	[Fact]
	public static void ToTargetBlock_ATransformThatThrows_FaultsTheTargetLikeAReaderThatThrows()
	{
		var failure = new ProbeException();
		int Transform(IDataRecord record) => record.GetInt32(0) == 2 ? throw failure : record.GetInt32(0);

		var faulted = new RecordingTarget<int>();
		Assert.Same(failure, Assert.Throws<ProbeException>(() => TypedReader.Create(Columns, Rows(3)).ToTargetBlock(faulted, true, Transform)));
		Assert.Equal([1], faulted.Received);
		Assert.Same(failure, faulted.Failure);

		var untouched = new RecordingTarget<int>();
		Assert.Same(failure, Assert.Throws<ProbeException>(() => TypedReader.Create(Columns, Rows(3)).ToTargetBlock(untouched, false, Transform)));
		Assert.Null(untouched.Failure);
		Assert.False(untouched.Completed);
	}

	[Fact]
	public static void ToTargetBlock_ThePooledOverload_RentsOneArrayPerRecord_WhoseFirstSlotsHoldTheValues()
	{
		var pool = new CountingPool();
		var target = new RecordingTarget<object[]>();

		Assert.Equal(3, TypedReader.Create(Columns, Rows(3)).ToTargetBlock(target, true, pool));

		Assert.Equal(3, pool.Rented);
		Assert.Equal(3, target.Received.Distinct().Count());
		for (int i = 0; i < 3; i++)
		{
			object[] array = target.Received[i];
			Assert.True(array.Length > Columns.Length); // the pool may give more than was asked for: only the first FieldCount slots are the record
			Assert.Equal([i + 1, $"row {i + 1}"], array[..Columns.Length]);
		}

		Assert.True(target.Completed);
	}

	[Fact]
	public static async Task AsSourceBlock_ABoundedBlock_StopsEarly_AndIsStillCompleted()
	{
		TypedReader reader = TypedReader.Create(Columns, Rows(5));

		IReceivableSourceBlock<object[]> block = reader.AsSourceBlock(new DataflowBlockOptions { BoundedCapacity = 2 });

		Assert.Equal(3, reader.RowsRead);
		Assert.True(block.TryReceiveAll(out IList<object[]>? items));
		Assert.Equal([1, 2], items.Select(item => item[0]));
		await Within(block.Completion);
	}

	// ---- DataflowExtensions.ToTargetBlockAsync (IDataReader): SendAsync, with a reader of either kind ----

	public enum ReaderKind { Plain, Db }

	public static TheoryData<ReaderKind> ReaderKinds { get; } = new(Enum.GetValues<ReaderKind>());

	public static TheoryData<ReaderKind, Shape> ReaderKindsAndShapes { get; } = CrossProduct();

	static TheoryData<ReaderKind, Shape> CrossProduct()
	{
		var data = new TheoryData<ReaderKind, Shape>();
		foreach (ReaderKind kind in Enum.GetValues<ReaderKind>())
		{
			foreach (Shape shape in Enum.GetValues<Shape>())
			{
				data.Add(kind, shape);
			}
		}

		return data;
	}

	// An NSubstitute reader that is, or is not, a DbDataReader: the adapters read the first with ReadAsync and the second with Read.
	static IDataReader Reader(ReaderKind kind, int rows)
		=> kind == ReaderKind.Plain ? FakeReader.Create(Names, Rows(rows)) : FakeReader.CreateDb(Names, Rows(rows));

	static int Calls(IDataReader reader, string method) => reader.ReceivedCalls().Count(call => call.GetMethodInfo().Name == method);

	[Theory]
	[MemberData(nameof(ReaderKindsAndShapes))]
	public static async Task ToTargetBlockAsync_DeliversEveryRow_CompletesTheTarget_AndReadsAsynchronouslyOnlyFromADbReader(ReaderKind kind, Shape shape)
	{
		IDataReader reader = Reader(kind, 3);
		var target = new RecordingTarget<object[]>();

		Assert.Equal(3, await ToTargetAsync(shape, reader, target, complete: true, Token));

		Assert.Equal([1, 2, 3], target.Received.Select(row => row[0]));
		Assert.True(target.Completed);
		Assert.Null(target.Failure);
		Assert.Equal(kind == ReaderKind.Db ? 0 : 4, Calls(reader, "Read"));      // three rows and the end
		Assert.Equal(kind == ReaderKind.Db ? 4 : 0, Calls(reader, "ReadAsync"));
	}

	[Theory]
	[MemberData(nameof(ReaderKinds))]
	public static async Task ToTargetBlockAsync_CancelledWhileWaitingForRoom_FaultsTheTarget_AndRethrows(ReaderKind kind)
	{
		var target = new BufferBlock<object[]>(new DataflowBlockOptions { BoundedCapacity = 1 });
		using var cancellation = new CancellationTokenSource();

		// Nothing consumes: the first record fills the block and the second waits for room.
		ValueTask<long> call = Reader(kind, 3).ToTargetBlockAsync(target, true, cancellation.Token);
		Assert.False(call.IsCompleted);
		cancellation.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await call);
		Exception fault = await Assert.ThrowsAnyAsync<Exception>(() => Within(target.Completion));
		Assert.IsAssignableFrom<OperationCanceledException>(fault);
	}

	[Theory]
	[MemberData(nameof(ReaderKinds))]
	public static async Task ToTargetBlockAsync_CancelledWhileWaitingForRoom_LeavesTheTargetAloneWhenCompleteIsFalse(ReaderKind kind)
	{
		var target = new BufferBlock<object[]>(new DataflowBlockOptions { BoundedCapacity = 1 });
		using var cancellation = new CancellationTokenSource();

		ValueTask<long> call = Reader(kind, 3).ToTargetBlockAsync(target, false, cancellation.Token);
		Assert.False(call.IsCompleted);
		cancellation.Cancel();

		await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await call);
		Assert.False(target.Completion.IsCompleted);
	}

	[Theory]
	[MemberData(nameof(ReaderKinds))]
	public static async Task ToTargetBlockAsync_ATargetCompletedMidStream_DeclinesTheRecord_SoTheCountExcludesIt(ReaderKind kind)
	{
		var target = new BufferBlock<int>();

		long count = await Reader(kind, 3).ToTargetBlockAsync(target, true, record =>
		{
			int id = (int)record.GetValue(0);
			if (id == 2) target.Complete(); // the target is completed while the second record is on its way
			return id;
		}, Token);

		Assert.Equal(1, count);
		Assert.True(target.TryReceiveAll(out IList<int>? items));
		Assert.Equal([1], items);
		await Within(target.Completion);
	}

	[Theory]
	[MemberData(nameof(ReaderKinds))]
	public static async Task ToTargetBlockAsync_ATransformThatThrows_FaultsTheTargetOnlyWhenCompleteIsTrue_AndRethrows(ReaderKind kind)
	{
		var failure = new ProbeException();
		int Transform(IDataRecord record) => (int)record.GetValue(0) == 2 ? throw failure : (int)record.GetValue(0);

		var faulted = new RecordingTarget<int>();
		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(async () => await Reader(kind, 3).ToTargetBlockAsync(faulted, true, Transform, Token)));
		Assert.Equal([1], faulted.Received);
		Assert.Same(failure, faulted.Failure);
		Assert.False(faulted.Completed);

		var untouched = new RecordingTarget<int>();
		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(async () => await Reader(kind, 3).ToTargetBlockAsync(untouched, false, Transform, Token)));
		Assert.Null(untouched.Failure);
		Assert.False(untouched.Completed);
	}

	// ---- ChannelDbExtensions.ToChannel<T> (IDbCommand, ChannelWriter<T>, complete, transform): the transform overload ----

	sealed record Rig(FakeDbConnection Connection, FakeDbCommand Command, ProbeReader Reader);

	static Rig Build(ConnectionState state, int rows = 3)
	{
		var connection = new FakeDbConnection();
		connection.SetState(state);
		var reader = new ProbeReader(Columns, Rows(rows));
		return new Rig(connection, new FakeDbCommand(connection, () => reader), reader);
	}

	static Channel<int> NewChannel() => System.Threading.Channels.Channel.CreateUnbounded<int>();

	static int Tenfold(IDataRecord record) => record.GetInt32(0) * 10;

	static List<int> Drain(ChannelReader<int> reader)
	{
		var items = new List<int>();
		while (reader.TryRead(out int item)) items.Add(item);
		return items;
	}

	[Fact]
	public static async Task ToChannelTransform_AClosedConnection_IsOpenedForTheReader_WithCloseConnection_AndTheChannelIsCompleted()
	{
		Rig rig = Build(ConnectionState.Closed);
		Channel<int> channel = NewChannel();

		Assert.Equal(3, await ((IDbCommand)rig.Command).ToChannel(channel.Writer, true, Tenfold, Token));

		Assert.Equal([CommandBehavior.CloseConnection], rig.Command.Behaviors); // the reader's disposal is what closes it
		Assert.Equal(1, rig.Connection.OpenCalls);
		Assert.Equal(1, rig.Reader.Disposals);
		Assert.Equal([10, 20, 30], Drain(channel.Reader));
		await Within(channel.Reader.Completion);
	}

	[Fact]
	public static async Task ToChannelTransform_AnOpenConnection_IsNotAskedToClose_AndStaysOpen()
	{
		Rig rig = Build(ConnectionState.Open);
		Channel<int> channel = NewChannel();

		Assert.Equal(3, await ((IDbCommand)rig.Command).ToChannel(channel.Writer, true, Tenfold, Token));

		Assert.Equal([CommandBehavior.Default], rig.Command.Behaviors);
		Assert.Equal(0, rig.Connection.OpenCalls);
		Assert.Equal(ConnectionState.Open, rig.Connection.State);
		Assert.Equal([10, 20, 30], Drain(channel.Reader));
	}

	[Fact]
	public static async Task ToChannelTransform_ATransformThatThrows_CompletesTheChannelWithThatException_AndDisposesTheReader()
	{
		var failure = new ProbeException();
		Rig rig = Build(ConnectionState.Open);
		Channel<int> channel = NewChannel();

		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(async () => await ((IDbCommand)rig.Command).ToChannel(
			channel.Writer, true, record => record.GetInt32(0) == 2 ? throw failure : record.GetInt32(0), Token)));

		Assert.Equal(1, rig.Reader.Disposals);
		Assert.Equal([1], Drain(channel.Reader)); // the record before the failure was written
		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(() => Within(channel.Reader.Completion)));
	}

	[Fact]
	public static async Task ToChannelTransform_ATransformThatThrows_LeavesTheWriterOpenWhenCompleteIsFalse()
	{
		var failure = new ProbeException();
		Rig rig = Build(ConnectionState.Open);
		Channel<int> channel = NewChannel();

		Assert.Same(failure, await Assert.ThrowsAsync<ProbeException>(async () => await ((IDbCommand)rig.Command).ToChannel(
			channel.Writer, false, record => record.GetInt32(0) == 2 ? throw failure : record.GetInt32(0), Token)));

		Assert.Equal(1, rig.Reader.Disposals);
		Assert.True(channel.Writer.TryWrite(99)); // still open
		Assert.False(channel.Reader.Completion.IsCompleted);
	}

	[Fact]
	public static async Task ToChannelTransform_ACompletedTargetAndAClosedConnection_ThrowsChannelClosed_BeforeAnythingIsOpenedOrExecuted()
	{
		Rig rig = Build(ConnectionState.Closed);
		Channel<int> channel = NewChannel();
		channel.Writer.Complete();

		await Assert.ThrowsAsync<ChannelClosedException>(async () => await ((IDbCommand)rig.Command).ToChannel(channel.Writer, true, Tenfold, Token));

		Assert.Equal(0, rig.Connection.OpenCalls);
		Assert.Empty(rig.Command.Behaviors);
		Assert.Equal(ConnectionState.Closed, rig.Connection.State);
	}
}
