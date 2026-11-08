using System.Diagnostics;
using System.Runtime.ExceptionServices;

namespace Open.Database.Extensions.Channel;

/// <summary>Constructs a <see cref="Transformer{T}"/>.</summary>
/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
internal class Transformer<T>(RowMap<T>? map = null)
	: Core.Transformer<T>(map)
{
	/// <inheritdoc cref="Create(RowMap{T}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(1)]
	public static new Transformer<T> Create()
		=> new();

	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	public static new Transformer<T> Create(RowMap<T>? map)
		=> new(map);

	/// <summary>
	/// The columns of the reader that map to properties: their ordinals and names, in the order of the ordinals.
	/// Columns of the reader that map to nothing are left out, so records read with these ordinals line up with these names.
	/// </summary>
	private (int[] Ordinals, ImmutableArray<string> Names) ResolveColumns(IDataReader reader)
	{
		Debug.Assert(reader is not null);
		MatchingColumns(reader, out int[] ordinals, out ImmutableArray<string> names);
		return (ordinals, names);
	}

	/// <summary>
	/// Creates the pipeline that buffers the records of a reader (stage 1) and maps them into the <paramref name="target"/> (stage 2).
	/// Stage 2 starts immediately, so the caller must run stage 1 through <see cref="Pipeline.RunAsync(Func{ValueTask{long}})"/>, which
	/// makes sure the buffer gets completed.
	/// </summary>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">If true, the target is completed after the last record has been delivered, or with the error that ended the pipeline,
	/// but never before stage 1 has ended, i.e. the reader has been released.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	public Pipeline CreatePipeline(ChannelWriter<T> target, bool complete, CancellationToken cancellationToken)
		=> new(this, target, complete, cancellationToken);

	/// <summary>
	/// Reads the results from the reader into a buffer as fast as possible and maps them to the target on a second task.
	/// The reader belongs to the caller and is not disposed.
	/// The returned task completes when all the records have been read (the target is completed later, after the last one has been delivered).
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">If true, completes the target when no more results are available.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The number of records read.</returns>
	public ValueTask<long> PipeResultsTo(
		IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		// Fail before anything is started (and without touching the target) if the reader can't be described.
		(int[] Ordinals, ImmutableArray<string> Names) columns = ResolveColumns(reader);
		Pipeline pipeline = CreatePipeline(target, complete, cancellationToken);
		return pipeline.RunAsync(() => pipeline.ReadFrom(reader, columns));
	}

	/// <inheritdoc cref="PipeResultsTo(IDataReader, ChannelWriter{T}, bool, CancellationToken)"/>
	public ValueTask<long> PipeResultsToAsync(
		DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		(int[] Ordinals, ImmutableArray<string> Names) columns = ResolveColumns(reader);
		Pipeline pipeline = CreatePipeline(target, complete, cancellationToken);
		return pipeline.RunAsync(() => pipeline.ReadFromAsync(reader, columns));
	}

	/// <summary>
	/// A bounded buffer (<see cref="Buffer"/>) that stage 1 fills from a reader, and the second stage that drains it
	/// into the target on its own task (so the reading thread isn't the one mapping records and waiting on a slow consumer).
	/// </summary>
	/// <remarks>
	/// Completion flows downstream: whoever runs stage 1 completes the buffer (with the error if reading failed),
	/// and once the second stage has delivered everything that was buffered, the target is completed.
	/// The target is only ever completed once BOTH stages have ended, so never while the reader may still be open,
	/// whichever stage ended the pipeline.
	/// </remarks>
	internal sealed class Pipeline
	{
		private readonly Transformer<T> _owner;
		private readonly Processor _processor;
		private readonly CancellationTokenSource _stop;
		private readonly ChannelWriter<T> _target;
		private readonly bool _complete;
		private readonly CancellationToken _cancellationToken;
		private volatile Exception? _deliveryError;
		private int _pending = 2;

		internal Pipeline(Transformer<T> owner, ChannelWriter<T> target, bool complete, CancellationToken cancellationToken)
		{
			Debug.Assert(owner is not null);
			Debug.Assert(target is not null);

			_owner = owner;
			_target = target;
			_complete = complete;
			_cancellationToken = cancellationToken;
			_processor = new Processor(owner); // The column names are only known once there is a reader.
			_stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			Token = _stop.Token;

			// Continuations must not run synchronously inside the writes of stage 1: that would put the mapping (and the
			// waiting on a slow consumer) inside the loop that reads the database, and keep the reader open for all of it.
			var buffer = System.Threading.Channels.Channel.CreateBounded<object[]>(new BoundedChannelOptions(MaxArrayBuffer)
			{
				SingleReader = true,
				SingleWriter = true,
				AllowSynchronousContinuations = false,
				FullMode = BoundedChannelFullMode.Wait,
			});
			Buffer = buffer.Writer;

			_ = DeliverAsync(buffer.Reader);
		}

		/// <summary>The writer of the internal buffer. Whoever fills it is responsible for completing it.</summary>
		public ChannelWriter<object[]> Buffer { get; }

		/// <summary>
		/// The token stage 1 must observe. It is cancelled with the caller's token, and also when the second stage stops,
		/// so that stage 1 can never be left waiting for room in a buffer that nothing drains any more.
		/// </summary>
		public CancellationToken Token { get; }

		/// <summary>Stage 1 for a reader of a command: writes the records of the reader to the <see cref="Buffer"/> without completing it.</summary>
		public ValueTask<long> Read(IDataReader reader)
			=> ReadFrom(reader, _owner.ResolveColumns(reader));

		/// <summary>Stage 1 for a reader of a command, using asynchronous reads.</summary>
		public ValueTask<long> ReadAsync(DbDataReader reader)
			=> ReadFromAsync(reader, _owner.ResolveColumns(reader));

		internal async ValueTask<long> ReadFrom(IDataReader reader, (int[] Ordinals, ImmutableArray<string> Names) columns)
		{
			Debug.Assert(reader is not null);
			Debug.Assert(columns.Ordinals is not null && !columns.Names.IsDefault);

			_processor.SetNames(columns.Names); // Before the first record can reach stage 2.

			// When every column is mapped, the whole record is taken in one call.
			IEnumerable<object[]> records = columns.Ordinals.Length == reader.FieldCount
				? reader.AsEnumerable(LocalPool)
				: reader.AsEnumerable(columns.Ordinals, LocalPool);

			long count = 0;
			foreach (object[] record in records)
			{
				// Each record is written (waiting for room if need be) BEFORE the next one is read: if reading fails, every record
				// that was read is already in the buffer. (WriteAll would start the write, read ahead, and abandon the write if that fails.)
				await Buffer.WriteAsync(record, Token).ConfigureAwait(false);
				count++;
			}

			return count;
		}

		internal async ValueTask<long> ReadFromAsync(DbDataReader reader, (int[] Ordinals, ImmutableArray<string> Names) columns)
		{
			Debug.Assert(reader is not null);
			Debug.Assert(columns.Ordinals is not null && !columns.Names.IsDefault);

			_processor.SetNames(columns.Names);

			int[] ordinals = columns.Ordinals;
			bool whole = ordinals.Length == reader.FieldCount; // Every column is mapped: the whole record in one call.
			long count = 0;

			// Like the synchronous read, this succeeds if, and only if, the reader reports that there is nothing left to read, and is
			// cancelled if the cancellation is seen while there still is. Once the token is cancelled, one more read that can't itself
			// be cancelled tells which of the two it is: if it finds a record, writing that record ends the call with the cancellation.
			while (await reader.ReadAsync(Token.IsCancellationRequested ? CancellationToken.None : Token).ConfigureAwait(false))
			{
				object[] record = LocalPool.Rent(ordinals.Length);
				if (whole)
				{
					reader.GetValues(record);
				}
				else
				{
					for (int i = 0; i < ordinals.Length; i++)
						record[i] = reader.GetValue(ordinals[i]);
				}

				await Buffer.WriteAsync(record, Token).ConfigureAwait(false);
				count++;
			}

			return count;
		}

		/// <summary>
		/// Runs stage 1 and guarantees the <see cref="Buffer"/> ends up completed, so the second stage can never be left waiting.
		/// </summary>
		/// <remarks>
		/// If the second stage had failed by the time stage 1 ended, whatever stage 1 ended with (it may have been stopped by the
		/// second stage, and a provider can report that in any way), the second stage's failure is thrown instead.
		/// </remarks>
		public async ValueTask<long> RunAsync(Func<ValueTask<long>> stage1)
		{
			Debug.Assert(stage1 is not null);

			Exception? error = null;
			try
			{
				long count = await stage1().ConfigureAwait(false);
				// Reading can also end quietly, not by throwing, when the second stage has stopped it. (Not when it is the caller who cancelled:
				// if stage 1 got to the end, that cancellation doesn't undo the read, even though it stops the second stage.)
				if (!_cancellationToken.IsCancellationRequested)
					ThrowIfDeliveryFailed();

				return count;
			}
			catch (Exception ex)
			{
				error = ex;
				ThrowIfDeliveryFailed();
				throw;
			}
			finally
			{
				try
				{
					Buffer.TryComplete(error); // Already completed (by the core) when this is the command-level path.
				}
				finally
				{
					StageEnded();
				}
			}
		}

		private void ThrowIfDeliveryFailed()
		{
			if (_deliveryError is { } failure)
				ExceptionDispatchInfo.Capture(failure).Throw();
		}

		// Stage 2. Maps the buffered records into the target until the buffer has been completed and drained.
		// If the buffer was completed with an error, that error surfaces here only after everything buffered has been delivered.
		private async Task DeliverAsync(ChannelReader<object[]> buffer)
		{
			Debug.Assert(buffer is not null);

			Exception? error = null;
			try
			{
				while (await buffer.WaitToReadAsync(_cancellationToken).ConfigureAwait(false))
				{
					while (buffer.TryRead(out object[]? record))
					{
						T item;
						try
						{
							item = _processor.Transform(record);
						}
						finally
						{
							LocalPool.Return(record);
						}

						await _target.WriteAsync(item, _cancellationToken).ConfigureAwait(false);
					}
				}
			}
			catch (Exception ex)
			{
				error = ex;
			}

			_deliveryError = error; // Visible to stage 1 by the time the cancellation below reaches it.
			try
			{
				// Stage 1 can never be left waiting for room in a buffer that nothing drains any more.
				// (Records left in the buffer are simply collected with it: their arrays aren't returned to the pool, which only
				// costs the reuse, on a path that is already a failure, and draining here would race the last writes of stage 1.)
				if (error is not null)
					_stop.Cancel();
			}
			finally
			{
				StageEnded();
			}
		}

		// The target is completed by whichever stage ends last: not before stage 1 has ended (the reader has been released), and
		// not before stage 2 has ended (everything that could be delivered has been, or delivery has failed).
		private void StageEnded()
		{
			if (Interlocked.Decrement(ref _pending) != 0) return;

			try
			{
				if (_complete)
					_target.TryComplete(_deliveryError);
			}
			finally
			{
				_stop.Dispose();
			}
		}
	}
}
