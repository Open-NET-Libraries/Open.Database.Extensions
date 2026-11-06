using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Dataflow;

/// <inheritdoc />
internal class Transformer<T>(RowMap<T>? map = null)
	: Core.Transformer<T>(map)
	where T : new()
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
	/// The internal two-stage pipeline behind the mapped overloads:
	/// rows are written (as raw arrays) into a bounded <see cref="TransformBlock{TInput, TOutput}"/> (the <em>source end</em>),
	/// which maps them and forwards them to the caller's target.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Completion is a signal that enters at the source end and flows downstream by itself:
	/// whoever writes into the pipeline calls <see cref="Complete"/> (or <see cref="Fault"/>) on the source end only, once it has
	/// released the reader, and when <c>complete</c> was requested, the target is completed (or faulted) after
	/// (1) the reader has been released and (2) every row has been delivered to the target.
	/// The target is never completed or faulted directly by the code that writes into the pipeline, and never before the reader is released.
	/// </para>
	/// <para>
	/// The block, its link to the target, and the completion flow exist from construction,
	/// so there is no window in which a failure has nothing to fault.
	/// The column names (and therefore the mapper) are bound late, once a reader exists (<see cref="Write"/>).
	/// </para>
	/// </remarks>
	internal sealed class Pipeline
	{
		readonly TransformBlock<object[], T> _block;
		readonly CancellationTokenSource _stop = new();
		readonly ITargetBlock<T> _target;
		readonly TaskCompletionSource<bool> _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
		Func<object?[], T>? _transform;
		int[] _ordinals = [];
		int _allColumnsInOrder = -1; // The reader's field count when every column is mapped, in order; otherwise -1.
		Exception? _failure;

		/// <summary>
		/// Creates the source end, links it to the <paramref name="target"/>, and (if <paramref name="complete"/>) arranges for
		/// the target to be completed or faulted once the writer has released the reader and the source end has drained.
		/// </summary>
		/// <param name="target">The target to deliver the mapped rows to.</param>
		/// <param name="complete">If true, the target is completed (or faulted) downstream when the source end finishes.</param>
		/// <param name="options">The options to apply to the transform block.</param>
		public Pipeline(ITargetBlock<T> target, bool complete, ExecutionDataflowBlockOptions? options = null)
		{
			Debug.Assert(target is not null, "The caller validates the target.");
			_target = target;
			_block = new TransformBlock<object[], T>(
				a =>
				{
					try
					{
						Debug.Assert(_transform is not null, "Bind runs before any row is written.");
						return _transform!(a);
					}
					finally
					{
						LocalPool.Return(a);
					}
				},
				options ?? new ExecutionDataflowBlockOptions
				{
					BoundedCapacity = MaxArrayBuffer,
					SingleProducerConstrained = true
				});

			_block.LinkTo(target);

			// An async writer must not wait forever on a pipeline that can no longer deliver: it would hold the reader
			// (and its connection) for good.  So the writer is stopped when the block faults or is cancelled (a row could not be
			// mapped), and, for as long as it is writing, when the target closes under it (see WriteAsync).
			// The writer then ends quietly, just as a declined Post() does.
			_ = _block.Completion.ContinueWith(
				t =>
				{
					if (!t.IsFaulted && !t.IsCanceled) return;

					_ = t.Exception; // Observed on purpose: with complete: false nobody else will look at it.
					_stop.Cancel();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Default);

			if (complete) _ = FlowCompletionAsync(target);
		}

		// The completion signal flowing downstream.  Never runs on a scheduler captured from the caller.
		async Task FlowCompletionAsync(ITargetBlock<T> target)
		{
			Debug.Assert(target is not null);

			// The target must not hear about the end of the pipeline before the reader has been released.
			await _released.Task.ConfigureAwait(false);

			// ...nor before every row has been delivered to it.
			Task completion = _block.Completion;
			try
			{
				await completion.ConfigureAwait(false);
			}
			catch
			{
				// The outcome is read from the task below.
			}

			try
			{
				if (completion.IsFaulted) target.Fault(Unwrap(completion.Exception!)); // A row could not be mapped.
				else if (completion.IsCanceled) target.Fault(new TaskCanceledException(completion));
				else if (Volatile.Read(ref _failure) is { } failure) target.Fault(failure); // The writer failed.
				else target.Complete();
			}
			catch
			{
				// Deliberate: nothing is left to report to (the call that built this pipeline has long since returned or
				// is only waiting for the reader), and a target that throws from Complete()/Fault() has broken its own contract.
				// Swallowing it here keeps this fire-and-forget task from faulting unobserved.
			}

			// Both paths hand the target a single exception where there is one (the same shape for a mapping failure and a reader failure).
			static Exception Unwrap(AggregateException exception)
				=> exception.InnerExceptions.Count == 1 ? exception.InnerExceptions[0] : exception;
		}

		/// <summary>
		/// Binds the mapper to the columns of the <paramref name="reader"/>.  Must be called before any row is written.
		/// </summary>
		/// <remarks>
		/// The mapper knows only the matched columns, so a row must hold exactly those, in that order
		/// (the reader may have other columns, anywhere among them).
		/// </remarks>
		void Bind(Transformer<T> transformer, IDataReader reader)
		{
			Debug.Assert(transformer is not null);
			Debug.Assert(reader is not null);

			(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(transformer.ColumnNames, true);
			int[] ordinals = new int[columns.Length];
			bool inOrder = true;
			for (int i = 0; i < ordinals.Length; i++)
			{
				ordinals[i] = columns[i].Ordinal;
				if (ordinals[i] != i) inOrder = false;
			}

			int fieldCount = reader.FieldCount;
			_ordinals = ordinals;
			_allColumnsInOrder = inOrder && ordinals.Length == fieldCount ? fieldCount : -1;
			_transform = new Processor(transformer, columns.Select(m => m.Name).ToImmutableArray()).Transform;
		}

		object[] ReadRow(IDataReader reader)
		{
			// Every column is mapped, in order: take the whole record in one call.
			if (_allColumnsInOrder >= 0) return reader.GetValues(_allColumnsInOrder, LocalPool);

			int[] ordinals = _ordinals;
			object[] row = LocalPool.Rent(ordinals.Length);
			for (int i = 0; i < ordinals.Length; i++)
				row[i] = reader.GetValue(ordinals[i]);

			return row;
		}

		/// <summary>
		/// Writes the rows of the <paramref name="reader"/> into the source end.  Does not complete anything.
		/// Stops (without error) if the source end declines a row.
		/// </summary>
		/// <returns>The number of rows written to the source end.</returns>
		public long Write(IDataReader reader, Transformer<T> transformer)
		{
			Debug.Assert(reader is not null, "The caller validates the reader.");
			Debug.Assert(transformer is not null);

			Bind(transformer, reader);

			long total = 0;
			while (reader.Read())
			{
				object[] row = ReadRow(reader);
				if (!_block.Post(row))
				{
					LocalPool.Return(row);
					break;
				}

				total++;
			}

			return total;
		}

		/// <inheritdoc cref="Write"/>
		public async ValueTask<long> WriteAsync(IDataReader reader, Transformer<T> transformer, CancellationToken cancellationToken)
		{
			Debug.Assert(reader is not null, "The caller validates the reader.");
			Debug.Assert(transformer is not null);

			Bind(transformer, reader);

			using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
			CancellationToken token = linked.Token;

			// Stop writing if the target closes under the pipeline, but only while writing: this race is abandoned when the write
			// ends (Task.WhenAny removes its registration from the target's Completion task), so a long-lived target is not left
			// holding anything from a call that has finished.
			Task closed = _target.Completion;
			var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			_ = Task.WhenAny(closed, finished.Task).ContinueWith(
				t =>
				{
					if (ReferenceEquals(t.Result, closed)) _stop.Cancel();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Default);

			long total = 0;
			try
			{
				while (
					reader is DbDataReader r
						? await r.ReadAsync(token).ConfigureAwait(false)
						: reader.Read())
				{
					object[] row = ReadRow(reader);
					if (!await _block.SendAsync(row, token).ConfigureAwait(false))
					{
						LocalPool.Return(row);
						break;
					}

					total++;
				}
			}
			catch (Exception) when (_stop.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
			{
				// The pipeline itself stopped the writer (the mapper failed, or the target closed): end quietly.  Whatever a provider
				// throws when it honours that cancellation (an OperationCanceledException or its own exception) is not a failure of the
				// caller's operation; the target has already heard about the cause.
			}
			finally
			{
				finished.TrySetResult(true); // Abandons the watch on the target.
			}

			return total;
		}

		/// <summary>
		/// Signals that all rows have been written and the reader has been released.  The rows still buffered continue to flow
		/// downstream and the target is completed (if requested) after the last one has been delivered.
		/// </summary>
		public void Complete()
		{
			_block.Complete();
			_released.TrySetResult(true);
		}

		/// <summary>
		/// Signals that writing failed and the reader has been released.  The rows already written are still delivered,
		/// and then the target is faulted with <paramref name="exception"/> (if completion was requested).
		/// </summary>
		public void Fault(Exception exception)
		{
			Debug.Assert(exception is not null, "A null failure would be mistaken for success.");
			Interlocked.CompareExchange(ref _failure, exception, null);
			_block.Complete();
			_released.TrySetResult(true);
		}
	}

	/// <summary>
	/// Transforms the results from the reader by first buffering the results and if/when the buffer size is reached, the results are transformed to a channel for reading.
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">Will call complete when no more results are avaiable.</param>
	/// <param name="options">The options to apply to the transform block.</param>
	/// <returns>The ChannelReader of the target.</returns>
	public long PipeResultsTo(
		IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		ExecutionDataflowBlockOptions? options = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		var pipeline = new Pipeline(target, complete, options);
		long total;
		try
		{
			total = pipeline.Write(reader, this);
		}
		catch (Exception ex)
		{
			pipeline.Fault(ex);
			throw;
		}

		pipeline.Complete();
		return total;
	}

	/// <summary>
	/// Transforms the results from the reader by first buffering the results and if/when the buffer size is reached, the results are transformed to a channel for reading.
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">Will call complete when no more results are avaiable.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <param name="options">The options to apply to the transform block.</param>
	/// <returns>The ChannelReader of the target.</returns>
	public async ValueTask<long> PipeResultsToAsync(
		IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		CancellationToken cancellationToken,
		ExecutionDataflowBlockOptions? options = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		var pipeline = new Pipeline(target, complete, options);
		long total;
		try
		{
			total = await pipeline.WriteAsync(reader, this, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			pipeline.Fault(ex);
			throw;
		}

		pipeline.Complete();
		return total;
	}
}
