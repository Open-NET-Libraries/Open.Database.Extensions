using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Open.Database.Extensions.Dataflow;

/// <inheritdoc />
internal class Transformer<T>(IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null)
	: Core.Transformer<T>(fieldMappingOverrides)
	where T : new()
{
	/// <inheritdoc cref="Create(IEnumerable{KeyValuePair{string, string?}}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(1)]
	public static new Transformer<T> Create()
		=> new();

	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	public static new Transformer<T> Create(params IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides)
		=> new(fieldMappingOverrides);

	/// <inheritdoc cref="Create(IEnumerable{KeyValuePair{string, string?}}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(-1)]
	public static new Transformer<T> Create(params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		=> new(fieldMappingOverrides?.Select(kv => new KeyValuePair<string, string?>(kv.Field, kv.Column)));

	/// <summary>
	/// The internal two-stage pipeline behind the mapped overloads:
	/// rows are written (as raw arrays) into a bounded <see cref="TransformBlock{TInput, TOutput}"/> (the <em>source end</em>),
	/// which maps them and forwards them to the caller's target.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Completion is a signal that enters at the source end and flows downstream by itself:
	/// whoever writes into the pipeline calls <see cref="Complete"/> (or <see cref="Fault"/>) on the source end only,
	/// and when <c>complete</c> was requested, the target is completed (or faulted) by the continuation on the block's
	/// <see cref="Task"/> only after every row has been delivered to it.
	/// The target is never completed directly by the code that writes into the pipeline.
	/// </para>
	/// <para>
	/// The block, its link to the target, and the completion continuation exist from construction,
	/// so there is no window in which a failure has nothing to fault.
	/// The column names (and therefore the mapper) are bound late, once a reader exists (<see cref="Write"/>).
	/// </para>
	/// </remarks>
	internal sealed class Pipeline
	{
		readonly TransformBlock<object[], T> _block;
		Func<object?[], T>? _transform;
		Exception? _failure;

		/// <summary>
		/// Creates the source end, links it to the <paramref name="target"/>, and (if <paramref name="complete"/>) arranges for
		/// the target to be completed or faulted once the source end has finished and drained.
		/// </summary>
		/// <param name="target">The target to deliver the mapped rows to.</param>
		/// <param name="complete">If true, the target is completed (or faulted) downstream when the source end finishes.</param>
		/// <param name="options">The options to apply to the transform block.</param>
		public Pipeline(ITargetBlock<T> target, bool complete, ExecutionDataflowBlockOptions? options = null)
		{
			_block = new TransformBlock<object[], T>(
				a =>
				{
					try
					{
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
			if (complete)
			{
				_ = _block
					.Completion
					.ContinueWith(t =>
					{
						// The block itself faulted (for example, a row could not be mapped).
						if (t.IsFaulted) target.Fault(t.Exception);

						// The writer failed: every row it did write has been delivered, so now the target learns of it.
						else if (Volatile.Read(ref _failure) is { } failure) target.Fault(failure);
						else target.Complete();
					},
					CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously,
					TaskScheduler.Current);
			}
		}

		/// <summary>
		/// Binds the mapper to the columns of the <paramref name="reader"/>.  Must be called before any row is written.
		/// </summary>
		void Bind(Transformer<T> transformer, IDataReader reader)
		{
			(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(transformer.ColumnNames, true);
			var names = columns.Select(m => m.Name).ToImmutableArray();
			_transform = new Processor(transformer, names).Transform;
		}

		/// <summary>
		/// Writes the rows of the <paramref name="reader"/> into the source end.  Does not complete anything.
		/// Stops (without error) if the source end declines a row.
		/// </summary>
		/// <returns>The number of rows written to the source end.</returns>
		public long Write(IDataReader reader, Transformer<T> transformer)
		{
			Bind(transformer, reader);
			return reader.ToTargetBlock(_block, false, LocalPool);
		}

		/// <inheritdoc cref="Write"/>
		public ValueTask<long> WriteAsync(IDataReader reader, Transformer<T> transformer, CancellationToken cancellationToken)
		{
			Bind(transformer, reader);
			return reader.ToTargetBlockAsync(_block, false, LocalPool, cancellationToken);
		}

		/// <summary>
		/// Signals that all rows have been written.  The rows still buffered continue to flow downstream
		/// and the target is completed (if requested) after the last one has been delivered.
		/// </summary>
		public void Complete() => _block.Complete();

		/// <summary>
		/// Signals that writing failed.  The rows already written are still delivered,
		/// and then the target is faulted with <paramref name="exception"/> (if completion was requested).
		/// </summary>
		public void Fault(Exception exception)
		{
			Interlocked.CompareExchange(ref _failure, exception, null);
			_block.Complete();
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
