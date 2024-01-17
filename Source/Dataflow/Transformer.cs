namespace Open.Database.Extensions.Dataflow;

/// <inheritdoc />
internal class Transformer<T>(IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
	: Core.Transformer<T>(fieldMappingOverrides)
	where T : new()
{
	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	public static new Transformer<T> Create(IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
		=> new(fieldMappingOverrides);

	/// <summary>
	/// Transforms the results from the reader by first buffering the results and if/when the buffer size is reached, the results are transformed to a channel for reading.
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">Will call complete when no more results are avaiable.</param>
	/// <param name="options">The options to apply to the transform block.</param>
	/// <returns>The ChannelReader of the target.</returns>
	internal long PipeResultsTo(
		IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		ExecutionDataflowBlockOptions? options = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		var columns = reader.GetMatchingOrdinals(ColumnNames, true);
		var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
		var names = columns.Select(m => m.Name).ToImmutableArray();

		var processor = new Processor(this, names);
		var transform = processor.Transform;
		var columnCount = columns.Length;

		var transformBlock = new TransformBlock<object[], T>(
			a =>
			{
				try
				{
					return transform(a);
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

		transformBlock.LinkTo(target);
		if (complete)
		{
			transformBlock
				.Completion
				.ContinueWith(t =>
				{
					if (t.IsFaulted) target.Fault(t.Exception);
					else target.Complete();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Current);
		}

		return reader.ToTargetBlock(transformBlock, true, LocalPool);
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
	internal async ValueTask<long> PipeResultsToAsync(
		IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		CancellationToken cancellationToken,
		ExecutionDataflowBlockOptions? options = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		var columns = reader.GetMatchingOrdinals(ColumnNames, true);
		var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
		var names = columns.Select(m => m.Name).ToImmutableArray();

		var processor = new Processor(this, names);
		var transform = processor.Transform;
		var columnCount = columns.Length;

		var transformBlock = new TransformBlock<object[], T>(
			a =>
			{
				try
				{
					return transform(a);
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

		transformBlock.LinkTo(target);
		if (complete)
		{
			_ = transformBlock
				.Completion
				.ContinueWith(t =>
				{
					if (t.IsFaulted) target.Fault(t.Exception);
					else target.Complete();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Current);
		}

		return await reader.ToTargetBlockAsync(transformBlock, true, LocalPool, cancellationToken).ConfigureAwait(false);
	}
}
