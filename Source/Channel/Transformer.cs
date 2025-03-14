namespace Open.Database.Extensions;

/// <summary>Constructs a <see cref="Transformer{T}"/>.</summary>
/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
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
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The ChannelReader of the target.</returns>
	internal async ValueTask<long> PipeResultsTo(IDataReader reader, ChannelWriter<T> target, bool complete, CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(ColumnNames, true);
		var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
		var names = columns.Select(m => m.Name).ToImmutableArray();

		var processor = new Processor(this, names);
		Func<object?[], T> transform = processor.Transform;

		Channel<object[]> channel = ChannelDbExtensions.CreateChannel<object[]>(MaxArrayBuffer, true);
		ChannelWriter<object[]> writer = channel.Writer;

		ValueTask<long> piped = channel
			.Reader
			.Transform(a =>
			{
				try
				{
					return transform(a);
				}
				finally
				{
					LocalPool.Return(a);
				}
			})
			.PipeTo(target, complete, cancellationToken);

		if (complete)
		{
			_ = piped
				.AsTask()
				.ContinueWith(t =>
				{
					if (t.IsFaulted) target.Complete(t.Exception);
					else target.Complete();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Current);
		}

		return await reader
			.ToChannel(writer, false, LocalPool, cancellationToken).ConfigureAwait(false);
	}

#if NETSTANDARD2_0
#else
	/// <summary>
	/// Transforms the results from the reader by first buffering the results and if/when the buffer size is reached, the results are transformed to a channel for reading.
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">Will call complete when no more results are avaiable.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The ChannelReader of the target.</returns>
	internal ValueTask<long> PipeResultsToAsync(DbDataReader reader, ChannelWriter<T> target, bool complete, CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(ColumnNames, true);
		var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
		var names = columns.Select(m => m.Name).ToImmutableArray();

		var processor = new Processor(this, names);
		Func<object?[], T> transform = processor.Transform;

		Channel<object[]> channel = ChannelDbExtensions.CreateChannel<object[]>(MaxArrayBuffer, true);
		ChannelWriter<object[]> writer = channel.Writer;

		ValueTask<long> piped = channel
			.Reader
			.Transform(a =>
			{
				try
				{
					return transform(a);
				}
				finally
				{
					LocalPool.Return(a);
				}
			})
			.PipeTo(target, complete, cancellationToken);

		if (complete)
		{
			_ = piped
				.AsTask()
				.ContinueWith(t =>
				{
					if (t.IsFaulted) target.Complete(t.Exception);
					else target.Complete();
				},
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Current);
		}

		return reader
			.ToChannelAsync(writer, false, LocalPool, cancellationToken);
	}
#endif

}
