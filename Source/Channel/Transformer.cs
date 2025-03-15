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
	[ExcludeFromCodeCoverage]
	public static new Transformer<T> Create(IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
		=> new(fieldMappingOverrides);

	/// <summary>
	/// Transforms the results from the reader by first buffering the results and then the final results are transformed to the target channel for reading.
	/// </summary>
	/// <remarks>
	/// This is necessary to absorb as much data as possible first and defer the transformation till later.
	/// </remarks>
	private ChannelWriter<object[]> PipeResultsToPrep(
		IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(ColumnNames, true);
		var names = columns.Select(m => m.Name).ToImmutableArray();

		var processor = new Processor(this, names);
		Func<object?[], T> transform = processor.Transform;

		Channel<object[]> channel = ChannelDbExtensions.CreateChannel<object[]>(MaxArrayBuffer, true);
		ChannelWriter<object[]> writer = channel.Writer;

		_ = channel
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
			.PipeTo(target, complete, cancellationToken)
			.AsTask();

		return writer;
	}

	/// <summary>
	/// Transforms the results from the reader by first buffering the results and if/when the buffer size is reached, the results are transformed to a channel for reading.
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">Will call complete when no more results are avaiable.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The ChannelReader of the target.</returns>
	internal ValueTask<long> PipeResultsTo(
		IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken)
		=> reader.ToChannel(
			PipeResultsToPrep(reader, target, complete, cancellationToken),
			true, LocalPool, cancellationToken);

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
	[ExcludeFromCodeCoverage]
	internal ValueTask<long> PipeResultsToAsync(
		DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken)
		=> reader.ToChannelAsync(
			PipeResultsToPrep(reader, target, complete, cancellationToken),
			true, LocalPool, cancellationToken);
#endif

}
