using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace Open.Database.Extensions.Channel;

/// <summary>Constructs a <see cref="Transformer{T}"/>.</summary>
/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
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

	private ImmutableArray<string> ResolveNames(IDataReader reader)
		=> reader.GetMatchingOrdinals(ColumnNames, true).Select(m => m.Name).ToImmutableArray();

	/// <summary>
	/// Creates the pipeline that buffers the records of a reader (stage 1) and maps them into the <paramref name="target"/> (stage 2).
	/// Stage 2 starts immediately, so the caller must make sure the pipeline's <see cref="Pipeline.Buffer"/> gets completed.
	/// </summary>
	/// <param name="target">The target channel to write to.</param>
	/// <param name="complete">If true, the target is completed after the last record has been delivered, or with the error that ended the pipeline.</param>
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

		_ = ResolveNames(reader); // Fail before anything is started if the reader doesn't have the columns.
		Pipeline pipeline = CreatePipeline(target, complete, cancellationToken);
		return pipeline.RunAsync(() => pipeline.Read(reader));
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

		_ = ResolveNames(reader);
		Pipeline pipeline = CreatePipeline(target, complete, cancellationToken);
		return pipeline.RunAsync(() => pipeline.ReadAsync(reader));
	}

	/// <summary>
	/// A bounded buffer (<see cref="Buffer"/>) that stage 1 fills from a reader, and the second stage that drains it
	/// into the target on its own task (so the producing thread isn't the one mapping records and waiting on a slow consumer).
	/// </summary>
	/// <remarks>
	/// Completion flows downstream: whoever runs stage 1 completes the buffer (with the error if reading failed),
	/// and once the second stage has delivered everything that was buffered, it completes the target.
	/// </remarks>
	internal sealed class Pipeline
	{
		private readonly Transformer<T> _owner;
		private readonly Processor _processor;
		private readonly CancellationTokenSource _stop;
		private readonly ChannelWriter<T> _target;
		private readonly bool _complete;
		private readonly CancellationToken _cancellationToken;
		private volatile Exception? _failure;

		internal Pipeline(Transformer<T> owner, ChannelWriter<T> target, bool complete, CancellationToken cancellationToken)
		{
			_owner = owner;
			_target = target;
			_complete = complete;
			_cancellationToken = cancellationToken;
			_processor = new Processor(owner); // The column names are only known once there is a reader.
			_stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			Token = _stop.Token;

			Channel<object[]> buffer = ChannelDbExtensions.CreateChannel<object[]>(MaxArrayBuffer, true);
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

		/// <summary>Stage 1: writes the records of the reader to the <see cref="Buffer"/> without completing it.</summary>
		public ValueTask<long> Read(IDataReader reader)
		{
			_processor.SetNames(_owner.ResolveNames(reader));
			return reader.ToChannel(Buffer, false, LocalPool, Token);
		}

		/// <summary>Stage 1, using asynchronous reads: writes the records of the reader to the <see cref="Buffer"/> without completing it.</summary>
		public ValueTask<long> ReadAsync(DbDataReader reader)
		{
			_processor.SetNames(_owner.ResolveNames(reader));
			return reader.ToChannelAsync(Buffer, false, LocalPool, Token);
		}

		/// <summary>
		/// Runs stage 1 and guarantees the <see cref="Buffer"/> ends up completed, so the second stage can never be left waiting.
		/// </summary>
		/// <remarks>
		/// If stage 1 was stopped because the second stage failed, the second stage's failure is thrown instead of the cancellation it caused.
		/// </remarks>
		public async ValueTask<long> RunAsync(Func<ValueTask<long>> stage1)
		{
			Exception? error = null;
			try
			{
				long count = await stage1().ConfigureAwait(false);

				ThrowIfSecondStageFailed(); // Reading can also end quietly (not by throwing) when the second stage has stopped it.

				return count;
			}
			catch (Exception ex)
			{
				error = ex;
				if (ex is OperationCanceledException)
					ThrowIfSecondStageFailed();

				throw;
			}
			finally
			{
				Buffer.TryComplete(error); // Already completed (by the core) when this is the command-level path.
			}
		}

		private void ThrowIfSecondStageFailed()
		{
			if (_failure is { } failure)
				ExceptionDispatchInfo.Capture(failure).Throw();
		}

		// Stage 2. Maps the buffered records into the target until the buffer has been completed and drained.
		// If the buffer was completed with an error, that error surfaces here only after everything buffered has been delivered.
		private async Task DeliverAsync(ChannelReader<object[]> buffer)
		{
			Exception? error = null;
			try
			{
				while (await buffer.WaitToReadAsync(_cancellationToken).ConfigureAwait(false))
				{
					while (buffer.TryRead(out object[]? row))
					{
						T item;
						try
						{
							item = _processor.Transform(row);
						}
						finally
						{
							LocalPool.Return(row);
						}

						await _target.WriteAsync(item, _cancellationToken).ConfigureAwait(false);
					}
				}
			}
			catch (Exception ex)
			{
				error = ex;
			}

			_failure = error;
			_stop.Cancel(); // Stage 1 can never be left waiting for room in a buffer that nothing drains any more.
			_stop.Dispose();

			if (_complete)
				_target.TryComplete(error);
		}
	}
}
