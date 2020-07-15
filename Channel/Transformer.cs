using Open.ChannelExtensions;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	internal class Transformer<T> : Core.Transformer<T>
		where T : new()
	{
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		public Transformer(IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			: base(fieldMappingOverrides)
		{
		}

		/// <summary>
		/// Static utility for creating a Transformer <typeparamref name="T"/>.
		/// </summary>
		/// <param name="fieldMappingOverrides"></param>
		/// <returns></returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1000:Do not declare static members on generic types", Justification = "This is simply an expressive helper that would seem odd to make another static class to handle.")]
		public static new Transformer<T> Create(IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			=> new Transformer<T>(fieldMappingOverrides);

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

			var columns = reader.GetMatchingOrdinals(ColumnNames, true);
			var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
			var names = columns.Select(m => m.Name).ToImmutableArray();

			var processor = new Processor(this, names);
			var transform = processor.Transform;

			var channel = ChannelDbExtensions.CreateChannel<object?[]>(MaxArrayBuffer, true);
			var writer = channel.Writer;

			var piped = channel
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

#if NETSTANDARD2_1
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

			var columns = reader.GetMatchingOrdinals(ColumnNames, true);
			var ordinals = columns.Select(m => m.Ordinal).ToImmutableArray();
			var names = columns.Select(m => m.Name).ToImmutableArray();

			var processor = new Processor(this, names);
			var transform = processor.Transform;

			var channel = ChannelDbExtensions.CreateChannel<object?[]>(MaxArrayBuffer, true);
			var writer = channel.Writer;

			var piped = channel
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
}
