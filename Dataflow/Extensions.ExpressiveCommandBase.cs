using System;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{

		/// <summary>
		/// Posts all records to a target block using the transform function.
		/// Stops if the target block rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public static void ToTargetBlock<T>(this ExpressiveCommandBase<, , , > source, ITargetBlock<T> target, Func<IDataRecord, T> transform)
			=> IterateReaderWhile(r => target.Post(transform(r)));

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// Stops if the channel rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public void ToChannel<T>(ChannelWriter<T> target, Func<IDataRecord, T> transform)
			=> IterateReaderWhile(r => target.TryWrite(transform(r)));

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// Stops if the channel rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public void ToChannel<T>(Channel<T> target, Func<IDataRecord, T> transform)
			=> ToChannel(target.Writer, transform);

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (deferred: data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>A reader of an unbounded channel containing the results.</returns>
		public ChannelReader<T> AsChannel<T>(
			Func<IDataRecord, T> transform,
			bool synchronousExecution = false)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
			{
				SingleWriter = true,
				AllowSynchronousContinuations = true
			});

			void I()
			{
				ToChannel(channel, transform);
				channel.Writer.Complete();
			}

			if (synchronousExecution) I();
			else Task.Run(I);
			return channel.Reader;
		}

		/// <summary>
		/// Returns a buffer block that will contain the results.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (deferred: data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>The buffer block that will contain the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlock<T>(
			Func<IDataRecord, T> transform,
			bool synchronousExecution = false)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var source = new BufferBlock<T>();
			void I()
			{
				ToTargetBlock(source, transform);
				source.Complete();
			}

			if (synchronousExecution) I();
			else Task.Run(I);
			return source;
		}

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public virtual IReceivableSourceBlock<T> AsSourceBlock<T>(
			IEnumerable<(string Field, string Column)>? fieldMappingOverrides,
			bool synchronousExecution = false,
			ExecutionDataflowBlockOptions? options = null)
		   where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;

			if (synchronousExecution)
			{
				var q = x.Results(out var deferred, options);
				ExecuteReader(reader =>
				{
					// Ignores fields that don't match.
					// ReSharper disable once PossibleMultipleEnumeration
					var columns = reader.GetMatchingOrdinals(cn, true);

					var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
					deferred(new QueryResult<IEnumerable<object[]>>(
						ordinalValues,
						columns.Select(c => c.Name),
						reader.AsEnumerable(ordinalValues)));
				});
				return q;
			}
			else
			{
				var q = x.ResultsAsync(out var deferred, options);
				ExecuteReader(reader =>
				{
					// Ignores fields that don't match.
					// ReSharper disable once PossibleMultipleEnumeration
					var columns = reader.GetMatchingOrdinals(cn, true);

					var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
					deferred(new QueryResult<IEnumerable<object[]>>(
						ordinalValues,
						columns.Select(c => c.Name),
						reader.AsEnumerable(ordinalValues)));
				});
				return q;
			}

		}


		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlock<T>(
			IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides,
			bool synchronousExecution = false,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
			=> AsSourceBlock<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), synchronousExecution, options);

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlock<T>(params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> AsSourceBlock<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>);
	}
}
