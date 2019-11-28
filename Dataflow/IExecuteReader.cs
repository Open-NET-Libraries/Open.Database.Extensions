using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Data.Common;
using System.Threading;
using System.Collections.Immutable;
using Open.Database.Extensions.Dataflow;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class DataflowExtensions
	{
		/// <summary>
		/// Posts all records to a target block using the transform function.
		/// Stops if the target block rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="command">The command to execute.</param>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public static void ToTargetBlock<T>(this IExecuteReader command,
			ITargetBlock<T> target, Func<IDataRecord, T> transform)
			=> command.IterateReaderWhile(r => target.Post(transform(r)));

		/// <summary>
		/// Returns a buffer block that will contain the results.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="command">The command to execute.</param>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (deferred: data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>The buffer block that will contain the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlock<T>(
			this IExecuteReader command,
			Func<IDataRecord, T> transform,
			bool synchronousExecution = false)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var source = new BufferBlock<T>();
			void I()
			{
				ToTargetBlock(command, source, transform);
				source.Complete();
			}

			if (synchronousExecution) I();
			else
			{
				Task.Run(I)
					.ContinueWith(
						t => ((IDataflowBlock)source).Fault(t.Exception),
						CancellationToken.None,
						TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
						TaskScheduler.Current);
			}

			return source;
		}

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to execute.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlock<T>(
			this IExecuteReader command,
			IEnumerable<(string Field, string Column)>? fieldMappingOverrides,
			bool synchronousExecution = false,
			ExecutionDataflowBlockOptions? options = null)
		   where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;

			if (synchronousExecution)
			{
				var q = x.Results(out var deferred, options);
				command.ExecuteReader(reader =>
				{
					// Ignores fields that don't match.
					var columns = reader.GetMatchingOrdinals(cn, true);

					var ordinalValues = columns.Select(c => c.Ordinal).ToImmutableArray();
					deferred(new QueryResult<IEnumerable<object[]>>(
						ordinalValues,
						columns.Select(c => c.Name).ToImmutableArray(),
						reader.AsEnumerable(ordinalValues)));
				});
				return q;
			}
			else
			{
				var q = x.ResultsAsync(out var deferred, options);
				Task.Run(async () =>
					{
						await command.ExecuteReaderAsync(async reader =>
						{
							// Ignores fields that don't match.
							var columns = reader.GetMatchingOrdinals(cn, true);

							var ordinalValues = columns.Select(c => c.Ordinal).ToImmutableArray();
							await deferred(new QueryResult<IEnumerable<object[]>>(
								ordinalValues,
								columns.Select(c => c.Name).ToImmutableArray(),
								reader.AsEnumerable(ordinalValues)));
						});
					})
					.ContinueWith(
						t => {
							if (t.IsFaulted) q.Fault(t.Exception);
							else q.Complete();
						},
						CancellationToken.None,
						TaskContinuationOptions.ExecuteSynchronously,
						TaskScheduler.Current);

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
		public static IReceivableSourceBlock<T> AsSourceBlock<T>(
			this IExecuteReader command,
			IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides,
			bool synchronousExecution = false,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
			=> AsSourceBlock<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), synchronousExecution, options);

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlock<T>(
			this IExecuteReader command,
			params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> AsSourceBlock<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Posts all transformed records to the provided target block.
		/// If .Complete is called on the target block, then the iteration stops.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="target">The target block to receive the records.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public static async ValueTask ToTargetBlockAsync<T>(
			this IExecuteReaderAsync command,
			ITargetBlock<T> target, Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var lastSend = Task.FromResult(true);
			await command.IterateReaderWhileAsync(async r =>
			{
				if (!await lastSend.ConfigureAwait(false))
					return false;

				var value = transform(r);
				lastSend = target.SendAsync(value, command.CancellationToken);
				return true;
			});
			await lastSend.ConfigureAwait(false);
		}

		/// <summary>
		/// Provides a BufferBlock as the source of records.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="options">The optional DataflowBlockOptions to use with the source.</param>
		/// <returns>A buffer block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			this IExecuteReaderAsync command,
			Func<IDataRecord, T> transform,
			DataflowBlockOptions? options = null)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var source = options == null
				? new BufferBlock<T>()
				: new BufferBlock<T>(options);

			Task.Run(async () => await ToTargetBlockAsync(command, source, transform))
				.ContinueWith(
					t => {
						if (t.IsFaulted) ((ITargetBlock<T>)source).Fault(t.Exception);
						else source.Complete();
					},
					CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously,
					TaskScheduler.Current);

			return source;
		}

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			this IExecuteReaderAsync command,
			IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
			=> AsSourceBlockAsync<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), options);

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			this IExecuteReaderAsync command,
			params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> AsSourceBlockAsync<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			this IExecuteReaderAsync command,
			ExecutionDataflowBlockOptions options,
			params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> AsSourceBlockAsync<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>, options);

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			this IExecuteReaderAsync command,
			IEnumerable<(string Field, string Column)>? fieldMappingOverrides,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;
			var block = x.ResultsBlock(out var initColumnNames, options);

			Task.Run(async () => await command.ExecuteReaderAsync(reader =>
					{
						// Ignores fields that don't match.
						var columns = reader.GetMatchingOrdinals(cn, true);

						var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
						initColumnNames(columns.Select(c => c.Name).ToArray());

						return reader is DbDataReader dbr
							? dbr.ToTargetBlockAsync(block,
								r => r.GetValuesFromOrdinals(ordinalValues),
								command.UseAsyncRead,
								command.CancellationToken)
							: reader.ToTargetBlockAsync(block,
								r => r.GetValuesFromOrdinals(ordinalValues),
								command.CancellationToken);
					}))
				.ContinueWith(
					t => {
						if (t.IsFaulted) ((ITargetBlock<object[]>)block).Fault(t.Exception);
						else block.Complete();
					},
					CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously,
					TaskScheduler.Current);

			return block;
		}
	}
}
