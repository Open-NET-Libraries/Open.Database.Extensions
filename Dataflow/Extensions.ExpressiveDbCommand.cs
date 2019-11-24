using System;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{
			   
		/// <summary>
		/// Posts all transformed records to the provided target block.
		/// If .Complete is called on the target block, then the iteration stops.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="target">The target block to receive the records.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public ValueTask ToTargetBlockAsync<T>(ITargetBlock<T> target, Func<IDataRecord, T> transform)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var lastSend = new ValueTask<bool>(true);
			return IterateReaderWhileAsync(async r =>
			{
				var ok = await lastSend;
				if (ok)
				{
					var value = transform(r);
					lastSend = target.Post(value)
						? new ValueTask<bool>(true)
						: new ValueTask<bool>(target.SendAsync(value, CancellationToken));
				}
				return ok;
			});
		}

		/// <summary>
		/// Provides a BufferBlock as the source of records.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="options">The optional DataflowBlockOptions to use with the source.</param>
		/// <returns>A buffer block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			Func<IDataRecord, T> transform,
			DataflowBlockOptions? options = null)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var source = options == null
				? new BufferBlock<T>()
				: new BufferBlock<T>(options);

			ToTargetBlockAsync(source, transform)
				.AsTask()
				.ContinueWith(t =>
				{
					if (t.IsFaulted) ((ITargetBlock<T>)source).Fault(t.Exception);
					else source.Complete();
				});

			return source;
		}

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides,
			ExecutionDataflowBlockOptions? options = null)
		   where T : new()
			=> AsSourceBlockAsync<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), options);

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlockAsync<T>(params (string Field, string Column)[] fieldMappingOverrides)
		where T : new()
			=> AsSourceBlockAsync<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
				ExecutionDataflowBlockOptions options,
				params (string Field, string Column)[] fieldMappingOverrides)
		where T : new()
			=> AsSourceBlockAsync<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>, options);


		/// <summary>
		/// Returns a source block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="options">The optional ExecutionDataflowBlockOptions to use with the source.</param>
		/// <returns>A transform block that is receiving the results.</returns>
		public IReceivableSourceBlock<T> AsSourceBlockAsync<T>(
			IEnumerable<(string Field, string Column)>? fieldMappingOverrides,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;
			var block = x.ResultsBlock(out var initColumnNames, options);

			ExecuteReaderAsync(reader =>
			{
				// Ignores fields that don't match.
				var columns = reader.GetMatchingOrdinals(cn, true);

				var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
				initColumnNames(columns.Select(c => c.Name).ToArray());

				return reader.ToTargetBlockAsync(block,
					r => r.GetValuesFromOrdinals(ordinalValues),
					UseAsyncRead,
					CancellationToken);
			})
			.AsTask()
			.ContinueWith(t =>
			{
				if (t.IsFaulted) ((ITargetBlock<object[]>)block).Fault(t.Exception);
				else block.Complete();
			});

			return block;
		}

	}
}
