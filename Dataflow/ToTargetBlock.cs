using Open.Database.Extensions.Dataflow;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Extensions for pipelining data with Dataflow blocks.
	/// </summary>
	public static partial class DataflowExtensions
	{
		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IDataReader reader,
			ITargetBlock<object[]> target,
			bool complete)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			try
			{
				var fieldCount = reader.FieldCount;
				var total = 0;
				while (reader.Read() && target.Post(reader.GetValues(fieldCount))) total++;
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IDataReader reader,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			try
			{
				var fieldCount = reader.FieldCount;
				var total = 0;
				while (reader.Read() && target.Post(reader.GetValues(fieldCount, arrayPool))) total++;
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IDataReader reader,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			try
			{
				var total = 0;
				while (reader.Read() && target.Post(transform(reader))) total++;
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IDataReader reader,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			where T : new()
			=> Transformer<T>
				.Create(fieldMappingOverrides)
				.PipeResultsTo(reader, target, complete);

		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IDbCommand command,
			ITargetBlock<object[]> target,
			bool complete)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete));
		}

		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IDbCommand command,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, arrayPool));
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IDbCommand command,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, transform));
		}

		/// <summary>
		/// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IDbCommand command,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, fieldMappingOverrides));
		}

		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IExecuteReader command,
			ITargetBlock<object[]> target,
			bool complete)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete));
		}

		/// <summary>
		/// Iterates an IDataReader and posts each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock(this IExecuteReader command,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, arrayPool));
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IExecuteReader command,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, transform));
		}

		/// <summary>
		/// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static long ToTargetBlock<T>(this IExecuteReader command,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, fieldMappingOverrides));
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IDataReader reader,
			ITargetBlock<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			try
			{
				var fieldCount = reader.FieldCount;
				var total = 0;
				if (reader is DbDataReader r)
				{
					while (
						await r.ReadAsync(cancellationToken).ConfigureAwait(false)
						&& await target.SendAsync(r.GetValues(fieldCount), cancellationToken).ConfigureAwait(false))
						total++;
				}
				else
				{
					while (
						reader.Read()
						&& await target.SendAsync(reader.GetValues(fieldCount), cancellationToken).ConfigureAwait(false))
						total++;
				}
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IDataReader reader,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			try
			{
				var fieldCount = reader.FieldCount;
				var total = 0;
				if (reader is DbDataReader r)
				{
					while (
						await r.ReadAsync(cancellationToken).ConfigureAwait(false)
						&& await target.SendAsync(r.GetValues(fieldCount, arrayPool), cancellationToken).ConfigureAwait(false))
						total++;
				}
				else
				{
					while (
						reader.Read()
						&& await target.SendAsync(reader.GetValues(fieldCount, arrayPool), cancellationToken).ConfigureAwait(false))
						total++;
				}
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync<T>(this IDataReader reader,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			try
			{
				var total = 0;
				if (reader is DbDataReader r)
				{
					while (
						await r.ReadAsync(cancellationToken).ConfigureAwait(false)
						&& await target.SendAsync(transform(reader), cancellationToken).ConfigureAwait(false))
						total++;
				}
				else
				{
					while (
						reader.Read()
						&& await target.SendAsync(transform(reader), cancellationToken).ConfigureAwait(false))
						total++;
				}
				if (complete) target.Complete();
				return total;
			}
			catch (Exception ex)
			{
				if (complete) target.Fault(ex);
				throw;
			}
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the target block.
		/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToTargetBlockAsync<T>(this IDataReader reader,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
			CancellationToken cancellationToken = default)
			where T : new()
			=> Transformer<T>
				.Create(fieldMappingOverrides)
				.PipeResultsToAsync(reader, target, complete, cancellationToken);

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IDbCommand command,
			ITargetBlock<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await Task.Yield();

			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, cancellationToken), cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IDbCommand command,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await Task.Yield();

			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, arrayPool), cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync<T>(this IDbCommand command,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await Task.Yield();

			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, transform), cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync<T>(this IDbCommand command,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
			CancellationToken cancellationToken = default)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await Task.Yield();

			var dbc = command as DbCommand;
			var state = dbc == null ? command.Connection.EnsureOpen() : await dbc.Connection.EnsureOpenAsync(cancellationToken);
			var behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = dbc == null ? command.ExecuteReader(behavior) : await dbc.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			return await ToTargetBlockAsync(reader, target, complete, fieldMappingOverrides, cancellationToken);
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IExecuteReader command,
			ITargetBlock<object[]> target,
			bool complete)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			await Task.Yield();
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, command.CancellationToken));
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync(this IExecuteReader command,
			ITargetBlock<object[]> target,
			bool complete,
			ArrayPool<object> arrayPool)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
			Contract.EndContractBlock();

			await Task.Yield();
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, arrayPool, command.CancellationToken));
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync<T>(this IExecuteReader command,
			ITargetBlock<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			await Task.Yield();
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, transform, command.CancellationToken));
		}

		/// <summary>
		/// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToTargetBlockAsync<T>(this IExecuteReader command,
			ITargetBlock<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			await Task.Yield();
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, complete, fieldMappingOverrides, command.CancellationToken));
		}
	}
}
