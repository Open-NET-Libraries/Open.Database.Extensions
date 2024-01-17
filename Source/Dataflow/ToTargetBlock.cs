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

namespace Open.Database.Extensions;

/// <summary>
/// Extensions for pipelining data with Dataflow blocks.
/// </summary>
public static partial class DataflowExtensions
{
	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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
	/// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the target block.
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
	/// <returns>The number of records processed.</returns>
	public static long ToTargetBlock<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
		=> Transformer<T>
			.Create()
			.PipeResultsTo(reader, target, complete);

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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
		=> Transformer<T>
			.Create(fieldMappingOverrides)
			.PipeResultsTo(reader, target, complete);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false, arrayPool));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false, transform));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
	/// <returns>The number of records processed.</returns>
	public static long ToTargetBlock<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false, fieldMappingOverrides));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and posts each record as an array to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false, arrayPool));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the target block.
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

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, false, transform));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
	/// <returns>The number of records processed.</returns>
	public static long ToTargetBlock<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		try
		{
			return command.ExecuteReader(reader =>
				ToTargetBlock(reader, target, complete, fieldMappingOverrides));
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
				{
					total++;
				}
			}
			else
			{
				while (
					reader.Read()
					&& await target.SendAsync(reader.GetValues(fieldCount), cancellationToken).ConfigureAwait(false))
				{
					total++;
				}
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
				{
					total++;
				}
			}
			else
			{
				while (
					reader.Read()
					&& await target.SendAsync(reader.GetValues(fieldCount, arrayPool), cancellationToken).ConfigureAwait(false))
				{
					total++;
				}
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
				{
					total++;
				}
			}
			else
			{
				while (
					reader.Read()
					&& await target.SendAsync(transform(reader), cancellationToken).ConfigureAwait(false))
				{
					total++;
				}
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
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToTargetBlockAsync<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Transformer<T>
			.Create()
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
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
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await Task.Yield();

		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await Task.Yield();

		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, arrayPool), cancellationToken: cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await Task.Yield();

		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, transform), cancellationToken: cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static async ValueTask<long> ToTargetBlockAsync<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await Task.Yield();

		try
		{
			var dbc = command as DbCommand;
			var state = dbc == null ? command.Connection.EnsureOpen() : await dbc.Connection!.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
			var behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = dbc == null ? command.ExecuteReader(behavior) : await dbc.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			return await ToTargetBlockAsync(reader, target, false, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await Task.Yield();

		try
		{
			var dbc = command as DbCommand;
			var state = dbc == null ? command.Connection.EnsureOpen() : await dbc.Connection!.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
			var behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = dbc == null ? command.ExecuteReader(behavior) : await dbc.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			return await ToTargetBlockAsync(reader, target, false, fieldMappingOverrides, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, command.CancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, arrayPool, command.CancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, transform, command.CancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
	/// <returns>The number of records processed.</returns>
	public static async ValueTask<long> ToTargetBlockAsync<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await Task.Yield();
		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, command.CancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
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
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await Task.Yield();
		try
		{
			return await command.ExecuteReaderAsync(reader =>
				ToTargetBlockAsync(reader, target, false, fieldMappingOverrides, command.CancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			if (complete)
			{
				complete = false;
				target.Fault(ex);
			}
			throw;
		}
		finally
		{
			if (complete)
				target.Complete();
		}
	}
}
