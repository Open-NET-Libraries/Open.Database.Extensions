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
			int fieldCount = reader.FieldCount;
			int total = 0;
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
			int fieldCount = reader.FieldCount;
			int total = 0;
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
			int total = 0;
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
		=> Dataflow.Transformer<T>
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
		=> Dataflow.Transformer<T>
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
	public static long ToTargetBlock<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, null);
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
	public static long ToTargetBlock<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, fieldMappingOverrides);
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
	public static long ToTargetBlock<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, null);
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
	public static long ToTargetBlock<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, fieldMappingOverrides);
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
			int fieldCount = reader.FieldCount;
			int total = 0;
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
			int fieldCount = reader.FieldCount;
			int total = 0;
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
			int total = 0;
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
		=> Dataflow.Transformer<T>
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
		=> Dataflow.Transformer<T>
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
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

		return await ToTargetBlockMappedAsync(
			read => command.ExecuteReaderAsync(read, CommandBehavior.SingleResult, cancellationToken),
			target, complete, null, cancellationToken).ConfigureAwait(false);
	}

	/// <summary>
	/// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the target block.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target block to receive the results.</param>
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
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

		return await ToTargetBlockMappedAsync(
			read => command.ExecuteReaderAsync(read, CommandBehavior.SingleResult, cancellationToken),
			target, complete, fieldMappingOverrides, cancellationToken).ConfigureAwait(false);
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
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
	public static async ValueTask<long> ToTargetBlockAsync<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await Task.Yield();
		return await ToTargetBlockMappedAsync(
			read => command.ExecuteReaderAsync(read),
			target, complete, null, command.CancellationToken).ConfigureAwait(false);
	}

	/// <summary>
	/// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the target block.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target block to receive the results.</param>
	/// <param name="complete">If true, the <paramref name="target"/> is completed once every record read has been delivered to it (after the internal buffer has drained, which can be after this method has returned), or faulted with the exception if reading or mapping failed; it is never completed or faulted before the reader has been released.  If false, the target is left open and is never completed or faulted by this method; a record that cannot be mapped then ends the pipeline silently, because the mapping runs behind this call and no exception reaches the caller.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The number of records read and handed to the internal buffer.  Delivery to the <paramref name="target"/> continues in the background, so some of them may not have reached it yet when this returns.</returns>
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
		return await ToTargetBlockMappedAsync(
			read => command.ExecuteReaderAsync(read),
			target, complete, fieldMappingOverrides, command.CancellationToken).ConfigureAwait(false);
	}

	// Shared by the mapped command-level overloads.
	//
	// The internal pipeline (source end -> mapper -> target) exists before anything can fail, and the only thing this
	// code ever completes or faults is the pipeline's SOURCE end.  The target is completed (or faulted) downstream by the
	// pipeline itself, after every row written into it has been delivered; it is never completed directly here.
	static long ToTargetBlockMapped<T>(
		Func<Func<IDataReader, long>, long> executeReader,
		ITargetBlock<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
	{
		// The public overloads that call this have already validated their arguments (once); nothing here guards again.
		System.Diagnostics.Debug.Assert(executeReader is not null);
		System.Diagnostics.Debug.Assert(target is not null);

		var pipeline = new Dataflow.Transformer<T>.Pipeline(target, complete);
		long total;
		try
		{
			total = executeReader(reader => pipeline.Write(reader, Dataflow.Transformer<T>.Create(fieldMappingOverrides)));
		}
		catch (Exception ex)
		{
			pipeline.Fault(ex);
			throw;
		}

		// The reader has been disposed by now.
		pipeline.Complete();
		return total;
	}

	static async ValueTask<long> ToTargetBlockMappedAsync<T>(
		Func<Func<IDataReader, ValueTask<long>>, ValueTask<long>> executeReader,
		ITargetBlock<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides,
		CancellationToken cancellationToken)
		where T : new()
	{
		// The public overloads that call this have already validated their arguments (once); nothing here guards again.
		System.Diagnostics.Debug.Assert(executeReader is not null);
		System.Diagnostics.Debug.Assert(target is not null);

		var pipeline = new Dataflow.Transformer<T>.Pipeline(target, complete);
		long total;
		try
		{
			total = await executeReader(reader => pipeline.WriteAsync(reader, Dataflow.Transformer<T>.Create(fieldMappingOverrides), cancellationToken)).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			pipeline.Fault(ex);
			throw;
		}

		// The reader has been disposed by now.
		pipeline.Complete();
		return total;
	}
}
