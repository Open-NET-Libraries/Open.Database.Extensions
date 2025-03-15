using System.Buffers;

namespace Open.Database.Extensions;

/// <summary>
/// Extensions for writing data to a channel.
/// </summary>
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Simplification and readability.")]
public static partial class ChannelDbExtensions
{
	private static async ValueTask<long> ToChannelCore<T>(
		IDbCommand command,
		ChannelWriter<T> writer,
		bool complete,
		CancellationToken cancellationToken,
		Func<IDataReader, ValueTask<long>> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
		{
			await writer
				.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken)
				.ConfigureAwait(false);
		}

		if (!complete)
		{
			await command
				.ExecuteReaderAsync(transform, cancellationToken: cancellationToken)
				.ConfigureAwait(false);
		}

		Exception? exception = null;
		try
		{
			return await command
				.ExecuteReaderAsync(transform, cancellationToken: cancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			exception = ex;
			throw;
		}
		finally
		{
			writer.TryComplete(exception);
		}
	}

	private static async ValueTask<long> ToChannelCore<T>(
		IExecuteReader command,
		ChannelWriter<T> writer,
		bool complete,
		Func<IDataReader, ValueTask<long>> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		Contract.EndContractBlock();

		await writer
			.WaitToWriteAndThrowIfClosedAsync(true, command.CancellationToken)
			.ConfigureAwait(false);

		if (!complete)
		{
			await command
				.ExecuteReaderAsync(transform)
				.ConfigureAwait(false);
		}

		Exception? exception = null;
		try
		{
			return await command
				.ExecuteReaderAsync(transform)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			exception = ex;
			throw;
		}
		finally
		{
			if (complete)
				writer.TryComplete(exception);
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return target.WriteAll(
			reader.AsEnumerable(),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return target.WriteAll(
			reader.AsEnumerable(arrayPool),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return target.WriteAll(
			reader.Select(transform, cancellationToken),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Transformer<T>
			.Create()
			.PipeResultsTo(reader, target, complete, cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Transformer<T>
			.Create(fieldMappingOverrides)
			.PipeResultsTo(reader, target, complete, cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IDbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, cancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IDbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, arrayPool, cancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

		Exception? exception = null;
		try
		{
			ConnectionState state = command.Connection.EnsureOpen();
			CommandBehavior behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using IDataReader reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			return await reader.ToChannel(target, false, transform, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			exception = ex;
			throw;
		}
		finally
		{
			if (complete)
				target.TryComplete(exception);
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, cancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, fieldMappingOverrides, cancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IExecuteReader command,
		ChannelWriter<object[]> target,
		bool complete)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel(this IExecuteReader command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, arrayPool, command.CancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and through the transform function and posts each record it to the target channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, transform, command.CancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete)
		where T : new()
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, fieldMappingOverrides, command.CancellationToken));

#if NETSTANDARD2_0
#else

	private static async ValueTask<long> ToChannelAsyncCore<T>(
		DbCommand command,
		ChannelWriter<T> writer,
		bool complete,
		CancellationToken cancellationToken,
		Func<DbDataReader, ValueTask<long>> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");

		if (!command.Connection.State.HasFlag(ConnectionState.Open))
		{
			await writer
				.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken)
				.ConfigureAwait(false);
		}

		if (!complete)
		{
			await command
				.ExecuteReaderAsync(transform, cancellationToken: cancellationToken)
				.ConfigureAwait(false);
		}

		Exception? exception = null;
		try
		{
			return await command
				.ExecuteReaderAsync(transform, cancellationToken: cancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			exception = ex;
			throw;
		}
		finally
		{
			writer.TryComplete(exception);
		}
	}

	private static async ValueTask<long> ToChannelAsyncCore<T>(
		IExecuteReaderAsync command,
		ChannelWriter<T> writer,
		bool complete,
		Func<IDataReader, ValueTask<long>> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (writer is null) throw new ArgumentNullException(nameof(writer));
		Contract.EndContractBlock();

		await writer
			.WaitToWriteAndThrowIfClosedAsync(true, command.CancellationToken)
			.ConfigureAwait(false);

		if (!complete)
		{
			await command
				.ExecuteReaderAsync(transform)
				.ConfigureAwait(false);
		}

		Exception? exception = null;
		try
		{
			return await command
				.ExecuteReaderAsync(transform)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			exception = ex;
			throw;
		}
		finally
		{
			if (complete)
				writer.TryComplete(exception);
		}
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return target.WriteAllAsync(
			reader.AsAsyncEnumerable(cancellationToken),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return target.WriteAllAsync(
			reader.AsAsyncEnumerable(arrayPool, cancellationToken),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return target.WriteAllAsync(
			reader.SelectAsync(transform, cancellationToken),
			complete,
			false,
			cancellationToken);
	}

	/// <summary>
	/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Transformer<T>
			.Create()
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Transformer<T>
			.Create(fieldMappingOverrides)
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync(this DbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, cancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync(this DbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, arrayPool, cancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		return ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, transform, cancellationToken));
	}

	/// <summary>
	/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, cancellationToken));

	/// <summary>
	/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The DbCommand to generate a reader from.</param>
	/// <param name="target">The target channel to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, fieldMappingOverrides, cancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
		ChannelWriter<object[]> target,
		bool complete)
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, command.CancellationToken)
				: reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool)
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, arrayPool, command.CancellationToken)
				: reader.ToChannel(target, false, arrayPool, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform)
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, transform, command.CancellationToken)
				: reader.ToChannel(target, false, transform, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete)
		where T : new()
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, command.CancellationToken)
				: reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
	/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
	/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="target">The target channel writer to receive the results.</param>
	/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The number of records processed.</returns>
	public static ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, fieldMappingOverrides, command.CancellationToken)
				: reader.ToChannel(target, false, fieldMappingOverrides, command.CancellationToken));
#endif

}
