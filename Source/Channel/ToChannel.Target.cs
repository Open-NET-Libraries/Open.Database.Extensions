using System.Diagnostics;
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
		Contract.EndContractBlock();

		if ((command.Connection.State & ConnectionState.Open) == 0)
		{
			await writer
				.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken)
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
			if (complete)
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
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once all the records have been written to it (or the source is empty).</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel(this IDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> target.WriteAll(
			reader.AsEnumerable(),
			complete,
			false,
			cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once all the records have been written to it (or the source is empty).</param>
	/// <param name="arrayPool">The array pool to rent each record's array from, or <see langword="null"/> to allocate new arrays. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel(this IDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object>? arrayPool,
		CancellationToken cancellationToken = default)
		=> target.WriteAll(
			reader.AsEnumerable(arrayPool),
			complete,
			false,
			cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once all the records have been written to it (or the source is empty).</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
		=> target.WriteAll(
			reader.Select(transform, cancellationToken),
			complete,
			false,
			cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it, or with the error that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read. The task completes once all the records have been read; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> Channel.Transformer<T>
			.Create()
			.PipeResultsTo(reader, target, complete, cancellationToken);

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it, or with the error that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read. The task completes once all the records have been read; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<long> ToChannel<T>(this IDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> Channel.Transformer<T>
			.Create(map)
			.PipeResultsTo(reader, target, complete, cancellationToken);

	/// <summary>
	/// Iterates the results of an <see cref="IDbCommand"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The command has no connection.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel(this IDbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, cancellationToken));

	/// <summary>
	/// Iterates the results of an <see cref="IDbCommand"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The command has no connection.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel(this IDbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
		=> ToChannelCore(
			command, target, complete, cancellationToken,
			reader => ToChannel(reader, target, false, arrayPool, cancellationToken));

	/// <summary>
	/// Iterates the results of an <see cref="IDbCommand"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The command has no connection.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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

		if ((command.Connection.State & ConnectionState.Open) == 0)
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

		Exception? exception = null;
		try
		{
			CommandBehavior behavior = CommandBehavior.SingleResult;
			IDataReader reader = await CommandExtensions.OpenAndExecuteReaderAsync(command, behavior, static (c, b, ct) => c.ExecuteReaderAsync(b, ct).AsTask(), cancellationToken).ConfigureAwait(false);

			using (reader)
			{
				return await reader.ToChannel(target, false, transform, cancellationToken).ConfigureAwait(false);
			}
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
	/// Iterates the results of an <see cref="IDbCommand"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it (or with the error that ended the operation), and only once the database reader has been released.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read. The task completes once all the records have been read and the reader has been released; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The command has no connection.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToChannel<T>(this IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelMapped(command, target, complete, null, cancellationToken);

	/// <summary>
	/// Iterates the results of an <see cref="IDbCommand"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it (or with the error that ended the operation), and only once the database reader has been released.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read. The task completes once all the records have been read and the reader has been released; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">The command has no connection.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<long> ToChannel<T>(this IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> ToChannelMapped(command, target, complete, map, cancellationToken);

	/// <summary>
	/// Iterates the results of an <see cref="IExecuteReader"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	public static ValueTask<long> ToChannel(this IExecuteReader command,
		ChannelWriter<object[]> target,
		bool complete)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Iterates the results of an <see cref="IExecuteReader"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	public static ValueTask<long> ToChannel(this IExecuteReader command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, arrayPool, command.CancellationToken));

	/// <summary>
	/// Iterates the results of an <see cref="IExecuteReader"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete,
		Func<IDataRecord, T> transform)
		=> ToChannelCore(
			command, target, complete,
			reader => reader.ToChannel(target, false, transform, command.CancellationToken));

	/// <summary>
	/// Iterates the results of an <see cref="IExecuteReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it (or with the error that ended the operation), and only once the database reader has been released.</param>
	/// <returns>The number of records read. The task completes once all the records have been read and the reader has been released; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete)
		=> ToChannelMapped(command, target, complete, null);

	/// <summary>
	/// Iterates the results of an <see cref="IExecuteReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed after the last record has been delivered to it (or with the error that ended the operation), and only once the database reader has been released.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The number of records read. The task completes once all the records have been read and the reader has been released; the records are delivered to the target by a second task, which may still be running.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<long> ToChannel<T>(this IExecuteReader command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map)
		=> ToChannelMapped(command, target, complete, map);

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

		if ((command.Connection.State & ConnectionState.Open) == 0)
		{
			await writer
				.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken)
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
			if (complete)
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
	/// Asynchronously iterates a <see cref="DbDataReader"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel(IDataReader, ChannelWriter{object[]}, bool, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> target.WriteAllAsync(
			reader.AsAsyncEnumerable(cancellationToken),
			complete,
			false,
			cancellationToken);

	/// <summary>
	/// Asynchronously iterates a <see cref="DbDataReader"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel(IDataReader, ChannelWriter{object[]}, bool, ArrayPool{object}?, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object>? arrayPool,
		CancellationToken cancellationToken = default)
		=> target.WriteAllAsync(
			reader.AsAsyncEnumerable(arrayPool, cancellationToken),
			complete,
			false,
			cancellationToken);

	/// <summary>
	/// Asynchronously iterates a <see cref="DbDataReader"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDataReader, ChannelWriter{T}, bool, Func{IDataRecord, T}, CancellationToken)"/>
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
	/// Asynchronously iterates a <see cref="DbDataReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDataReader, ChannelWriter{T}, bool, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> Channel.Transformer<T>
			.Create()
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously iterates a <see cref="DbDataReader"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDataReader, ChannelWriter{T}, bool, RowMap{T}?, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> Channel.Transformer<T>
			.Create(map)
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously iterates the results of a <see cref="DbCommand"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel(IDbCommand, ChannelWriter{object[]}, bool, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync(this DbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, cancellationToken));

	/// <summary>
	/// Asynchronously iterates the results of a <see cref="DbCommand"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel(IDbCommand, ChannelWriter{object[]}, bool, ArrayPool{object}, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync(this DbCommand command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
		=> ToChannelAsyncCore(
			command, target, complete, cancellationToken,
			reader => ToChannelAsync(reader, target, false, arrayPool, cancellationToken));

	/// <summary>
	/// Asynchronously iterates the results of a <see cref="DbCommand"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDbCommand, ChannelWriter{T}, bool, Func{IDataRecord, T}, CancellationToken)"/>
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
	/// Asynchronously iterates the results of a <see cref="DbCommand"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDbCommand, ChannelWriter{T}, bool, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync<T>(this DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		=> ToChannelMappedAsync(command, target, complete, null, cancellationToken);

	/// <summary>
	/// Asynchronously iterates the results of a <see cref="DbCommand"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IDbCommand, ChannelWriter{T}, bool, RowMap{T}?, CancellationToken)"/>
	public static ValueTask<long> ToChannelAsync<T>(this DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> ToChannelMappedAsync(command, target, complete, map, cancellationToken);

	/// <summary>
	/// Asynchronously iterates the results of an <see cref="IExecuteReaderAsync"/> and writes each record as an array of values to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel(IExecuteReader, ChannelWriter{object[]}, bool)"/>
	public static ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
		ChannelWriter<object[]> target,
		bool complete)
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, command.CancellationToken)
				: reader.ToChannel(target, false, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates the results of an <see cref="IExecuteReaderAsync"/> and writes each record as an array of values rented from an array pool to the target channel.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The channel writer that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the last record has been written to it and the database reader has been released, or with the error that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from, or <see langword="null"/> to allocate new arrays. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <returns>The number of records written to the target.</returns>
	/// <remarks>
	/// <para>
	/// If the connection is not already open, the operation first waits until the target can accept a write, then opens the connection; reading therefore starts immediately only when the connection is already open. To keep a connection open after the operation, open it before calling this method.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ChannelClosedException">The target has already been completed.</exception>
	public static ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
		ChannelWriter<object[]> target,
		bool complete,
		ArrayPool<object>? arrayPool)
		=> ToChannelAsyncCore(
			command, target, complete,
			reader => command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, false, arrayPool, command.CancellationToken)
				: reader.ToChannel(target, false, arrayPool, command.CancellationToken));

	/// <summary>
	/// Asynchronously iterates the results of an <see cref="IExecuteReaderAsync"/> and writes each record after it has been transformed by <paramref name="transform"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IExecuteReader, ChannelWriter{T}, bool, Func{IDataRecord, T})"/>
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
	/// Asynchronously iterates the results of an <see cref="IExecuteReaderAsync"/> and writes each record mapped to a new instance of <typeparamref name="T"/> to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IExecuteReader, ChannelWriter{T}, bool)"/>
	public static ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete)
		=> ToChannelMappedAsync(command, target, complete, null);

	/// <summary>
	/// Asynchronously iterates the results of an <see cref="IExecuteReaderAsync"/> and writes each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to the target channel.
	/// </summary>
	/// <inheritdoc cref="ToChannel{T}(IExecuteReader, ChannelWriter{T}, bool, RowMap{T}?)"/>
	public static ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map)
		=> ToChannelMappedAsync(command, target, complete, map);

	// A map that the model can't honor is a failure of the operation, not a precondition: with `complete` the target is
	// faulted with it (so that whoever awaits the target isn't left waiting), and the call throws. Nothing has been executed and there
	// is no reader yet, so completing the target here is not completing it before a reader is released.
	// (The preconditions - null arguments, no connection, a closed target, a cancelled token - have been checked before this, and
	// leave the target alone.)
	private static Channel.Transformer<T> CreateMapper<T>(
		RowMap<T>? map,
		ChannelWriter<T> target,
		bool complete)
	{
		Debug.Assert(target is not null);

		try
		{
			return map is null
				? Channel.Transformer<T>.Create()
				: Channel.Transformer<T>.Create(map);
		}
		catch (Exception ex)
		{
			if (complete)
				target.TryComplete(ex);

			throw;
		}
	}

	// The mapped command-level overloads run as a pipeline. The command's reader is read into an internal, bounded
	// buffer (stage 1), and the writer handed to the core is that buffer's writer: the core completes it once the reader
	// has been released, or with the error that ended the read. A second stage, created up front, drains the buffer into
	// the target and completes the target (when `complete`) after the last record has been delivered.
	private static async ValueTask<long> ToChannelMapped<T>(
		IDbCommand command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken)
	{
		// The outer guard of the mapped path. It has to run here, before the pipeline exists (creating it starts its second stage), so it
		// can't be left to the core, which checks the same arguments as the outer guard of the unmapped overloads, and only once it runs.
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if ((command.Connection.State & ConnectionState.Open) == 0)
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

		Channel.Transformer<T> mapper = CreateMapper(map, target, complete);
		Channel.Transformer<T>.Pipeline pipeline = mapper.CreatePipeline(target, complete, cancellationToken);
		return await pipeline
			.RunAsync(() => ToChannelCore(command, pipeline.Buffer, true, pipeline.Token, pipeline.Read))
			.ConfigureAwait(false);
	}

	private static async ValueTask<long> ToChannelMapped<T>(
		IExecuteReader command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map)
	{
		// The outer guard of the mapped path. It has to run here, before the pipeline exists (creating it starts its second stage), so it
		// can't be left to the core, which checks the same arguments as the outer guard of the unmapped overloads, and only once it runs.
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await target.WaitToWriteAndThrowIfClosedAsync(true, command.CancellationToken).ConfigureAwait(false);

		Channel.Transformer<T> mapper = CreateMapper(map, target, complete);
		Channel.Transformer<T>.Pipeline pipeline = mapper.CreatePipeline(target, complete, command.CancellationToken);
		return await pipeline
			.RunAsync(() => ToChannelCore(command, pipeline.Buffer, true, pipeline.Read))
			.ConfigureAwait(false);
	}

	private static async ValueTask<long> ToChannelMappedAsync<T>(
		DbCommand command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken)
	{
		// The outer guard of the mapped path. It has to run here, before the pipeline exists (creating it starts its second stage), so it
		// can't be left to the core, which checks the same arguments as the outer guard of the unmapped overloads, and only once it runs.
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (command.Connection is null) throw new InvalidOperationException("Command has no connection.");
		Contract.EndContractBlock();

		if ((command.Connection.State & ConnectionState.Open) == 0)
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

		Channel.Transformer<T> mapper = CreateMapper(map, target, complete);
		Channel.Transformer<T>.Pipeline pipeline = mapper.CreatePipeline(target, complete, cancellationToken);
		return await pipeline
			.RunAsync(() => ToChannelAsyncCore(command, pipeline.Buffer, true, pipeline.Token, pipeline.ReadAsync))
			.ConfigureAwait(false);
	}

	private static async ValueTask<long> ToChannelMappedAsync<T>(
		IExecuteReaderAsync command,
		ChannelWriter<T> target,
		bool complete,
		RowMap<T>? map)
	{
		// The outer guard of the mapped path. It has to run here, before the pipeline exists (creating it starts its second stage), so it
		// can't be left to the core, which checks the same arguments as the outer guard of the unmapped overloads, and only once it runs.
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await target.WaitToWriteAndThrowIfClosedAsync(true, command.CancellationToken).ConfigureAwait(false);

		Channel.Transformer<T> mapper = CreateMapper(map, target, complete);
		Channel.Transformer<T>.Pipeline pipeline = mapper.CreatePipeline(target, complete, command.CancellationToken);
		return await pipeline
			.RunAsync(() => ToChannelAsyncCore(
				command, pipeline.Buffer, true,
				reader => command.UseAsyncRead && reader is DbDataReader r
					? pipeline.ReadAsync(r)
					: pipeline.Read(reader)))
			.ConfigureAwait(false);
	}
}
