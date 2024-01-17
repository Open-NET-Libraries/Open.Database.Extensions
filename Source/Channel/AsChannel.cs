namespace Open.Database.Extensions;

[System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2012:Use ValueTasks correctly", Justification = "Intentionally running in the background.")]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'.", Justification = "<Pending>")]
public static partial class ChannelDbExtensions
{
	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IDataReader reader,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDataReader reader,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDataReader reader,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IDbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IDbCommand command,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDbCommand command,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IDbCommand command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IExecuteReader command,
		bool singleReader)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannel(this IExecuteReader command,
		bool singleReader,
		ArrayPool<object> arrayPool)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, arrayPool);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IExecuteReader command,
		bool singleReader,
		Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, transform);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IExecuteReader command,
		bool singleReader)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannel<T>(this IExecuteReader command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, fieldMappingOverrides);
		return channel.Reader;
	}

#if NETSTANDARD2_0
#else
	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to an unbound channel.
	/// Iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this DbDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this DbDataReader reader,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this DbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader and writes each record as an array to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this DbCommand command,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbCommand command,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The command to acquire a reader from to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this DbCommand command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this IExecuteReaderAsync command,
		bool singleReader)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<object[]> AsChannelAsync(this IExecuteReaderAsync command,
		bool singleReader,
		ArrayPool<object> arrayPool)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, arrayPool);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader through the transform function and writes each record to an unbound channel.
	/// Be sure to await the completion.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader,
		Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, transform);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Asynchronously iterates an DbDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record an unbound channel.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IDataReader to iterate.</param>
	/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
	/// <returns>The channel reader containing the results.</returns>
	public static ChannelReader<T> AsChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, fieldMappingOverrides);
		return channel.Reader;
	}
#endif

}
