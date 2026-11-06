namespace Open.Database.Extensions;

[SuppressMessage("Reliability", "CA2012:Use ValueTasks correctly", Justification = "Intentionally running in the background.")]
[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'.", Justification = "<Pending>")]
public static partial class ChannelDbExtensions
{
	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from, or <see langword="null"/> to allocate new arrays. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IDataReader reader,
		bool singleReader,
		ArrayPool<object>? arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IDataReader reader,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannel<T>(this IDataReader reader,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(reader, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannel<T>(this IDataReader reader,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannel<T>(reader, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)), cancellationToken);

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IDbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IDbCommand command,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IDbCommand command,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IDbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannel<T>(this IDbCommand command,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannel<T>(this IDbCommand command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannel<T>(command, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)), cancellationToken);

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IExecuteReader command,
		bool singleReader)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannel(this IExecuteReader command,
		bool singleReader,
		ArrayPool<object> arrayPool)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, arrayPool);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IExecuteReader command,
		bool singleReader,
		Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, transform);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannel<T>(this IExecuteReader command,
		bool singleReader)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannel<T>(this IExecuteReader command,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannel(command, channel.Writer, true, fieldMappingOverrides);
		return channel.Reader;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannel<T>(this IExecuteReader command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
		=> ToChannel<T>(command, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)));

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this DbDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this DbDataReader reader,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(reader, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading a <see cref="DbDataReader"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannelAsync<T>(this DbDataReader reader,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelAsync<T>(reader, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)), cancellationToken);

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this DbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this DbCommand command,
		bool singleReader,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, arrayPool, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this DbCommand command,
		bool singleReader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, transform, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this DbCommand command,
		bool singleReader,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannelAsync<T>(this DbCommand command,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, fieldMappingOverrides, cancellationToken);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of a <see cref="DbCommand"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The channel is then completed with the resulting exception.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannelAsync<T>(this DbCommand command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ToChannelAsync<T>(command, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)), cancellationToken);

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this IExecuteReaderAsync command,
		bool singleReader)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record as an array of values rented from an array pool.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from, or <see langword="null"/> to allocate new arrays. The consumer of the channel is responsible for returning each array to the pool.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<object[]> ToChannelAsync(this IExecuteReaderAsync command,
		bool singleReader,
		ArrayPool<object>? arrayPool)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<object[]> channel = CreateChannel<object[]>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, arrayPool);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record after it has been transformed by <paramref name="transform"/>.
	/// </summary>
	/// <typeparam name="T">The type of the items written to the channel: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item written to the channel.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader,
		Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, transform);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static ChannelReader<T> ToChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowPlan{T}"/> declared once for a read that runs more than occasionally. A <see cref="RowPlan{T}"/> flows through this <c>IEnumerable&lt;KeyValuePair&lt;string, string?&gt;&gt;</c> parameter, which is why the pair overloads take it and no public method takes a plan.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	public static ChannelReader<T> ToChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		Channel<T> channel = CreateChannel<T>(-1, singleReader);
		_ = ToChannelAsync(command, channel.Writer, true, fieldMappingOverrides);
		return channel.Reader;
	}

	/// <summary>
	/// Starts asynchronously reading the results of an <see cref="IExecuteReaderAsync"/> in the background and returns the reader of a new unbounded channel that receives each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="singleReader"><see langword="true"/> if at most one consumer reads from the returned channel at a time, which lets the channel optimize for that; otherwise <see langword="false"/>.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names, applied on top of the default name-based mapping.</param>
	/// <returns>The reader of the channel that receives the records. Its <see cref="ChannelReader{T}.Completion"/> completes after the last record has been written, or faults with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately without waiting for rows: reading begins at once and continues on a background task, and its failures are reported only through the returned reader's <see cref="ChannelReader{T}.Completion"/>. Await it, or read the channel until it is completed, to know when the work has finished.
	/// </para>
	/// <para>
	/// The channel is unbounded, so a consumer that is slower than the database lets records accumulate in memory. Use the overloads that take a <see cref="ChannelWriter{T}"/> target, for example one of a bounded channel, when back-pressure is needed.
	/// </para>
	/// <para>
	/// Convenience overload: the tuples are converted to key/value pairs; prefer a <see cref="RowPlan{T}"/> or key/value pairs.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowPlan{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ChannelReader<T> ToChannelAsync<T>(this IExecuteReaderAsync command,
		bool singleReader,
		IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
		where T : new()
		=> ToChannelAsync<T>(command, singleReader, fieldMappingOverrides?.Select(static m => new KeyValuePair<string, string?>(m.Field, m.Column)));
}
