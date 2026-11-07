namespace Open.Database.Extensions;

/// <summary>
/// Extensions for pipelining data with Dataflow blocks.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2012:Use ValueTasks correctly", Justification = "Intentionally running in the background.")]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'.", Justification = "<Pending>")]
public static partial class DataflowExtensions
{
	static BufferBlock<T> GetBufferBlock<T>(DataflowBlockOptions? options = null)
		=> options == null ? new BufferBlock<T>() : new BufferBlock<T>(options);

	/// <summary>
	/// Reads an <see cref="IDataReader"/> to the end, posting each record as an array of values to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDataReader reader, DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(reader, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads an <see cref="IDataReader"/> to the end, posting each record as an array of values rented from an array pool to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDataReader reader,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(reader, buffer, true, arrayPool);
		return buffer;
	}

	/// <summary>
	/// Reads an <see cref="IDataReader"/> to the end, posting each record after it has been transformed by <paramref name="transform"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(reader, buffer, true, transform);
		return buffer;
	}

	/// <summary>
	/// Reads an <see cref="IDataReader"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(reader, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads an <see cref="IDataReader"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
		RowMap<T>? map,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(reader, buffer, true, map);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/> to the end, posting each record as an array of values to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDbCommand command,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/> to the end, posting each record as an array of values rented from an array pool to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDbCommand command,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(command, buffer, true, arrayPool);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/> to the end, posting each record after it has been transformed by <paramref name="transform"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true, transform);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
		RowMap<T>? map,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true, map);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/> to the end, posting each record as an array of values to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <inheritdoc cref="AsSourceBlockAsync(IExecuteReader, DataflowBlockOptions?)"/>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IExecuteReader command,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/> to the end, posting each record as an array of values rented from an array pool to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <inheritdoc cref="AsSourceBlockAsync(IExecuteReader, ArrayPool{object}, DataflowBlockOptions?)"/>
	public static IReceivableSourceBlock<object[]> AsSourceBlock(this IExecuteReader command,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		ToTargetBlock(command, buffer, true, arrayPool);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/> to the end, posting each record after it has been transformed by <paramref name="transform"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <inheritdoc cref="AsSourceBlockAsync{T}(IExecuteReader, Func{IDataRecord, T}, DataflowBlockOptions?)"/>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true, transform);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <inheritdoc cref="AsSourceBlockAsync{T}(IExecuteReader, DataflowBlockOptions?)"/>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/> to the end, posting each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, to a new <see cref="BufferBlock{T}"/>, and returns the block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// This method is synchronous: it returns only after the source has been read to the end (or the block declined a record), so the calling thread is blocked while the database responds. Exceptions from reading propagate to the caller.
	/// </para>
	/// <para>
	/// Records are posted without waiting for room. With the default unbounded options every record is accepted, but if <paramref name="options"/> sets a bounded capacity, a full block declines the next record, which silently stops the reading early and drops that record. Use the <c>AsSourceBlockAsync</c> overloads for a bounded block, or when the caller should not be blocked.
	/// </para>
	/// <para>
	/// Mapping is done by an internal pipeline, so the last records and the completion of the block can arrive shortly after this method returns.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <inheritdoc cref="AsSourceBlockAsync{T}(IExecuteReader, RowMap{T}?, DataflowBlockOptions?)"/>
	public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
		RowMap<T>? map,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		ToTargetBlock(command, buffer, true, map);
		return buffer;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values as it is read.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDataReader reader,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(reader, buffer, true, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values rented from an array pool as it is read.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDataReader reader,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(reader, buffer, true, arrayPool, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record after it has been transformed by <paramref name="transform"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(reader, buffer, true, transform, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(reader, buffer, true, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading an <see cref="IDataReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <seealso cref="RowMap{T}"/>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
		RowMap<T>? map,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(reader, buffer, true, map, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values as it is read.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDbCommand command,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(command, buffer, true, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values rented from an array pool as it is read.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDbCommand command,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(command, buffer, true, arrayPool, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record after it has been transformed by <paramref name="transform"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true, transform, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IDbCommand"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <param name="cancellationToken">A token that can cancel the reading. The block is then faulted with the resulting exception.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <seealso cref="RowMap{T}"/>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
		RowMap<T>? map,
		DataflowBlockOptions? options = null,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true, map, cancellationToken);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values as it is read.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IExecuteReader command,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record as an array of values rented from an array pool as it is read.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IExecuteReader command,
		ArrayPool<object> arrayPool,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<object[]> buffer = GetBufferBlock<object[]>(options);
		_ = ToTargetBlockAsync(command, buffer, true, arrayPool);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record after it has been transformed by <paramref name="transform"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
		Func<IDataRecord, T> transform,
		DataflowBlockOptions? options = null)
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true, transform);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// </remarks>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true);
		return buffer;
	}

	/// <summary>
	/// Starts reading the results of an <see cref="IExecuteReader"/> in the background and returns a new <see cref="BufferBlock{T}"/> that receives each record mapped to a new instance of <typeparamref name="T"/> by name, or through <paramref name="map"/> when given, as it is read.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="options">Options for the returned block, such as its bounded capacity, or <see langword="null"/> for the defaults.</param>
	/// <returns>The block that receives the records. It is completed after the last record has been delivered, or faulted with the exception that ended the reading.</returns>
	/// <remarks>
	/// <para>
	/// The call returns immediately; reading continues on a background task and sends each record with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput)"/>, which waits for room in a bounded block. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types. Failures are reported only through the returned block's <see cref="IDataflowBlock.Completion"/>.
	/// </para>
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <seealso cref="RowMap{T}"/>
	public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
		RowMap<T>? map,
		DataflowBlockOptions? options = null)
		where T : new()
	{
		BufferBlock<T> buffer = GetBufferBlock<T>(options);
		_ = ToTargetBlockAsync(command, buffer, true, map);
		return buffer;
	}
}
