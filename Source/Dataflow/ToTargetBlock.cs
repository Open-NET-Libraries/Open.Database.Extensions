namespace Open.Database.Extensions;

/// <summary>
/// Extensions for pipelining data with Dataflow blocks.
/// </summary>
public static partial class DataflowExtensions
{
	/// <summary>
	/// Reads an <see cref="IDataReader"/>, posting each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads an <see cref="IDataReader"/>, posting each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads an <see cref="IDataReader"/>, posting each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads an <see cref="IDataReader"/>, posting each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	public static long ToTargetBlock<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete)
		where T : new()
		=> Dataflow.Transformer<T>
			.Create()
			.PipeResultsTo(reader, target, complete);

	/// <summary>
	/// Reads an <see cref="IDataReader"/>, posting each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static long ToTargetBlock<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map)
		where T : new()
		=> Dataflow.Transformer<T>
			.Create(map)
			.PipeResultsTo(reader, target, complete);

	/// <summary>
	/// Reads the results of an <see cref="IDbCommand"/>, posting each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IDbCommand"/>, posting each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IDbCommand"/>, posting each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IDbCommand"/>, posting each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IDbCommand"/>, posting each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static long ToTargetBlock<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, map);
	}

	/// <summary>
	/// Reads the results of an <see cref="IExecuteReader"/>, posting each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IExecuteReader"/>, posting each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IExecuteReader"/>, posting each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are posted with <see cref="DataflowBlock.Post{TInput}(ITargetBlock{TInput}, TInput)"/>, which does not wait: a target that declines a record, because it is complete or because it is bounded and currently full, stops the reading. The declined record has already been read and is not delivered. Use the <c>Async</c> overload to feed a bounded target, as it waits for room.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IExecuteReader"/>, posting each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
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
	/// Reads the results of an <see cref="IExecuteReader"/>, posting each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading blocks the calling thread. Reading stops, without an error, as soon as the internal pipeline declines a record.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static long ToTargetBlock<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		return ToTargetBlockMapped(read => command.ExecuteReader(read), target, complete, map);
	}

	/// <summary>
	/// Asynchronously reads an <see cref="IDataReader"/>, sending each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads an <see cref="IDataReader"/>, sending each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads an <see cref="IDataReader"/>, sending each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads an <see cref="IDataReader"/>, sending each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	public static ValueTask<long> ToTargetBlockAsync<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Dataflow.Transformer<T>
			.Create()
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously reads an <see cref="IDataReader"/>, sending each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="reader">The data reader to iterate. The caller owns it: it is not closed or disposed here, and it must stay open until the operation completes.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<long> ToTargetBlockAsync<T>(this IDataReader reader,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		where T : new()
		=> Dataflow.Transformer<T>
			.Create(map)
			.PipeResultsToAsync(reader, target, complete, cancellationToken);

	/// <summary>
	/// Asynchronously reads the results of an <see cref="IDbCommand"/>, sending each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads the results of an <see cref="IDbCommand"/>, sending each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads the results of an <see cref="IDbCommand"/>, sending each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads the results of an <see cref="IDbCommand"/>, sending each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
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
	/// Asynchronously reads the results of an <see cref="IDbCommand"/>, sending each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="cancellationToken">A token that can cancel the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <exception cref="OperationCanceledException">The operation was canceled.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<long> ToTargetBlockAsync<T>(this IDbCommand command,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map,
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
			target, complete, map, cancellationToken).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously reads the results of an <see cref="IExecuteReader"/>, sending each record as an array of values to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously reads the results of an <see cref="IExecuteReader"/>, sending each record as an array of values rented from an array pool to the target block, and stops when the target declines a record.
	/// </summary>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="arrayPool">The array pool to rent each record's array from. The consumer of the records is responsible for returning each array to the pool.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously reads the results of an <see cref="IExecuteReader"/>, sending each record after it has been transformed by <paramref name="transform"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type of the items delivered to the block: the return type of the transform function.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed when reading ends and the reader has been released (including when reading stops because the target declined a record), or faulted with the exception that ended the operation.</param>
	/// <param name="transform">The function that transforms each <see cref="IDataRecord"/> into the item delivered to the block.</param>
	/// <returns>The number of records accepted by the target.</returns>
	/// <remarks>
	/// <para>
	/// Records are sent with <see cref="DataflowBlock.SendAsync{TInput}(ITargetBlock{TInput}, TInput, CancellationToken)"/>, which waits for room in a bounded target. A target that declines a record, for example because it is complete, stops the reading; the declined record has already been read and is not delivered. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously reads the results of an <see cref="IExecuteReader"/>, sending each record mapped to a new instance of <typeparamref name="T"/> to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
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
	/// Asynchronously reads the results of an <see cref="IExecuteReader"/>, sending each record mapped to a new instance of <typeparamref name="T"/>, using the field-to-column overrides to the target block, and stops when the target declines a record.
	/// </summary>
	/// <typeparam name="T">The type each record is mapped to.</typeparam>
	/// <param name="command">The expressive command to execute. The connection follows the command's ownership rules: it is opened only if needed and released when reading finishes. The command's own cancellation token applies.</param>
	/// <param name="target">The block that receives the records.</param>
	/// <param name="complete">If <see langword="true"/>, the target is completed once the reader has been released and every record has been delivered to it (which can be after this method has returned), or faulted with the exception that ended the operation.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The number of records read and handed to the internal buffer. Delivery to the target continues in the background, so some of them may not have reached it yet when this returns.</returns>
	/// <remarks>
	/// <para>
	/// Reading stops, without an error, as soon as the internal pipeline declines a record. Asynchronous database calls are used when the reader or command derives from the <c>Db*</c> base types.
	/// </para>
	/// <para>
	/// If a connection should stay open after the operation, open it before calling this method; a connection that this method opens is closed again when reading ends.
	/// </para>
	/// <para>
	/// Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="target"/> is <see langword="null"/>. The exception is reported through the returned task.</exception>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<long> ToTargetBlockAsync<T>(this IExecuteReader command,
		ITargetBlock<T> target,
		bool complete,
		RowMap<T>? map)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		await Task.Yield();
		return await ToTargetBlockMappedAsync(
			read => command.ExecuteReaderAsync(read),
			target, complete, map, command.CancellationToken).ConfigureAwait(false);
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
		RowMap<T>? map)
		where T : new()
	{
		// The public overloads that call this have already validated their arguments (once); nothing here guards again.
		System.Diagnostics.Debug.Assert(executeReader is not null);
		System.Diagnostics.Debug.Assert(target is not null);

		var pipeline = new Dataflow.Transformer<T>.Pipeline(target, complete);
		long total;
		try
		{
			total = executeReader(reader => pipeline.Write(reader, Dataflow.Transformer<T>.Create(map)));
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
		RowMap<T>? map,
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
			total = await executeReader(reader => pipeline.WriteAsync(reader, Dataflow.Transformer<T>.Create(map), cancellationToken)).ConfigureAwait(false);
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
