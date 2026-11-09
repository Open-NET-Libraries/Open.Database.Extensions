namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> Results<T>(
	this IDataReader reader, RowMap<T>? map = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(map ?? RowMap<T>.Default, reader, 0, -1, default(SharedReader));
	}

	/// <inheritdoc cref="Results{T}(IDataReader, RowMap{T}?)"/>
	public static IEnumerable<T> Results<T>(
		this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(RowMap<T>.Default, reader, 0, -1, default(SharedReader));
	}

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, RowMap<T>? map = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.ToQueue(map ?? RowMap<T>.Default, reader, 0, -1, default(SharedReader)).DequeueEach();
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, RowMap{T}?)" />
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.ToQueue(RowMap<T>.Default, reader, 0, -1, default(SharedReader)).DequeueEach();
	}

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> Results<T>(this IDbCommand command, RowMap<T>? map = null)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(reader => RowReader.ToQueue(map ?? RowMap<T>.Default, reader, 0, -1, default(OwnedReader)).DequeueEach());
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, RowMap{T}?)" />
	public static IEnumerable<T> Results<T>(this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(reader => RowReader.ToQueue(RowMap<T>.Default, reader, 0, -1, default(OwnedReader)).DequeueEach());
	}

	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		RowMap<T>? map = null,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.StreamAsync(map ?? RowMap<T>.Default, reader, 0, -1, useReadAsync: true, default(SharedReader), cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, RowMap{T}?, CancellationToken)" />
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken)
		=> ResultsAsync<T>(reader, null, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		RowMap<T>? map = null,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return await ResultsBufferedAsyncCore(reader, map, useReadAsync, default(SharedReader), cancellationToken).ConfigureAwait(false);
	}

	// Shared by the reader-level helper (the caller's reader: SharedReader) and the command-level terminals (their own reader: OwnedReader).
	internal static async ValueTask<IEnumerable<T>> ResultsBufferedAsyncCore<T, TEnd>(DbDataReader reader, RowMap<T>? map, bool useReadAsync, TEnd end, CancellationToken cancellationToken)
		where TEnd : struct, IRowsEnd
		=> (await RowReader.ToQueueAsync(map ?? RowMap<T>.Default, reader, 0, -1, useReadAsync, end, cancellationToken).ConfigureAwait(false)).DequeueEach();

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
	this DbDataReader reader,
	RowMap<T>? map,
	CancellationToken cancellationToken)
	=> ResultsBufferedAsync<T>(reader, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken)
		=> ResultsBufferedAsync<T>(reader, null, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		RowMap<T>? map = null,
		bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReaderAsync(reader => ResultsBufferedAsyncCore(reader, map, useReadAsync, default(OwnedReader), cancellationToken), CommandBehavior.Default, cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> ResultsAsync<T>(command, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken)
		=> ResultsAsync<T>(command, null, true, cancellationToken);

	// NOTE: The Results<T> methods should be faster than the ResultsFromDataTable<T> variations but are provided for validation of this assumption.

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = reader.ToDataTable();
		return table.To<T>(map, true);
	}

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = command.ToDataTable();
		return table.To<T>(map, true);
	}
}
