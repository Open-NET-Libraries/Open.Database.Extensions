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

		return RowReader.Stream(map ?? RowMap<T>.Default, reader, 0, -1);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, RowMap{T}?)"/>
	public static IEnumerable<T> Results<T>(
		this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(RowMap<T>.Default, reader, 0, -1);
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

		return RowReader.ToQueue(map ?? RowMap<T>.Default, reader, 0, -1).DequeueEach();
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, RowMap{T}?)" />
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.ToQueue(RowMap<T>.Default, reader, 0, -1).DequeueEach();
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

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>(map));
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, RowMap{T}?)" />
	public static IEnumerable<T> Results<T>(this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>());
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

		return RowReader.StreamAsync(map ?? RowMap<T>.Default, reader, 0, -1, useReadAsync: true, readThrough: false, cancellationToken);
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

		return (await RowReader.ToQueueAsync(map ?? RowMap<T>.Default, reader, 0, -1, useReadAsync, cancellationToken).ConfigureAwait(false)).DequeueEach();
	}

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

		return command.ExecuteReaderAsync(reader => reader.ResultsBufferedAsync<T>(map, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);
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

	const string ResultsFromDataTableObsolete = "Use .ToDataTable() then table.Map(map).ToList() instead, for example: using var table = command.ToDataTable(); var rows = table.Map(map).ToList(). This method cleared the table as the rows were read; Map does not, so dispose or clear the table yourself. This method threw for a type that cannot be mapped before it ran; Map<T> still throws, but after the data is loaded: read RowMap<T>.Default (or build your RowMap) first to keep the early failure. Removed in 12.";

	/// <summary>
	/// Obsolete: use <see cref="DataReaderExtensions.ToDataTable(IDataReader)"/> then table.Map(map).ToList().
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsFromDataTableObsolete)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = reader.ToDataTable();
		return table.To<T>(map, true);
	}

	/// <summary>
	/// Obsolete: use <see cref="CommandExtensions.ToDataTable(IDbCommand, CommandBehavior)"/> then table.Map(map).ToList().
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsFromDataTableObsolete)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = command.ToDataTable();
		return table.To<T>(map, true);
	}
}
