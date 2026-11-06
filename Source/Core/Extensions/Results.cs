namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
		public static IEnumerable<T> Results<T>(
		this IDataReader reader, RowMap<T>? map = null)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream((map ?? RowMap<T>.Default), reader, 0, -1);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, RowMap{T}?)"/>
	public static IEnumerable<T> Results<T>(
		this IDataReader reader)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(RowMap<T>.Default, reader, 0, -1);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, RowMap{T}?)"/>
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> Results<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> Results<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
		public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, RowMap<T>? map = null)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.ToQueue((map ?? RowMap<T>.Default), reader, 0, -1).DequeueEach();
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, RowMap{T}?)" />
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.ToQueue(RowMap<T>.Default, reader, 0, -1).DequeueEach();
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, RowMap{T}?)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsBuffered<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
		public static IEnumerable<T> Results<T>(this IDbCommand command, RowMap<T>? map = null)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>(map));
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, RowMap{T}?)" />
	public static IEnumerable<T> Results<T>(this IDbCommand command)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>());
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, RowMap{T}?)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> Results<T>(this IDbCommand command, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> Results<T>(command, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));

	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
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
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.StreamAsync((map ?? RowMap<T>.Default), reader, 0, -1, useReadAsync: true, cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, RowMap{T}?, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, RowMap{T}?, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken,
		params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
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
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return (await RowReader.ToQueueAsync((map ?? RowMap<T>.Default), reader, 0, -1, useReadAsync, cancellationToken).ConfigureAwait(false)).DequeueEach();
	}

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
		public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		RowMap<T>? map,
		CancellationToken cancellationToken)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), useReadAsync, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader, CancellationToken cancellationToken,
		params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
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
		where T : new()
		=> command.ExecuteReaderAsync(reader => reader.ResultsBufferedAsync<T>(map, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken)
		where T : new()
		=> ResultsAsync<T>(command, null, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-3)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), useReadAsync, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-2)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken,
		params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
		=> ResultsAsync<T>(command, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)), cancellationToken);

	// NOTE: The Results<T> methods should be faster than the ResultsFromDataTable<T> variations but are provided for validation of this assumption.

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, RowMap<T>? map = null)
		where T : new()
	{
		using var table = reader.ToDataTable();
		return table.To<T>(map, true);
	}

	/// <inheritdoc cref="ResultsFromDataTable{T}(IDataReader, RowMap{T}?)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsFromDataTable<T>(reader, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, RowMap<T>? map = null)
		where T : new()
	{
		using var table = command.ToDataTable();
		return table.To<T>(map, true);
	}

	/// <inheritdoc cref="ResultsFromDataTable{T}(IDbCommand, RowMap{T}?)" />
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <exception cref="ArgumentException">The tuples are not a valid map: an unknown or repeated property, an empty column name, or two properties that read the same column.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsFromDataTable<T>(command, RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));
}
