namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> Results<T>(
		this IDataReader reader, params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.Results(reader);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, IEnumerable{KeyValuePair{string, string?}}?)"/>
	public static IEnumerable<T> Results<T>(
		this IDataReader reader)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var x = new Transformer<T>();
		return x.Results(reader);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, IEnumerable{KeyValuePair{string, string?}}?)"/>
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> Results<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> Results<T>(reader, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!reader.Read())
			return Enumerable.Empty<T>();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.ResultsBuffered(reader, true);
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, IEnumerable{KeyValuePair{string, string?}}?)" />
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!reader.Read())
			return Enumerable.Empty<T>();

		var x = new Transformer<T>();
		return x.ResultsBuffered(reader, true);
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, IEnumerable{KeyValuePair{string, string?}}?)" />
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsBuffered<T>(reader, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> Results<T>(this IDbCommand command, params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>(fieldMappingOverrides));
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, IEnumerable{KeyValuePair{string, string?}}?)" />
	public static IEnumerable<T> Results<T>(this IDbCommand command)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>());
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, IEnumerable{KeyValuePair{string, string?}}?)" />
	[OverloadResolutionPriority(-2)]
	public static IEnumerable<T> Results<T>(this IDbCommand command, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> Results<T>(command, fieldMappingOverrides?.Select(kvp => new KeyValuePair<string, string?>(kvp.Field, kvp.Column)));

	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.ResultsAsync(reader, cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		CancellationToken cancellationToken = default)
		where T : new()

		=> ResultsAsync<T>(reader, cancellationToken, fieldMappingOverrides);

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, CancellationToken)" />
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken,
		params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(reader, fieldMappingOverrides, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken,
		params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(reader, cancellationToken, fieldMappingOverrides?.Select(kvp => new KeyValuePair<string, string?>(kvp.Field, kvp.Column)));

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static async ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
			return Enumerable.Empty<T>(); // else readStarted = true;

		var x = new Transformer<T>(fieldMappingOverrides);
		// Ignore missing columns.
		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(x.ColumnNames, true);

		return x.AsDequeueingEnumerable(
			await RetrieveAsyncInternal(
				Transformer<T>.LocalPool,
				reader, cancellationToken,
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.Name),
				readStarted: true,
				useReadAsync: useReadAsync).ConfigureAwait(false),
			Transformer<T>.LocalPool);
	}

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, true, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader, CancellationToken cancellationToken,
		params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, true, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		where T : new()

		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)), useReadAsync, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader, CancellationToken cancellationToken,
		params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null,
		bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> command.ExecuteReaderAsync(reader => reader.ResultsBufferedAsync<T>(fieldMappingOverrides, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken,
		params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken)
		where T : new()
		=> ResultsAsync<T>(command, null, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-2)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null,
		bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)), useReadAsync, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		IEnumerable<(string Field, string? Column)>? fieldMappingOverrides,
		CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, IEnumerable{KeyValuePair{string, string?}}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-2)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken,
		params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, cancellationToken);

	// NOTE: The Results<T> methods should be faster than the ResultsFromDataTable<T> variations but are provided for validation of this assumption.

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null)
		where T : new()
	{
		using var table = reader.ToDataTable();
		return table.To<T>(fieldMappingOverrides, true);
	}

	/// <inheritdoc cref="ResultsFromDataTable{T}(IDataReader, IEnumerable{KeyValuePair{string, string?}}?)" />
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsFromDataTable<T>(reader, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)));

	/// <summary>
	/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides = null)
		where T : new()
	{
		using var table = command.ToDataTable();
		return table.To<T>(fieldMappingOverrides, true);
	}

	/// <inheritdoc cref="ResultsFromDataTable{T}(IDbCommand, IEnumerable{KeyValuePair{string, string?}}?)" />
	[OverloadResolutionPriority(-1)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> ResultsFromDataTable<T>(command, fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)));
}
