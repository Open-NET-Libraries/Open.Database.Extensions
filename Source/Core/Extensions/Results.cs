using Open.Database.Extensions.Core;

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
	public static IEnumerable<T> Results<T>(this IDataReader reader, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
	{
		if (reader is null) throw new System.ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.Results(reader);
	}

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> Results<T>(this IDataReader reader, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> Results<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string? Column)>);

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> Results<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides)
		where T : new()
		=> Results<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
	{
		if (reader is null) throw new System.ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!reader.Read())
			return Enumerable.Empty<T>();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.ResultsBuffered(reader, true);
	}

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> ResultsBuffered<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string? Column)>);

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides)
		where T : new()
		=> ResultsBuffered<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> Results<T>(this IDbCommand command, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new System.ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>(fieldMappingOverrides));
	}

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> Results<T>(this IDbCommand command, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> Results<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string? Column)>);

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IEnumerable<T> Results<T>(this IDbCommand command, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides)
		where T : new()
		=> Results<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

#if NETSTANDARD2_0
#else
	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IAsyncEnumerable<T> ResultsAsync<T>(this DbDataReader reader, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new System.ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var x = new Transformer<T>(fieldMappingOverrides);
		return x.ResultsAsync(reader, cancellationToken);
	}

	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IAsyncEnumerable<T> ResultsAsync<T>(this DbDataReader reader, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string? Column)>);

	/// <summary>
	/// Asynchronously iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IAsyncEnumerable<T> ResultsAsync<T>(this DbDataReader reader, CancellationToken cancellationToken, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> ResultsAsync<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string? Column)>, cancellationToken);

	/// <summary>
	/// Iterates each record and attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	public static IAsyncEnumerable<T> ResultsAsync<T>(this DbDataReader reader, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides, CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), cancellationToken);
#endif

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static async ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
	{
		if (reader is null) throw new System.ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
			return Enumerable.Empty<T>(); // else readStarted = true;

		var x = new Transformer<T>(fieldMappingOverrides);
		// Ignore missing columns.
		var columns = reader.GetMatchingOrdinals(x.ColumnNames, true);

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

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, CancellationToken cancellationToken)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides, bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), useReadAsync, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides, CancellationToken cancellationToken)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
		=> ResultsBufferedAsync<T>(reader, (IEnumerable<(string Field, string? Column)>)fieldMappingOverrides);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="reader">The IDataReader to read results from.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(this DbDataReader reader, CancellationToken cancellationToken, params (string Field, string? Column)[] fieldMappingOverrides)
		where T : new()
		=> ResultsBufferedAsync<T>(reader, fieldMappingOverrides, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> command.ExecuteReaderAsync(reader => ResultsBufferedAsync<T>(reader, fieldMappingOverrides, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, CancellationToken cancellationToken)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides, bool useReadAsync = true, CancellationToken cancellationToken = default)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), useReadAsync, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides, CancellationToken cancellationToken)
		where T : new()
		=> ResultsAsync<T>(command, fieldMappingOverrides, true, cancellationToken);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
		=> ResultsAsync<T>(command, (IEnumerable<(string Field, string? Column)>)fieldMappingOverrides);

	/// <summary>
	/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to generate a reader from.</param>
	/// <param name="cancellationToken">A cancellation token.</param>
	/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <returns>A task containing the list of results.</returns>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, CancellationToken cancellationToken, params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
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
}
