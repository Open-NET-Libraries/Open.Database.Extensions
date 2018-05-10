using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{
		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDataReader reader, IEnumerable<(string Field, string Column)> fieldMappingOverrides)
			where T : new()
		{
			if (!reader.Read()) return Enumerable.Empty<T>();

			var x = new Transformer<T>(fieldMappingOverrides);
			// Ignore missing columns.
			var columns = reader.GetMatchingOrdinals(x.ColumnNames, true);

			return x.AsDequeueingEnumerable(RetrieveInternal(reader,
				columns.Select(c => c.Ordinal).ToArray(),
				columns.Select(c => c.Name).ToArray(), readStarted: true));
		}

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDataReader reader, params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> Results<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> Results<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDbCommand command, IEnumerable<(string Field, string Column)> fieldMappingOverrides)
			where T : new()
			=> command.ExecuteReader(reader => reader.Results<T>(fieldMappingOverrides));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDbCommand command, params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> Results<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDbCommand command, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> Results<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>A task containing the list of results.</returns>
		public static async Task<IEnumerable<T>> ResultsAsync<T>(this DbDataReader reader, IEnumerable<(string Field, string Column)> fieldMappingOverrides, CancellationToken? token = null, bool useReadAsync = true) where T : new()
		{
			var t = token ?? CancellationToken.None;
			if (!await reader.ReadAsync(t)) return Enumerable.Empty<T>(); // else readStarted = true;

			var x = new Transformer<T>(fieldMappingOverrides);
			// Ignore missing columns.
			var columns = reader.GetMatchingOrdinals(x.ColumnNames, true);

			return x.AsDequeueingEnumerable(await RetrieveAsyncInternal(reader, t,
				columns.Select(c => c.Ordinal).ToArray(),
				columns.Select(c => c.Name).ToArray(), readStarted: true, useReadAsync: useReadAsync));
		}

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbDataReader reader, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides, CancellationToken? token = null, bool useReadAsync = true) where T : new()
			=> ResultsAsync<T>(reader, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), token, useReadAsync);

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbDataReader reader, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
			=> ResultsAsync<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="token">A cancellation token.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbDataReader reader, CancellationToken token, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
			=> ResultsAsync<T>(reader, fieldMappingOverrides as IEnumerable<(string Field, string Column)>, token);


		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<(string Field, string Column)> fieldMappingOverrides, CancellationToken? token = null, bool useReadAsync = true) where T : new()
			=> command.ExecuteReaderAsync(reader => reader.ResultsAsync<T>(fieldMappingOverrides, token, useReadAsync: useReadAsync), token: token);

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides, CancellationToken? token = null, bool useReadAsync = true) where T : new()
			=> ResultsAsync<T>(command, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), token);

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
			=> ResultsAsync<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

		/// <summary>
		/// Asynchronously returns all records and iteratively attempts to map the fields to type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="token">A cancellation token.</param>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public static Task<IEnumerable<T>> ResultsAsync<T>(this DbCommand command, CancellationToken token, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
			=> ResultsAsync<T>(command, fieldMappingOverrides as IEnumerable<(string Field, string Column)>, token);



		// NOTE: The Results<T> methods should be faster than the ResultsFromDataTable<T> variations but are provided for validation of this assumption.

		/// <summary>
		/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
			=> reader.ToDataTable().To<T>(fieldMappingOverrides, true);

		/// <summary>
		/// Loads all data into a DataTable before Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
			=> command.ToDataTable().To<T>(fieldMappingOverrides, true);


	}
}
