using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{

		internal static QueryResult<Queue<object[]>> RetrieveInternal(this IDataReader reader,
			IEnumerable<int> ordinals,
			IEnumerable<string>? columnNames = null,
			bool readStarted = false)
			=> new QueryResult<Queue<object[]>>(
				ordinals as IList<int> ?? ordinals.ToArray(),
				columnNames as IList<string> ?? columnNames.ToArray(),
				new Queue<object[]>(reader.AsEnumerableInternal(ordinals, readStarted)));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader)
		{
			var names = reader.GetNames();
			return new QueryResult<Queue<object[]>>(
				Enumerable.Range(0, names.Length), names,
				new Queue<object[]>(reader.AsEnumerable()));
		}

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, IEnumerable<int> ordinals)
			=> RetrieveInternal(reader, ordinals);

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, int n, params int[] others)
			=> Retrieve(reader, Enumerable.Repeat(n, 1).Concat(others));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, IEnumerable<string> columnNames)
		{
			var columns = reader.GetOrdinalMapping(columnNames);
			return RetrieveInternal(reader,
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.Name));
		}

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, string c, params string[] others)
			=> Retrieve(reader, Enumerable.Repeat(c, 1).Concat(others));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
			=> command.ExecuteReader(reader => reader.Retrieve(), behavior);

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, IEnumerable<int> ordinals)
			=> command.ExecuteReader(reader => reader.Retrieve(ordinals));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, int n, params int[] others)
			=> command.ExecuteReader(reader => Retrieve(reader, Enumerable.Repeat(n, 1).Concat(others)));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, IEnumerable<string> columnNames)
			=> command.ExecuteReader(reader => reader.Retrieve(columnNames));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, string c, params string[] others)
			=> command.ExecuteReader(reader => Retrieve(reader, Enumerable.Repeat(c, 1).Concat(others)));

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static async ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var fieldCount = reader.FieldCount;
			var names = reader.GetNames(); // pull before first read.
			var buffer = new Queue<object[]>();

			while (useReadAsync ? await reader.ReadAsync(cancellationToken).ConfigureAwait(true) : (!cancellationToken.IsCancellationRequested && reader.Read()))
			{
				var row = new object[fieldCount];
				reader.GetValues(row);
				buffer.Enqueue(row);
			}

			if (!useReadAsync) cancellationToken.ThrowIfCancellationRequested();

			return new QueryResult<Queue<object[]>>(
				Enumerable.Range(0, names.Length),
				names,
				buffer);
		}

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, CancellationToken cancellationToken)
			=> RetrieveAsync(reader, true, cancellationToken);

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Internal function that requires a cancellation token.")]
		static async ValueTask<QueryResult<Queue<object[]>>> RetrieveAsyncInternal(DbDataReader reader, CancellationToken cancellationToken, IEnumerable<int> ordinals, IEnumerable<string>? columnNames = null, bool readStarted = false, bool useReadAsync = true)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var buffer
				= new Queue<object[]>();

			var result
				= new QueryResult<Queue<object[]>>(
				   ordinals,
				   columnNames ?? ordinals.Select(reader.GetName),
				   buffer);

			var fieldCount = result.ColumnCount;
			var o = result.Ordinals;

			Func<IDataRecord, object[]> handler;
			if (fieldCount == 0) handler = record => Array.Empty<object>();
			else handler = record =>
			{
				var row = new object[fieldCount];
				for (var i = 0; i < fieldCount; i++)
					row[i] = reader.GetValue(o[i]);
				return row;
			};

			if (readStarted)
				buffer.Enqueue(handler(reader));

			while (useReadAsync
				? await reader.ReadAsync(cancellationToken).ConfigureAwait(true)
				: (!cancellationToken.IsCancellationRequested && reader.Read()))
				buffer.Enqueue(handler(reader));

			if (!useReadAsync)
				cancellationToken.ThrowIfCancellationRequested();

			return result;
		}

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, IEnumerable<int> ordinals, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> RetrieveAsyncInternal(reader, cancellationToken, ordinals, useReadAsync: useReadAsync);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, IEnumerable<int> ordinals, CancellationToken cancellationToken)
			=> RetrieveAsyncInternal(reader, cancellationToken, ordinals);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, int n, params int[] others)
			=> RetrieveAsync(reader, Enumerable.Repeat(n, 1).Concat(others));

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Method takes params and cannot have a the cancellation token last.")]
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, CancellationToken cancellationToken, int n, params int[] others)
			=> RetrieveAsync(reader, Enumerable.Repeat(n, 1).Concat(others), cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, IEnumerable<string> columnNames, bool normalizeColumnOrder = false, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			// Validate columns first.
			var columns = reader.GetOrdinalMapping(columnNames, normalizeColumnOrder);
			return RetrieveAsyncInternal(reader, cancellationToken,
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.Name), useReadAsync: useReadAsync);
		}

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, IEnumerable<string> columnNames, bool normalizeColumnOrder, CancellationToken cancellationToken)
			=> RetrieveAsync(reader, columnNames, normalizeColumnOrder, true, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, IEnumerable<string> columnNames, CancellationToken cancellationToken)
			=> RetrieveAsync(reader, columnNames, false, true, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, string c, params string[] others)
			=> RetrieveAsync(reader, Enumerable.Repeat(c, 1).Concat(others));

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Method takes params and cannot have a the cancellation token last.")]
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbDataReader reader, CancellationToken cancellationToken, string c, params string[] others)
			=> RetrieveAsync(reader, Enumerable.Repeat(c, 1).Concat(others), cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> command.ExecuteReaderAsync(reader => RetrieveAsync(reader, useReadAsync, cancellationToken), CommandBehavior.SequentialAccess, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, CancellationToken cancellationToken)
			=> RetrieveAsync(command, true, cancellationToken);

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Internal function that requires a cancellation token.")]
		static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsyncInternal(DbCommand command, CancellationToken token, IEnumerable<int> ordinals, IEnumerable<string>? columnNames = null, bool useReadAsync = true)
			=> command.ExecuteReaderAsync(reader => RetrieveAsyncInternal(reader, token, ordinals, columnNames, useReadAsync: useReadAsync), cancellationToken: token);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, IEnumerable<int> ordinals, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> RetrieveAsyncInternal(command, cancellationToken, ordinals, useReadAsync: useReadAsync);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, IEnumerable<int> ordinals, CancellationToken cancellationToken)
			=> RetrieveAsyncInternal(command, cancellationToken, ordinals);

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, int n, params int[] others)
			=> RetrieveAsync(command, Enumerable.Repeat(n, 1).Concat(others));

		/// <summary>
		/// Asynchronously enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are left unchanged (retained).
		/// </summary>>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="cancellationToken">A cancellation token.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Method takes params and cannot have a the cancellation token last.")]
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, CancellationToken cancellationToken, int n, params int[] others)
			=> RetrieveAsync(command, Enumerable.Repeat(n, 1).Concat(others), cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, IEnumerable<string> columnNames, bool normalizeColumnOrder = false, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> command.ExecuteReaderAsync(reader => RetrieveAsync(reader, columnNames, normalizeColumnOrder, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, IEnumerable<string> columnNames, bool normalizeColumnOrder, CancellationToken cancellationToken)
			=> RetrieveAsync(command, columnNames, normalizeColumnOrder, true, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, IEnumerable<string> columnNames, CancellationToken cancellationToken)
			=> RetrieveAsync(command, columnNames, false, true, cancellationToken);

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, string c, params string[] others)
			=> RetrieveAsync(command, Enumerable.Repeat(c, 1).Concat(others));

		/// <summary>
		/// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Method takes params and cannot have a the cancellation token last.")]
		public static ValueTask<QueryResult<Queue<object[]>>> RetrieveAsync(this DbCommand command, CancellationToken cancellationToken, string c, params string[] others)
			=> RetrieveAsync(command, Enumerable.Repeat(c, 1).Concat(others), false, cancellationToken);

	}
}
