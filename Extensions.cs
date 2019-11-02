using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	/// <summary>
	/// Core non-DB-specific extensions for building a command and retrieving data using best practices.
	/// </summary>
	public static partial class Extensions
	{
		// https://stackoverflow.com/questions/17660097/is-it-possible-to-speed-this-method-up/17669142#17669142
		internal static Action<T, object?> BuildUntypedSetter<T>(this PropertyInfo propertyInfo)
		{
			var targetType = propertyInfo.DeclaringType;
			var exTarget = Expression.Parameter(targetType ?? throw new InvalidOperationException(), "t");
			var exValue = Expression.Parameter(typeof(object), "p");
			var methodInfo = propertyInfo.GetSetMethod();
			var exBody = Expression.Call(exTarget, methodInfo,
			   Expression.Convert(exValue, propertyInfo.PropertyType));
			var lambda = Expression.Lambda<Action<T, object?>>(exBody, exTarget, exValue);
			var action = lambda.Compile();
			return action;
		}

		internal static object? DBNullValueToNull(object? value)
			=> value == DBNull.Value ? null : value;

		/// <summary>
		/// Any DBNull values are converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>The converted enumerable.</returns>
		public static IEnumerable<object?> DBNullToNull(this IEnumerable<object?> values)
		{
			foreach (var v in values)
				yield return DBNullValueToNull(v);
		}

		/// <summary>
		/// Returns a copy of this array with any DBNull values converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>A new array containing the results with.</returns>
		public static object?[] DBNullToNull(this object?[] values)
		{
			var len = values.Length;
			var result = new object?[len];
			for (var i = 0; i < len; i++)
			{
				result[i] = DBNullValueToNull(values[i]);
			}
			return result;
		}

		/// <summary>
		/// Replaces any DBNull values in the array with null;
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>The converted enumerable.</returns>
		public static object?[] ReplaceDBNullWithNull(this object?[] values)
		{
			var len = values.Length;
			for (var i = 0; i < len; i++)
			{
				var value = values[i];
				if (value == DBNull.Value) values[i] = null;
			}
			return values;
		}

		/// <summary>
		/// If the connection isn't open, opens the connection.
		/// If the connection is in neither open or close, first closes the connection.
		/// </summary>
		/// <returns>The prior connection state.</returns>
		public static ConnectionState EnsureOpen(this IDbConnection connection)
		{
			var state = connection.State;

			if (state.HasFlag(ConnectionState.Broken))
				connection.Close();

			if (!connection.State.HasFlag(ConnectionState.Open))
				connection.Open();

			return state;
		}

		/// <summary>
		/// If the connection isn't open, opens the connection.
		/// If the connection is in neither open or close, first closes the connection.
		/// </summary>
		/// <param name="connection">The connection to transact with.</param>
		/// <param name="token">An optional token to cancel opening.</param>
		/// <param name="configureAwait">If true (default) will retain the context after opening.</param>
		/// <returns>A task containing the prior connection state.</returns>
		public static async ValueTask<ConnectionState> EnsureOpenAsync(this DbConnection connection, CancellationToken token = default, bool configureAwait = true)
		{
			token.ThrowIfCancellationRequested();

			var state = connection.State;
			if (state.HasFlag(ConnectionState.Broken))
				connection.Close();

			if (!connection.State.HasFlag(ConnectionState.Open))
			{
				var o = connection.OpenAsync(token);
				if (configureAwait) await o;
				else await o.ConfigureAwait(false);

				if (token.IsCancellationRequested && !state.HasFlag(ConnectionState.Closed))
					connection.Close(); // Fake finally...

				token.ThrowIfCancellationRequested();
			}

			return state;
		}

		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameter(this IDbCommand target,
			string name, object? value = null)
		{
			if (name == null)
				throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			var c = target.CreateParameter();
			c.ParameterName = name;
			if (value != null) // DBNull.Value is allowed.
				c.Value = value;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameter(this IDbCommand target,
			string name, object value, DbType type, ParameterDirection direction = ParameterDirection.Input)
		{
			var p = target.AddParameterType(name, type);
			p.Value = value;
			p.Direction = direction;
			return p;
		}

		/// <summary>
		/// Shortcut for adding command a typed (non-input) parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameterType(this IDbCommand target,
			string? name, DbType type, ParameterDirection direction = ParameterDirection.Input)
		{
			if (direction != ParameterDirection.ReturnValue && name == null)
				throw new ArgumentNullException(nameof(name), "Parameter names can only be null for a return parameter.");
			else if (name != null && string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			var c = target.CreateParameter();
			if (name != null) c.ParameterName = name;
			c.DbType = type;
			c.Direction = direction;
			target.Parameters.Add(c);
			return c;
		}


		/// <summary>
		/// Shortcut for adding command a typed return parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddReturnParameter(this IDbCommand target,
			DbType type, string? name = null)
			=> target.AddParameterType(name, type, ParameterDirection.ReturnValue);

		/// <summary>
		/// Shortcut for adding command a return parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddReturnParameter(this IDbCommand target,
			string? name = null)
		{
			var c = target.CreateParameter();
			if (!string.IsNullOrWhiteSpace(name)) c.ParameterName = name;
			c.Direction = ParameterDirection.ReturnValue;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void ForEach(this IDataReader reader, Action<IDataRecord> handler)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			while (reader.Read())
				handler(reader);
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask ForEachAsync(this DbDataReader reader,
			Action<IDataRecord> handler, CancellationToken token = default, bool useReadAsync = true)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(token))
					handler(reader);
			}
			else if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read())
					handler(reader);

				token.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read())
					handler(reader);
			}
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		public static async ValueTask ForEachAsync(this DbDataReader reader, Func<IDataRecord, ValueTask> handler, CancellationToken token = default)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			while (await reader.ReadAsync(token))
				await handler(reader);
		}

		/// <summary>
		/// Returns all the column names for the current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>The enumerable of column names.</returns>
		public static IEnumerable<string> ColumnNames(this IDataRecord record)
		{
			var fieldCount = record.FieldCount;
			for (var i = 0; i < fieldCount; i++)
				yield return record.GetName(i);
		}

		/// <summary>
		/// Returns all the column names for the current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>The array of column names.</returns>
		public static string[] GetNames(this IDataRecord record)
		{
			var fieldCount = record.FieldCount;
			var columnNames = new string[fieldCount];
			for (var i = 0; i < fieldCount; i++)
				columnNames[i] = record.GetName(i);
			return columnNames;
		}

		/// <summary>
		/// Returns the (name,ordinal) mapping for current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>An enumerable of the mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record)
			=> record.GetNames().Select((n, o) => (Name: n, Ordinal: o));

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <returns>An enumerable of the mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames)
		{
			if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			return columnNames.Select(n => (Name: n, Ordinal: record.GetOrdinal(n))); // Does do a case-insensitive search after a case-sensitive one.
		}

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns>An enumerable of the mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort)
		{
			if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			var q = OrdinalMapping(record, columnNames);
			return sort
				? q.OrderBy(m => m.Ordinal)
				: q;
		}

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns></returns>
		public static (string Name, int Ordinal)[] GetOrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		{
			if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			if (columnNames is ICollection<string> cn && cn.Count == 0)
				return Array.Empty<(string Name, int Ordinal)>();

			try
			{
				return record
					.OrdinalMapping(columnNames, sort)
					.ToArray();
			}
			catch (IndexOutOfRangeException ex)
			{
				var mismatch = new HashSet<string>(columnNames);
				mismatch.ExceptWith(record.GetNames());

				// Columns not mapped correctly.  Report all columns that are mismatched/missing.
				throw new IndexOutOfRangeException($"Invalid columns: {string.Join(", ", mismatch.OrderBy(c => c).ToArray())}", ex);
			}
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <returns>An enumerable for iterating the values of a record.</returns>
		public static IEnumerable<object> EnumerateValues(this IDataRecord record)
		{
			var count = record.FieldCount;
			for (var i = 0; i < count; i++)
				yield return record.GetValue(i);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
		public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IEnumerable<int> ordinals)
		{
			if (ordinals == null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			var o = ordinals as IList<int> ?? ordinals.ToArray();
			var count = o.Count;
			for (var i = 0; i < count; i++)
				yield return record.GetValue(o[i]);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="firstOrdinal">The first ordinal to query.</param>
		/// <param name="remainingOrdinals">The remaining set of ordinals to query.</param>
		/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
		public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, int firstOrdinal, params int[] remainingOrdinals)
			=> EnumerateValuesFromOrdinals(record, Enumerable.Repeat(firstOrdinal, 1).Concat(remainingOrdinals));


		static object[] GetValuesFromOrdinalsInternal(this IDataRecord record, IList<int> ordinals)
		{
			var count = ordinals.Count;
			if (count == 0) return Array.Empty<object>();

			var values = new object[count];
			for (var i = 0; i < count; i++)
				values[i] = record.GetValue(ordinals[i]);

			return values;
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An array of values matching the ordinal positions requested.</returns>
		public static object[] GetValuesFromOrdinals(this IDataRecord record, IEnumerable<int> ordinals)
			=> GetValuesFromOrdinalsInternal(record, ordinals as IList<int> ?? ordinals.ToArray());

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns></returns>
		public static IEnumerable<(string Name, int Ordinal)> MatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		{
			if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			// Normalize the requested column names to be lowercase.
			columnNames = columnNames.Select(c =>
			{
				if (string.IsNullOrWhiteSpace(c))
					throw new ArgumentException("Column names cannot be null or whitespace only.");
				return c.ToLowerInvariant();
			});

			var actual = record.OrdinalMapping();
			if (sort)
			{
				var requested = new HashSet<string>(columnNames);
				// Return actual values based upon if their lower-case counterparts exist in the requested.
				return actual
					.Where(m => requested.Contains(m.Name.ToLowerInvariant()));
			}
			else
			{
				// Create a map of lower-case keys to actual.
				var actualColumns = actual.ToDictionary(m => m.Name.ToLowerInvariant(), m => m);
				return columnNames
					.Where(c => actualColumns.ContainsKey(c)) // Select lower case column names if they exist in the dictionary.
					.Select(c => actualColumns[c]); // Then select the actual values based upon that key.
			}
		}

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns></returns>
		public static (string Name, int Ordinal)[] GetMatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		{
			return MatchingOrdinals(record, columnNames, sort).ToArray();

			/* Note:
             * It is not necessary to call .ToArray() because the names are already pulled from the reader,
             * and this method could return IEnumerable,
             * but it's more common use is to have an array so best to keep it an array.
             */
		}

		/// <summary>
		/// Returns all the data type names for the columns of current result set.
		/// </summary>
		/// <param name="reader">The reader to get data type names from.</param>
		/// <returns>The enumerable of data type names.</returns>
		public static IEnumerable<string> DataTypeNames(this IDataRecord reader)
		{
			var fieldCount = reader.FieldCount;
			for (var i = 0; i < fieldCount; i++)
				yield return reader.GetDataTypeName(i);
		}


		/// <summary>
		/// Returns all the data type names for the columns of current result set.
		/// </summary>
		/// <param name="reader">The reader to get data type names from.</param>
		/// <returns>The array of data type names.</returns>
		public static string[] GetDataTypeNames(this IDataRecord reader)
		{
			var fieldCount = reader.FieldCount;
			var results = new string[fieldCount];
			for (var i = 0; i < fieldCount; i++)
				results[i] = reader.GetDataTypeName(i);
			return results;
		}

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader)
		{
			if (reader.Read())
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = new object[fieldCount];
					reader.GetValues(row);
					yield return row;
				} while (reader.Read());
			}
		}

		static IEnumerable<object[]> AsEnumerableInternal(this IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
		{
			if (ordinals == null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			if (readStarted || reader.Read())
			{
				var o = ordinals as IList<int> ?? ordinals.ToArray();
				var fieldCount = o.Count;
				if (fieldCount == 0)
				{
					do
					{
						yield return Array.Empty<object>();
					}
					while (reader.Read());
				}
				else
				{
					do
					{
						var row = new object[fieldCount];
						for (var i = 0; i < fieldCount; i++)
							row[i] = reader.GetValue(o[i]);
						yield return row;
					}
					while (reader.Read());
				}

			}
		}

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals)
			=> AsEnumerableInternal(reader, ordinals, false);

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params int[] others)
			=> AsEnumerable(reader, Enumerable.Repeat(n, 1).Concat(others));

		/// <summary>
		/// Generic enumerable extension for DataColumnCollection.
		/// </summary>
		/// <param name="columns">The column collection.</param>
		/// <returns>An enumerable of DataColumns.</returns>
		public static IEnumerable<DataColumn> AsEnumerable(this DataColumnCollection columns)
		{
			foreach (DataColumn c in columns)
				yield return c;
		}

		/// <summary>
		/// Generic enumerable extension for DataRowCollection.
		/// </summary>
		/// <param name="rows">The row collection.</param>
		/// <returns>An enumerable of DataRows.</returns>
		public static IEnumerable<DataRow> AsEnumerable(this DataRowCollection rows)
		{
			foreach (DataRow r in rows)
				yield return r;
		}

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<(string Field, string Column)>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.Results(table, clearSourceTable);
		}

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.Results(table, false);
		}

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
			=> table.To<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), clearSourceTable);

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> Iterate<T>(this IDataReader reader, Func<IDataRecord, T> transform, CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read())
					yield return transform(reader);
			}
			{
				while (reader.Read())
					yield return transform(reader);
			}
		}

		/// <summary>
		/// Shortcut for .Iterate(transform).ToList();
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>A list of the transformed results.</returns>
		public static List<T> ToList<T>(this IDataReader reader,
			Func<IDataRecord, T> transform, CancellationToken token = default)
			=> reader.Iterate(transform, token).ToList();

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
			Func<IDataRecord, T> transform, CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var list = new List<T>();
			while (await reader.ReadAsync(token)) list.Add(transform(reader));
			return list;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
			Func<IDataRecord, ValueTask<T>> transform, CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var list = new List<T>();
			while (await reader.ReadAsync(token)) list.Add(await transform(reader));
			return list;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			Func<IDataRecord, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default, CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			return await reader.ToListAsync(transform, token).ConfigureAwait(false);

		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default, CancellationToken token = default, bool useReadAsync = true)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;

			using var reader = await command.ExecuteReaderAsync(behavior, token);
			if (useReadAsync)
			{
				return await reader.ToListAsync(transform, token).ConfigureAwait(false);
			}
			else
			{
				var r = reader.ToList(transform, token);
				token.ThrowIfCancellationRequested();
				return r;
			}
		}

		/// <summary>
		/// Shortcut for .Iterate(transform).ToArray();
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>An array of the transformed results.</returns>
		public static T[] ToArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
			=> reader.Iterate(transform).ToArray();

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ToList(command, behavior, transform);

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Iterate(transform).ToList();
		}

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ToArray(command, behavior, transform);

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Iterate(transform).ToArray();
		}

		/// <summary>
		/// Loads all remaining data from an IDataReader into a DataTable.
		/// </summary>
		/// <param name="reader">The IDataReader to load data from.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDataReader reader)
		{
			var table = new DataTable();
			table.Load(reader);
			return table;
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTables.
		/// Calls .NextResult() to check for more results.
		/// </summary>
		/// <param name="reader">The IDataReader to load data from.</param>
		/// <returns>The resultant list of DataTables.</returns>
		public static List<DataTable> ToDataTables(this IDataReader reader)
		{
			var results = new List<DataTable>();
			do
			{
				results.Add(reader.ToDataTable());
			}
			while (reader.NextResult());
			return results;
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTable.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.ToDataTable();
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTables.
		/// Calls .NextResult() to check for more results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The resultant list of DataTables.</returns>
		public static List<DataTable> ToDataTables(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.ToDataTables();
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			while (reader.Read() && predicate(reader)) { }
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		/// <param name="token">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, bool useReadAsync = false, CancellationToken token = default)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();
			if (useReadAsync)
			{
				while (await reader.ReadAsync(token) && await predicate(reader)) { }
			}
			else
			{
				while (reader.Read() && await predicate(reader)) { }
			}
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			handler(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return transform(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		public static async ValueTask ExecuteReaderAsync(this DbCommand command,
			Action<DbDataReader> handler,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken token = default)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			handler(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static async ValueTask ExecuteReaderAsync(this IDbCommand command,
			Func<IDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (command is DbCommand c)
			{
				await c.ExecuteReaderAsync(reader => handler(reader), behavior);
				return;
			}

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			await handler(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		public static async ValueTask ExecuteReaderAsync(this DbCommand command,
			Func<DbDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken token = default)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			await handler(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
			Func<DbDataReader, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			return transform(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this IDbCommand command,
			Func<IDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (command is DbCommand c)
				return await c.ExecuteReaderAsync(reader => transform(reader), behavior);

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await transform(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
			Func<DbDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default, CancellationToken token = default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			return await transform(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector,
			CommandBehavior behavior = CommandBehavior.Default)
			=> IterateReader(command, behavior, transform, selector);

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			if (selector == null) throw new ArgumentNullException(nameof(selector));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return selector(reader.Iterate(transform));
		}

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
			=> IterateReader(command, behavior, handler);

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void IterateReader(this IDbCommand command, CommandBehavior behavior, Action<IDataRecord> handler)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			reader.ForEach(handler);
		}

		internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			if (command == null) throw new ArgumentNullException(nameof(command));
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (reader.Read())
				yield return transform(reader);
		}

		internal static IEnumerable<object[]> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			if (command == null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			if (reader.Read())
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = new object[fieldCount];
					reader.GetValues(row);
					yield return row;
				} while (reader.Read());
			}
		}

		/// <summary>
		/// Iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void IterateReaderWhile(this IDbCommand command, Func<IDataRecord, bool> predicate, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			reader.IterateWhile(predicate);
		}


		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask ForEachAsync(this DbCommand command,
			Action<IDataRecord> handler,
			CommandBehavior behavior,
			CancellationToken token = default,
			bool useReadAsync = true)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			if (useReadAsync)
			{
				while (await reader.ReadAsync(token))
					handler(reader);
			}
			else if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read())
					handler(reader);

				token.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read())
					handler(reader);
			}
		}

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		public static ValueTask ForEachAsync(this DbCommand command,
			Action<IDataRecord> handler,
			CancellationToken token = default,
			bool useReadAsync = true)
			=> ForEachAsync(command, handler, CommandBehavior.Default, token, useReadAsync);

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask ForEachAsync(this DbCommand command,
			Func<IDataRecord, ValueTask> handler, CommandBehavior behavior,
			CancellationToken token = default,
			bool useReadAsync = true)
		{
			if (handler == null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			if (useReadAsync)
			{
				while (await reader.ReadAsync(token))
					await handler(reader);
			}
			else if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read())
					await handler(reader);

				token.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read())
					await handler(reader);
			}
		}

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		public static ValueTask ForEachAsync(this DbCommand command,
			Func<IDataRecord, ValueTask> handler,
			CancellationToken token = default,
			bool useReadAsync = true)
			=> ForEachAsync(command, handler, CommandBehavior.Default, token, useReadAsync);

		/// <summary>
		/// Asynchronously iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The DbDataReader to load data from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="token">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, CancellationToken token = default)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			while (await reader.ReadAsync(token) && await predicate(reader)) { }
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="reader">The DbDataReader to load data from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, CancellationToken token = default, bool useReadAsync = true)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(token) && predicate(reader)) { }
			}
			else if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read() && predicate(reader))
					token.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read() && predicate(reader)) { }
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask IterateReaderWhileAsync(this DbCommand command, Func<IDataRecord, ValueTask<bool>> predicate, CommandBehavior behavior = CommandBehavior.Default, CancellationToken token = default, bool useReadAsync = true)
		{
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(token);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, token);
			await reader.IterateWhileAsync(predicate, useReadAsync, token);
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Throws if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T First<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
			return reader.Iterate(transform).First();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T FirstOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
			return reader.Iterate(transform).FirstOrDefault();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Throws if none or more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T Single<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Iterate(transform).Single();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.  Throws if more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T SingleOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Iterate(transform).SingleOrDefault();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the transform limited by the take count.</returns>
		public static List<T> Take<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Iterate(transform).Take(count).ToList();
		}

		/// <summary>
		/// Iterates an IDataReader and skips the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The number of records to skip.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the transform after the skip count.</returns>
		public static List<T> Skip<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (0 < count--) reader.Read();
			return reader.Iterate(transform).ToList();
		}

		/// <summary>
		/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="skip">The number of entries to skip before starting to take results.</param>
		/// <param name="take">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the skip, transform and take operation.</returns>
		public static List<T> SkipThenTake<T>(this IDbCommand command, int skip, int take, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (0 < skip--) reader.Read();
			return reader.Iterate(transform).Take(take).ToList();
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnMap">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<KeyValuePair<int, string>> columnMap)
		{
			if (columnMap == null) throw new ArgumentNullException(nameof(columnMap));
			Contract.EndContractBlock();

			return columnMap
				.ToDictionary(
					c => c.Value,
					c => DBNullValueToNull(record.GetValue(c.Key)));
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnMap">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<(int, string)> columnMap)
		{
			if (columnMap == null) throw new ArgumentNullException(nameof(columnMap));
			Contract.EndContractBlock();

			return columnMap
				.ToDictionary(
					c => c.Item2,
					c => DBNullValueToNull(record.GetValue(c.Item1)));
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, ISet<string> columnNames)
		{
			if (columnNames == null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			if (columnNames.Count != 0)
			{
				for (var i = 0; i < record.FieldCount; i++)
				{
					var n = record.GetName(i);
					if (columnNames.Contains(n))
						e.Add(n, DBNullValueToNull(record.GetValue(i)));
				}
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.  If none specified, the result will contain all columns.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, params string[] columnNames)
		{
			if (columnNames.Length != 0)
				return ToDictionary(record, new HashSet<string>(columnNames));

			var e = new Dictionary<string, object?>();
			for (var i = 0; i < record.FieldCount; i++)
			{
				var n = record.GetName(i);
				e.Add(n, DBNullValueToNull(record.GetValue(i)));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
			=> ToDictionary(record, new HashSet<string>(columnNames));

		/// <summary>
		/// Useful extension for dequeuing items from a queue.
		/// Not thread safe but queueing/dequeuing items in between items is supported.
		/// </summary>
		/// <typeparam name="T">Return type of the source queue</typeparam>
		/// <returns>An enumerable of the items contained within the queue.</returns>
		public static IEnumerable<T> DequeueEach<T>(this Queue<T> source)
		{
			while (source.Count != 0)
				yield return source.Dequeue();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are then converted to null.
		/// </summary>
		/// <returns>The enumerable first ordinal values.</returns>
		public static IEnumerable<object?> FirstOrdinalResults(this IDataReader reader)
		{
			var results = new Queue<object>(reader.Iterate(r => r.GetValue(0)));
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDataReader reader)
			=> reader is DbDataReader dbr
			? dbr.FirstOrdinalResults<T0>()
			: reader.FirstOrdinalResults().Cast<T0>();

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbDataReader reader)
		{
			var results = new Queue<T0>();
			while (reader.Read())
			{
				results.Enqueue(
					reader.IsDBNull(0)
					? default
					: reader.GetFieldValue<T0>(0)
				);
			}

			return results.DequeueEach();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are then converted to null.
		/// </summary>
		/// <returns>The enumerable first ordinal values.</returns>
		public static IEnumerable<object?> FirstOrdinalResults(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			var results = new Queue<object>(IterateReaderInternal(command, behavior | CommandBehavior.SingleResult, r => r.GetValue(0)));
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
			=> command is DbCommand dbc
			? dbc.FirstOrdinalResults<T0>()
			: command.FirstOrdinalResults(behavior | CommandBehavior.SingleResult).Cast<T0>();

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleResult);
			return reader.FirstOrdinalResults<T0>();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The list of values.</returns>
		public static async ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbDataReader reader, CancellationToken token = default, bool useReadAsync = true)
		{
			var results = new Queue<object>();
			await reader.ForEachAsync(r => results.Enqueue(r.GetValue(0)), token, useReadAsync).ConfigureAwait(false);
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The enumerable of casted values.</returns>
		public static async ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbDataReader reader, CancellationToken token = default, bool useReadAsync = true)
		{
			var results = new Queue<T0>();
			if (useReadAsync)
			{
				while (await reader.ReadAsync(token))
				{
					results.Enqueue(
						await reader.IsDBNullAsync(0, token).ConfigureAwait(false)
						? default
						: await reader.GetFieldValueAsync<T0>(0, token).ConfigureAwait(false)
					);
				}
			}
			else if (token.CanBeCanceled)
			{
				while (!token.IsCancellationRequested && reader.Read())
				{
					results.Enqueue(
						reader.IsDBNull(0)
						? default
						: reader.GetFieldValue<T0>(0)
					);
				}
				token.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read())
				{
					results.Enqueue(
						reader.IsDBNull(0)
						? default
						: reader.GetFieldValue<T0>(0)
					);
				}
			}

			return results.DequeueEach();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The list of values.</returns>
		public static ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, CancellationToken token = default, bool useReadAsync = true)
			=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync(token, useReadAsync), behavior | CommandBehavior.SingleResult, token);

		/// <summary>
		/// Reads the first column from every record..
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="token">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <returns>The enumerable of casted values.</returns>
		public static ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, CancellationToken token = default, bool useReadAsync = true)
			=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync<T0>(token, useReadAsync), behavior | CommandBehavior.SingleResult, token);

	}
}
