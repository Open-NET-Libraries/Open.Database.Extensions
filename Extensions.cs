using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Core non-DB-specific extensions for building a command and retrieving data using best practices.
	/// </summary>
	public static partial class Extensions
	{
		internal static bool IsStillAlive(this Task task)
		{
			return !task.IsCompleted && !task.IsCanceled && !task.IsFaulted;
		}

		internal static bool IsStillAlive<T>(this ITargetBlock<T> task)
		{
			return IsStillAlive(task.Completion);
		}

		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameter(this IDbCommand target,
			string name, object value = null)
		{
			var c = target.CreateParameter();
			c.ParameterName = name;
			if (value != null) // DBNull.Value is allowed.
				c.Value = value;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Shortcut for adding command a typed (non-input) parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameterType(this IDbCommand target,
			string name, DbType type)
		{
			var c = target.CreateParameter();
			c.ParameterName = name;
			c.DbType = type;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Shortcut for creating an IDbCommand from any IDbConnection.
		/// </summary>
		/// <param name="connection">The connection to create a command from.</param>
		/// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
		/// <param name="commandText">The command text or stored procedure name to use.</param>
		/// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
		/// <returns>The created IDbCommand.</returns>
		public static IDbCommand CreateCommand(this IDbConnection connection,
			CommandType type,
			string commandText,
			int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
		{
			var command = connection.CreateCommand();
			command.CommandType = type;
			command.CommandText = commandText;
			command.CommandTimeout = secondsTimeout;

			return command;
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void ForEach(this IDataReader reader, Action<IDataRecord> handler)
		{
			while (reader.Read()) handler(reader);
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
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An array of values matching the ordinal positions requested.</returns>
		public static object[] GetValuesFromOrdinals(this IDataRecord record, params int[] ordinals)
		{
			var count = ordinals.Length;
			if (count == 0) return Array.Empty<object>();
			
			var values = new object[count];
			for (var i = 0; i < count; i++)
				values[i] = record.GetValue(ordinals[i]);
			return values;
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

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals)
		{
			if (reader.Read())
			{
				var o = ordinals.ToArray();
				var fieldCount = o.Length;
				if (fieldCount == 0)
				{
					do
					{
						yield return Array.Empty<object>();
					} while (reader.Read());
				}
				else
				{
					do
					{
						var row = new object[fieldCount];
						for (var i = 0; i < fieldCount; i++)
							row[i] = reader.GetValue(i);
						yield return row;
					} while (reader.Read());
				}

			}
		}

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params int[] others)
			=> AsEnumerable(reader, new int[1] { n }.Concat(others));

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
		/// Loads all data into a queue before iterating (dequeing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
		/// <returns></returns>
		public static IEnumerable<T> ToEntities<T>(this DataTable table, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null, bool clearSourceTable = false) where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.Results(table, clearSourceTable);
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> Iterate<T>(this IDataReader reader, Func<IDataRecord, T> transform)
		{
			while (reader.Read())
				yield return transform(reader);
		}

		/// <summary>
		/// Shortcut for .Iterate(transform).ToList();
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of the transformed results.</returns>
		public static List<T> ToList<T>(this IDataReader reader, Func<IDataRecord, T> transform)
			=> reader.Iterate(transform).ToList();

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
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
				return reader.Iterate(transform).ToList();
		}

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader(behavior))
				return reader.Iterate(transform).ToList();
		}

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
				return reader.Iterate(transform).ToArray();
		}

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader(behavior))
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
		/// Loads all data from a command through an IDataReader into a DataTable.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			using (var reader = command.ExecuteReader(behavior))
				return reader.ToDataTable();
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
		public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate)
		{
			while (reader.Read() && predicate(reader)) { }
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
				handler(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The result of the transform.</returns>
		public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
				return transform(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
		{
			using (var reader = command.ExecuteReader())
				return selector(reader.Iterate(transform));
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
		{
			using (var reader = command.ExecuteReader(behavior))
				return selector(reader.Iterate(transform));
		}

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler)
		{
			using (var reader = command.ExecuteReader())
				reader.ForEach(handler);
		}

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void IterateReader(this IDbCommand command, CommandBehavior behavior, Action<IDataRecord> handler)
		{
			using (var reader = command.ExecuteReader(behavior))
				reader.ForEach(handler);
		}

		internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader(behavior))
			{
				while (reader.Read())
					yield return transform(reader);
			}
		}

		internal static IEnumerable<object[]> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
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
		}

		/// <summary>
		/// Iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void IterateReaderWhile(this IDbCommand command, Func<IDataRecord, bool> predicate, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
				reader.IterateWhile(predicate);
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Throws if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public static T First<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
				return reader.Iterate(transform).First();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public static T FirstOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
				return reader.Iterate(transform).FirstOrDefault();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Throws if none or more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public static T Single<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
				return reader.Iterate(transform).Single();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.  Throws if more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public static T SingleOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
				return reader.Iterate(transform).SingleOrDefault();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform limited by the take count.</returns>
		public static List<T> Take<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
				return reader.Iterate(transform).Take(count).ToList();
		}

		/// <summary>
		/// Iterates an IDataReader and skips the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The number of records to skip.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform after the skip count.</returns>
		public static List<T> Skip<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
			{
				while (0 < count--) reader.Read();
				return reader.Iterate(transform).ToList();
			}
		}

		/// <summary>
		/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="skip">The number of entries to skip before starting to take results.</param>
		/// <param name="take">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the skip, transform and take operation.</returns>
		public static List<T> SkipThenTake<T>(this IDbCommand command, int skip, int take, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
			{
				while (0 < skip--) reader.Read();
				return reader.Iterate(transform).Take(take).ToList();
			}
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnMap">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionary(this IDataRecord record, IEnumerable<KeyValuePair<int, string>> columnMap)
		{
			var e = new Dictionary<string, object>();
			if (columnMap != null)
			{
				foreach (var c in columnMap)
					e.Add(c.Value, record.GetValue(c.Key));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnMap">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionary(this IDataRecord record, IEnumerable<(int, string)> columnMap)
		{
			var e = new Dictionary<string, object>();
			if (columnMap != null)
			{
				foreach (var c in columnMap)
					e.Add(c.Item2, record.GetValue(c.Item1));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionary(this IDataRecord record, ISet<string> columnNames)
		{
			var e = new Dictionary<string, object>();
			if (columnNames != null && columnNames.Count != 0)
			{
				for (var i = 0; i < record.FieldCount; i++)
				{
					var n = record.GetName(i);
					if (columnNames.Contains(n))
						e.Add(n, record.GetValue(i));
				}
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.  If none specified, the result will contain all columns.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionary(this IDataRecord record, params string[] columnNames)
		{
			if (columnNames.Length != 0)
				return ToDictionary(record, new HashSet<string>(columnNames));

			var e = new Dictionary<string, object>();
			for (var i = 0; i < record.FieldCount; i++)
			{
				var n = record.GetName(i);
				e.Add(n, record.GetValue(i));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
			=> ToDictionary(record, new HashSet<string>(columnNames));

		static QueryResult<Queue<object[]>> RetrieveBlanksInternal(IDataReader reader)
			=> new QueryResult<Queue<object[]>>(
				Array.Empty<int>(),
				Array.Empty<string>(),
				new Queue<object[]>(AsEnumerable(reader, Enumerable.Empty<int>())));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the results.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader)
		{
			var names = reader.GetNames();
			return new QueryResult<Queue<object[]>>(
				Enumerable.Range(0, names.Length).ToArray(),
				names,
				new Queue<object[]>(AsEnumerable(reader)));
		}

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, IEnumerable<int> ordinals)
		{
			var o = ordinals as ISet<int> ?? new HashSet<int>(ordinals);
			if (o.Count == 0)
			{
				// No column names specified?  Then return results, but empty ones.  Simplify the results for counting.  
				return RetrieveBlanksInternal(reader);
			}
			else
			{
				var ordinalValues = ordinals.OrderBy(n => n).ToArray();
				return new QueryResult<Queue<object[]>>(
					ordinalValues,
					ordinalValues.Select(n => reader.GetName(n)).ToArray(),
					new Queue<object[]>(AsEnumerable(reader, ordinalValues)));
			}
		}

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, int n, params int[] others)
			=> Retrieve(reader, new int[1] { n }.Concat(others));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, IEnumerable<string> columnNames)
		{
			var cn = columnNames as ISet<string> ?? new HashSet<string>(columnNames);
			if (cn.Count == 0)
			{
				// No column names specified?  Then return results, but empty ones.  Simplify the results for counting.  
				return RetrieveBlanksInternal(reader);
			}
			else
			{
				// Validate the requested columns first.
				var columns = cn
					.Select(n => (name: n, ordinal: reader.GetOrdinal(n)))
					.OrderBy(c => c.ordinal)
					.ToArray();

				var ordinalValues = columns.Select(c => c.ordinal).ToArray();
				return new QueryResult<Queue<object[]>>(
					ordinalValues,
					columns.Select(c => c.name).ToArray(),
					new Queue<object[]>(AsEnumerable(reader, ordinalValues)));
			}
		}
		
		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDataReader reader, string c, params string[] others)
			=> Retrieve(reader, new string[1] { c }.Concat(others));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command)
			=> ExecuteReader(command, reader => reader.Retrieve());

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, IEnumerable<int> ordinals)
			=> ExecuteReader(command, reader => reader.Retrieve(ordinals));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, int n, params int[] others)
			=> ExecuteReader(command, reader => Retrieve(reader, new int[1] { n }.Concat(others)));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, IEnumerable<string> columnNames)
			=> ExecuteReader(command, reader => reader.Retrieve(columnNames));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>the DataReaderResults that contain all the results and the column mappings.</returns>
		public static QueryResult<Queue<object[]>> Retrieve(this IDbCommand command, string c, params string[] others)
			=> ExecuteReader(command, reader => Retrieve(reader, new string[1] { c }.Concat(others)));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDataReader reader, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(Retrieve(reader, x.ColumnNames));
		}

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public static IEnumerable<T> Results<T>(this IDbCommand command, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(Retrieve(command, x.ColumnNames));
		}

		// NOTE: The Results<T> methods should faster than the ResultsFromDataTable<T> variations but are provided for validation of this assumption.

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
			=> reader.ToDataTable().ToEntities<T>(fieldMappingOverrides, true);

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
			=> command.ToDataTable().ToEntities<T>(fieldMappingOverrides, true);

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receivethe results.</param>
		public static void ToTargetBlock<T>(this IDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform)
		{
			while (target.IsStillAlive() && reader.Read() && target.Post(transform(reader))) { }
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		public static void ToTargetBlock<T>(this IDbCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform)
			=> command.ExecuteReader(reader => reader.ToTargetBlock(target, transform));

		// NOTE: Do not provide AsSourceBlock extensions due to the asynchronous nature.
		// If an asynchronous source block is desired, then the extension needs full control of command setup and execution to ensure proper disposal and completion.

		/// <summary>
		/// Creates an ExpressiveDbCommand for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <param name="type">The command type.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand Command(
			this IDbConnectionFactory<IDbConnection> target,
			string command,
			CommandType type = CommandType.Text)
			=> new ExpressiveDbCommand(target, type, command);

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this IDbConnectionFactory<IDbConnection> target,
			string command)
			=> new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);
	}
}
