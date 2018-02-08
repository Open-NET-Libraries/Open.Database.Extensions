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
		public static void Iterate(this IDataReader reader, Action<IDataRecord> handler)
		{
			while (reader.Read()) handler(reader);
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
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, Func<IDataRecord, T> transform)
		{
			using (var reader = command.ExecuteReader())
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
		public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.Default)
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
			while (reader.Read() && predicate(reader)) ;
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
		public static TResult IterateReader<TEntity,TResult>(
			this IDbCommand command,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
		{
			using (var reader = command.ExecuteReader())
				return selector(reader.Iterate(transform));
		}


		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
				reader.Iterate(handler);
		}

		internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
			{
				while (reader.Read())
					yield return transform(reader);
			}
		}

		internal static IEnumerable<Dictionary<string, object>> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
			{
				if (reader.Read())
				{
					// First capture the indexes for reuse.
					yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes);
					while (reader.Read())
						yield return reader.ToDictionary(columnIndexes);
				}
			}
		}

		internal static IEnumerable<Dictionary<string, object>> IterateReaderInternal(IDbCommand command, ISet<string> columnNames, CommandBehavior behavior = CommandBehavior.Default)
		{
			using (var reader = command.ExecuteReader(behavior))
			{
				if (reader.Read())
				{
					yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, columnNames);
					while (reader.Read())
						yield return reader.ToDictionary(columnIndexes);
				}
			}
		}

		internal static IEnumerable<Dictionary<string, object>> IterateInternal(IDataReader reader, ISet<string> columnNames)
		{
			if (reader.Read())
			{
				yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, columnNames);
				while (reader.Read())
					yield return reader.ToDictionary(columnIndexes);
			}
		}

		internal static IEnumerable<Dictionary<string, object>> IterateInternal(IDataReader reader)
		{
			if (reader.Read())
			{
				yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes);
				while (reader.Read())
					yield return reader.ToDictionary(columnIndexes);
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
			using (var reader = command.ExecuteReader())
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
			using (var reader = command.ExecuteReader())
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
		/// Returns the specified column data of IDataRecord as a Dictionary and stores the column indexes in the columnIndexes out parameter.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnIndexes">The map of indexes to column names queried.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionaryOutIndexes(this IDataRecord record, out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, ISet<string> columnNames)
		{
			var indexes = new Dictionary<int, string>();
			var e = new Dictionary<string, object>();
			if (columnNames != null && columnNames.Count != 0)
			{
				for (var i = 0; i < record.FieldCount; i++)
				{
					var n = record.GetName(i);
					if (columnNames.Contains(n))
					{
						indexes.Add(i, n);
						e.Add(n, record.GetValue(i));
					}
				}
			}
			columnIndexes = indexes.OrderBy(kvp => kvp.Key).ToList().AsReadOnly();
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
		/// Returns the specified column data of IDataRecord as a Dictionary and stores the column indexes in the columnIndexes out parameter.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnIndexes">The map of indexes to column names queried.</param>
		/// <param name="columnNames">The column names to query.  If none specified, the result will contain all columns.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionaryOutIndexes(this IDataRecord record, out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, params string[] columnNames)
		{
			if (columnNames.Length != 0)
				return ToDictionaryOutIndexes(record, out columnIndexes, new HashSet<string>(columnNames));

			var indexes = new Dictionary<int, string>();
			var e = new Dictionary<string, object>();
			for (var i = 0; i < record.FieldCount; i++)
			{
				var n = record.GetName(i);
				indexes.Add(i, n);
				e.Add(n, record.GetValue(i));
			}
			columnIndexes = indexes.OrderBy(kvp => kvp.Key).ToList().AsReadOnly();
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

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnIndexes">The map of indexes to column names queried.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object> ToDictionaryOutIndexes(this IDataRecord record, out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, IEnumerable<string> columnNames)
			=> ToDictionaryOutIndexes(record, out columnIndexes, new HashSet<string>(columnNames));
		
		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDataReader reader, ISet<string> columnNames)
			=> IterateInternal(reader, columnNames).ToList();

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDataReader reader, IEnumerable<string> columnNames)
			=> Retrieve(reader, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="reader">The IDataReader to read results from.</param>
		/// <param name="columnNames">The column names to select.  If none specified, the results will contain all columns.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDataReader reader, params string[] columnNames)
			=> columnNames.Length == 0
				? IterateInternal(reader).ToList()
				: Retrieve(reader, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDbCommand command, ISet<string> columnNames)
			=> IterateReaderInternal(command, columnNames).ToList();

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDbCommand command, IEnumerable<string> columnNames)
			=> Retrieve(command, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.  If none specified, the results will contain all columns.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> Retrieve(this IDbCommand command, params string[] columnNames)
			=> columnNames.Length == 0
				? IterateReaderInternal(command).ToList()
				: Retrieve(command, new HashSet<string>(columnNames));

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
			var n = x.ColumnNames;

			// Use a queue so that when each item is subsequently enumerated, the reference is removed and memory is progressively cleaned up.
			var q = new Queue<Dictionary<string, object>>();
			foreach (var e in IterateInternal(reader, n))
				q.Enqueue(e);

			return x.Transform(q);
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receivethe results.</param>
		public static void ToTargetBlock<T>(this IDataReader reader,
			Func<IDataRecord, T> transform,
			ITargetBlock<T> target)
		{
			while (target.IsStillAlive() && reader.Read() && target.Post(transform(reader))) ;
		}

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
		{
			return new ExpressiveDbCommand(target, type, command);
		}

		/// <summary>
		/// Creates an ExpressiveDbCommand with command type set to StoredProcedure for subsequent configuration and execution.
		/// </summary>
		/// <param name="target">The connection factory to generate a commands from.</param>
		/// <param name="command">The command text or stored procedure name to use.</param>
		/// <returns>The resultant ExpressiveDbCommand.</returns>
		public static ExpressiveDbCommand StoredProcedure(
			this IDbConnectionFactory<IDbConnection> target,
			string command)
		{
			return new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);
		}
	}
}
