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
		/// <returns></returns>
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
            using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
                return reader.Iterate(transform).ToList();
        }

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTable.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDbCommand command)
        {
            using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
            {
                var table = new DataTable();
                table.Load(reader);
                return table;
            }
        }

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
		public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate)
        {
            while (reader.Read() && predicate(reader));
        }

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
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
		public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader,T> transform, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
               return transform(reader);
        }

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
                reader.Iterate(handler);
        }

        internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, Func<IDataRecord,T> transform, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
            {
                while(reader.Read())
                {
                    yield return transform(reader);
                }
            }
        }

        internal static IEnumerable<Dictionary<string,object>> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
            {
                while (reader.Read())
                {
                    yield return reader.ToDictionary();
                }
            }
        }

        internal static IEnumerable<Dictionary<string, object>> IterateReaderInternal(IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, r => r.ToDictionary(columnNames));

		/// <summary>
		/// Iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public static void IterateReaderWhile(this IDbCommand command, Func<IDataRecord, bool> predicate, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
                reader.IterateWhile(predicate);
        }

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
        public static Dictionary<string, object> ToDictionary(this IDataRecord record, HashSet<string> columnNames)
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
        {
            return ToDictionary(record, new HashSet<string>(columnNames));
        }

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> ToList(this IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, columnNames).ToList();

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> ToList(this IDbCommand command, IEnumerable<string> columnNames)
            => ToList(command, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.  If none specified, the results will contain all columns.</param>
		/// <returns>A list of dictionaries represending the requested data.</returns>
		public static List<Dictionary<string, object>> ToList(this IDbCommand command, params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal(command).ToList()
                : ToList(command, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as an array of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>An array of dictionaries represending the requested data.</returns>
		public static Dictionary<string, object>[] ToArray(this IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, columnNames).ToArray();

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as an array of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>An array of dictionaries represending the requested data.</returns>
		public static Dictionary<string, object>[] ToArray(this IDbCommand command, IEnumerable<string> columnNames)
            => ToArray(command, new HashSet<string>(columnNames));

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as an array of Dictionaries containing only the specified column values.
		/// </summary>
		/// <param name="command">The IDbCommand to generate the reader from.</param>
		/// <param name="columnNames">The column names to select.  If none specified, the results will contain all columns.</param>
		/// <returns>An array of dictionaries represending the requested data.</returns>
		public static Dictionary<string, object>[] ToArray(this IDbCommand command, params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal(command).ToArray()
                : ToArray(command, new HashSet<string>(columnNames));


		/// <summary>
		/// Iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to recieve the results.</param>
		public static void ToTargetBlock<T>(this IDataReader reader,
			Func<IDataRecord, T> transform,
			ITargetBlock<T> target)
		{
			while (target.IsStillAlive() && reader.Read() && target.Post(transform(reader)));
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
            string command, CommandType type = CommandType.Text)
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
