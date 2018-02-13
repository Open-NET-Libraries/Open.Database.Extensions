
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.SqlClient
{
    /// <summary>
    /// SqlClient extensions for building a command and retrieving data using best practices.
    /// </summary>
    public static class Extensions
    {

        /// <summary>
        /// Shortcut for adding command parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="type">The DbType of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static IDbDataParameter AddParameter(this SqlCommand target,
            string name, object value, SqlDbType type)
        {
            var p = target.AddParameterType(name, type);
            p.Value = value;
            return p;
        }

        /// <summary>
        /// Shortcut for adding command a typed (non-input) parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="type">The SqlDbType of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static IDbDataParameter AddParameterType(this SqlCommand target, string name, SqlDbType type)
        {
            var c = target.CreateParameter();
            c.ParameterName = name;
            c.SqlDbType = type;
            target.Parameters.Add(c);
            return c;
        }

        /// <summary>
        /// Shortcut for adding command a typed (non-input) parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="type">The SqlDbType of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static IDbDataParameter AddParameterType(this IDbCommand target, string name, SqlDbType type)
        {
            return AddParameterType((SqlCommand)target, name, type);
        }

        /// <summary>
        /// Shortcut for creating an SqlCommand from any SqlConnection.
        /// </summary>
        /// <param name="connection">The connection to create a command from.</param>
        /// <param name="type">The command type.  Text, StoredProcedure, or TableDirect.</param>
        /// <param name="commandText">The command text or stored procedure name to use.</param>
        /// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
        /// <returns>The created SqlCommand.</returns>
        public static SqlCommand CreateCommand(this SqlConnection connection,
            CommandType type, string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
        {
            var command = connection.CreateCommand();
            command.CommandType = type;
            command.CommandText = commandText;
            command.CommandTimeout = secondsTimeout;

            return command;
        }

        /// <summary>
        /// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="reader">The SqlDataReader to read from.</param>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <returns>A task containing a list of all results.</returns>
        public static async Task<List<T>> ToListAsync<T>(this SqlDataReader reader,
            Func<IDataRecord, T> transform)
        {
            var list = new List<T>();
            while (await reader.ReadAsync()) list.Add(transform(reader));
            return list;
        }

        /// <summary>
        /// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="command">The SqlCommand to generate a reader from.</param>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <returns>A task containing a list of all results.</returns>
        public static async Task<List<T>> ToListAsync<T>(this SqlCommand command,
            Func<IDataRecord, T> transform)
        {
            using (var reader = await command.ExecuteReaderAsync())
                return await reader.ToListAsync(transform);
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="reader">The SqlDataReader to read from.</param>
        /// <param name="target">The target block to receive the results.</param>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        public static async Task ToTargetBlockAsync<T>(this SqlDataReader reader,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
        {
            Task<bool> lastSend = null;
            while (target.IsStillAlive()
                && await reader.ReadAsync()
                && (lastSend == null || await lastSend))
            {
                // Allows for a premtive read before waiting for the next send.
                lastSend = target.SendAsync(transform(reader));
            }
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="command">The IDbCommand to generate a reader from.</param>
        /// <param name="target">The target block to receive the results.</param>
        /// <param name="transform">The transform function for each IDataRecord.</param>
        public static async Task ToTargetBlockAsync<T>(this SqlCommand command,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
        {
            if (target.IsStillAlive())
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (target.IsStillAlive())
                        await reader.ToTargetBlockAsync(target, transform);
                }
            }
        }

        /// <summary>
        /// Asynchronously iterates all records from an IDataReader.
        /// </summary>
        /// <param name="command">The IDbCommand to generate a reader from.</param>
        /// <param name="handler">The handler function for each IDataRecord.</param>
        /// <param name="token">Optional cancellatio token.</param>
        public static async Task IterateReaderAsync(this SqlCommand command, Action<IDataRecord> handler, CancellationToken? token = null)
        {
            using (var reader = await command.ExecuteReaderAsync())
            {
                if (token.HasValue)
                {
                    var t = token.Value;
                    while (!t.IsCancellationRequested && await reader.ReadAsync())
                        handler(reader);
                }
                else
                {
                    while (await reader.ReadAsync())
                        handler(reader);
                }
            }
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader on a command while the predicate returns true.
        /// </summary>
        /// <param name="command">The IDbCommand to generate a reader from.</param>
        /// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        public static async Task IterateReaderAsyncWhile(this SqlCommand command, Func<IDataRecord, bool> predicate, CommandBehavior behavior = CommandBehavior.Default)
        {
            using (var reader = await command.ExecuteReaderAsync(behavior))
                while (await reader.ReadAsync() && predicate(reader)) { }
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader on a command while the predicate returns true.
        /// </summary>
        /// <param name="command">The IDbCommand to generate a reader from.</param>
        /// <param name="predicate">The hanlder function that processes each IDataRecord and decides if iteration should continue.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        public static async Task IterateReaderAsyncWhile(this SqlCommand command, Func<IDataRecord, Task<bool>> predicate, CommandBehavior behavior = CommandBehavior.Default)
        {
            using (var reader = await command.ExecuteReaderAsync(behavior))
                while (await reader.ReadAsync() && await predicate(reader)) { }
        }


        /// <summary>
        /// Asynchronously enumerates all the remaining values of the current result set of a data reader.
        /// </summary>
        /// <param name="reader">The reader to enumerate.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
        public static async Task<QueryResult<Queue<object[]>>> RetrieveAsync(this SqlDataReader reader)
        {
            var fieldCount = reader.FieldCount;
            var names = reader.GetNames();
            var buffer = new Queue<object[]>();

            while (await reader.ReadAsync())
            {
                var row = new object[fieldCount];
                reader.GetValues(row);
                buffer.Enqueue(row);
            }

            return new QueryResult<Queue<object[]>>(
                Enumerable.Range(0, names.Length).ToArray(),
                names,
                buffer);
        }

        static async Task<QueryResult<Queue<object[]>>> RetrieveAsyncInternal(SqlDataReader reader, int[] ordinals, string[] columnNames = null)
        {
            var fieldCount = ordinals.Length;
            if (columnNames == null) columnNames = ordinals.Select(n => reader.GetName(n)).ToArray();
            else if (columnNames.Length != fieldCount) throw new ArgumentException("Mismatched array lengths of ordinals and names.");

            Func<IDataRecord, object[]> handler;
            if (fieldCount == 0) handler = record => Array.Empty<object>();
            else handler = record =>
            {
                var row = new object[fieldCount];
                for (var i = 0; i < fieldCount; i++)
                    row[i] = reader.GetValue(ordinals[i]);
                return row;
            };

            var buffer = new Queue<object[]>();
            while (await reader.ReadAsync())
                buffer.Enqueue(handler(reader));

            return new QueryResult<Queue<object[]>>(
                ordinals,
                columnNames,
                buffer);
        }

        /// <summary>
        /// Asynchronously enumerates all the remaining values of the current result set of a data reader.
        /// </summary>
        /// <param name="reader">The reader to enumerate.</param>
        /// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
        public static Task<QueryResult<Queue<object[]>>> RetrieveAsync(this SqlDataReader reader, IEnumerable<int> ordinals)
            => RetrieveAsyncInternal(reader, ordinals as int[] ?? ordinals.ToArray());  

        /// <summary>
        /// Asynchronously enumerates all the remaining values of the current result set of a data reader.
        /// </summary>
        /// <param name="reader">The reader to enumerate.</param>
        /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
        /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains a buffer block of the results and the column mappings.</returns>
        public static Task<QueryResult<Queue<object[]>>> RetrieveAsync(this SqlDataReader reader, int n, params int[] others)
            => RetrieveAsync(reader, new int[1] { n }.Concat(others));

        /// <summary>
        /// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
        /// </summary>
        /// <param name="reader">The IDataReader to read results from.</param>
        /// <param name="columnNames">The column names to select.</param>
        /// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public static Task<QueryResult<Queue<object[]>>> RetrieveAsync(this SqlDataReader reader, IEnumerable<string> columnNames, bool normalizeColumnOrder = false)
        {
            // Validate the requested columns first.
            var x = columnNames
                .Select(n => (name: n, ordinal: reader.GetOrdinal(n)));

            if (normalizeColumnOrder)
                x = x.OrderBy(c => c.ordinal);

            var columns = x.ToArray();

            return RetrieveAsyncInternal(reader, 
                columns.Select(c => c.ordinal).ToArray(),
                columns.Select(c => c.name).ToArray());
        }

        /// <summary>
        /// Asynchronously enumerates all records within the current result set using an IDataReader and returns the desired results.
        /// </summary>
        /// <param name="reader">The IDataReader to read results from.</param>
        /// <param name="c">The first column name to include in the request to the reader for each record.</param>
        /// <param name="others">The remaining column names to request from the reader for each record.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public static Task<QueryResult<Queue<object[]>>> RetrieveAsync(this SqlDataReader reader, string c, params string[] others)
            => RetrieveAsync(reader, new string[1] { c }.Concat(others));


        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="reader">The IDataReader to read results from.</param>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public static async Task<IEnumerable<T>> ResultsAsync<T>(this SqlDataReader reader, IEnumerable<(string Field, string Column)> fieldMappingOverrides) where T : new()
        {
            var x = new Transformer<T>(fieldMappingOverrides);
            return x.AsDequeueingEnumerable(await reader.RetrieveAsync(x.ColumnNames));
        }

        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="reader">The IDataReader to read results from.</param>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public static Task<IEnumerable<T>> ResultsAsync<T>(this SqlDataReader reader, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides) where T : new()
            => reader.ResultsAsync<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="reader">The IDataReader to read results from.</param>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public static async Task<IEnumerable<T>> ResultsAsync<T>(this SqlDataReader reader, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
        {
            var x = new Transformer<T>(fieldMappingOverrides);
            return x.AsDequeueingEnumerable(await reader.RetrieveAsync(x.ColumnNames));
        }

        /// <summary>
        /// Creates an ExpressiveSqlCommand for subsequent configuration and execution.
        /// </summary>
        /// <param name="target">The connection factory to generate a commands from.</param>
        /// <param name="command">The command text or stored procedure name to use.</param>
        /// <param name="type">The command type.</param>
        /// <returns>The resultant ExpressiveSqlCommand.</returns>
        public static ExpressiveSqlCommand Command(
            this IDbConnectionFactory<SqlConnection> target,
            string command, CommandType type = CommandType.Text)
        {
            return new ExpressiveSqlCommand(target, type, command);
        }

        /// <summary>
        /// Creates an ExpressiveSqlCommand with command type set to StoredProcedure for subsequent configuration and execution.
        /// </summary>
        /// <param name="target">The connection factory to generate a commands from.</param>
        /// <param name="command">The command text or stored procedure name to use.</param>
        /// <returns>The resultant ExpressiveSqlCommand.</returns>
        public static ExpressiveSqlCommand StoredProcedure(
            this IDbConnectionFactory<SqlConnection> target,
            string command)
        {
            return new ExpressiveSqlCommand(target, CommandType.StoredProcedure, command);
        }
    }
}
