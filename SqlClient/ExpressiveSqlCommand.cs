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
	/// A specialized for SqlClient abstraction for executing commands on a database using best practices and simplified expressive syntax.
	/// </summary>
	public class ExpressiveSqlCommand : ExpressiveAsyncCommandBase<SqlConnection, SqlCommand, SqlDbType, ExpressiveSqlCommand>
    {
		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string command, List<Param> @params = null)
            : base(connFactory, type, command, @params)
        {
        }

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string command, params Param[] @params)
            : this(connFactory, type, command, @params.ToList())
        {
        }

		/// <summary>
		/// Handles adding the list of parameters to a new command.
		/// </summary>
		/// <param name="command"></param>
		protected override void AddParams(SqlCommand command)
        {
            foreach (var p in Params)
            {
                var np = command.Parameters.AddWithValue(p.Name, p.Value);
                if (p.Type.HasValue) np.SqlDbType = p.Type.Value;
            }
        }

		/// <summary>
		/// Asynchronously executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public override async Task ExecuteAsync(Func<SqlCommand, Task> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(
                Type, Command, Timeout))
            {
                AddParams(cmd);
                await con.OpenAsync();
                await handler(cmd);
            }
        }

		/// <summary>
		/// Asynchronously executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		public override async Task<T> ExecuteAsync<T>(Func<SqlCommand, Task<T>> transform)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(
                Type, Command, Timeout))
            {
                AddParams(cmd);
                await con.OpenAsync();
                return await transform(cmd);
            }
        }

		/// <summary>
		/// Asynchronously executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for the data reader.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public Task ExecuteReaderAsync(Func<SqlDataReader, Task> handler, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await handler(await command.ExecuteReaderAsync(behavior)));

		/// <summary>
		/// Asynchronously executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The result of the transform.</returns>
		public Task<T> ExecuteReaderAsync<T>(Func<SqlDataReader, Task<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await transform(await command.ExecuteReaderAsync(behavior)));

		/// <summary>
		/// Calls ExecuteNonQueryAsync on the underlying command.
		/// </summary>
		/// <returns>The integer responise from the method.</returns>
		public override Task<int> ExecuteNonQueryAsync()
            => ExecuteAsync(command => command.ExecuteNonQueryAsync());

		/// <summary>
		/// Calls ExecuteScalarAsync on the underlying command.
		/// </summary>
		/// <returns>The varlue returned from the method.</returns>
		public override Task<object> ExecuteScalarAsync()
            => ExecuteAsync(command => command.ExecuteScalarAsync());

		/// <summary>
		/// Iterates asynchronously and will stop iterating if canceled.
		/// </summary>
		/// <param name="handler">The active IDataRecord is passed to this handler.</param>
		/// <param name="token">An optional cancellation token.</param>
		/// <returns></returns>
		public override Task IterateReaderAsync(Action<IDataRecord> handler, CancellationToken? token = null)
            => ExecuteAsync(command => command.IterateReaderAsync(handler, token));

		/// <summary>
		/// Iterates asynchronously until the handler returns false.  Then cancels.
		/// </summary>
		/// <param name="predicate">If true, the iteration continues.</param>
		/// <returns>The task that completes when the iteration is done or the predicate evaluates false.</returns>
		public override Task IterateReaderAsyncWhile(Func<IDataRecord, bool> predicate)
            => ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));

		/// <summary>
		/// Iterates asynchronously until the handler returns false.  Then cancels.
		/// </summary>
		/// <param name="predicate">If true, the iteration continues.</param>
		/// <returns>The task that completes when the iteration is done or the predicate evaluates false.</returns>
		public override Task IterateReaderAsyncWhile(Func<IDataRecord, Task<bool>> predicate)
			=> ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));


        /// <summary>
        /// Returns a source block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public override ISourceBlock<T> AsSourceBlockAsync<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;
			var block = x.ResultsBlock(out Action<string[]> initColumnNames);

			ExecuteReaderAsync(async reader =>
			{
				// Validate the requested columns first.
				var columns = cn
					.Select(n => (name: n, ordinal: reader.GetOrdinal(n)))
					.OrderBy(c => c.ordinal)
					.ToArray();

				var ordinalValues = columns.Select(c => c.ordinal).ToArray();
				initColumnNames(columns.Select(c => c.name).ToArray());

				Task<bool> lastSend = null;
				while(await reader.ReadAsync())
				{
					if (lastSend!=null && !await lastSend) break;
					lastSend = block.SendAsync(reader.GetValuesFromOrdinals(ordinalValues));
				}

				block.Complete();
			});

			return block;
		}

        /// <summary>
        /// Asynchronously iterates all records within the first result set using an IDataReader and returns the results.
        /// </summary>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public override Task<QueryResult<Queue<object[]>>> RetrieveAsync()
            => ExecuteReaderAsync(reader => reader.RetrieveAsync());

        /// <summary>
        /// Asynchronously iterates all records within the current result set using an IDataReader and returns the desired results.
        /// </summary>
        /// <param name="ordinals">The ordinals to request from the reader for each record.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public override Task<QueryResult<Queue<object[]>>> RetrieveAsync(IEnumerable<int> ordinals)
            => ExecuteReaderAsync(reader => reader.RetrieveAsync(ordinals));

        /// <summary>
        /// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
        /// </summary>
        /// <param name="columnNames">The column names to select.</param>
        /// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public override Task<QueryResult<Queue<object[]>>> RetrieveAsync(IEnumerable<string> columnNames, bool normalizeColumnOrder = false)
            => ExecuteReaderAsync(reader => reader.RetrieveAsync(columnNames, normalizeColumnOrder));
    }

}
