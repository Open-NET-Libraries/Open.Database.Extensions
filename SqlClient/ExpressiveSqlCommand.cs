using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
		/// Asynchronously returns all records via a transform function.
		/// </summary>
		/// <param name="transform">The desired column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public override Task<List<T>> ToListAsync<T>(Func<IDataRecord, T> transform)
            => ExecuteAsync(command => command.ToListAsync(transform));

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
		/// <param name="predicate"></param>
		/// <returns></returns>
		public override Task IterateReaderAsyncWhile(Func<IDataRecord, bool> predicate)
            => ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));

    }

}
