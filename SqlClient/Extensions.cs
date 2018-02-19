
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        /// Shortcut for creating an SqlCommand from any SqlConnection.
        /// </summary>
        /// <param name="connection">The connection to create a command from.</param>
        /// <param name="commandText">The command text or stored procedure name to use.</param>
        /// <param name="secondsTimeout">The number of seconds to wait before the command times out.</param>
        /// <returns>The created SqlCommand.</returns>
        public static SqlCommand CreateStoredProcedureCommand(this SqlConnection connection,
            string commandText, int secondsTimeout = CommandTimeout.DEFAULT_SECONDS)
            => connection.CreateCommand(CommandType.StoredProcedure, commandText, secondsTimeout);

        /// <summary>
        /// Shortcut for adding command parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="type">The DbType of the parameter.</param>
        /// <param name="direction">The direction of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static SqlParameter AddParameter(this SqlCommand target,
            string name, object value, SqlDbType type, ParameterDirection direction = ParameterDirection.Input)
        {
            var p = target.AddParameterType(name, type, direction);
            p.Value = value;
            return p;
        }

        /// <summary>
        /// Shortcut for adding command a typed (non-input) parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="type">The SqlDbType of the parameter.</param>
        /// <param name="direction">The direction of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static SqlParameter AddParameterType(this SqlCommand target,
            string name, SqlDbType type, ParameterDirection direction = ParameterDirection.Input)
        {
            var c = target.CreateParameter();
            c.ParameterName = name;
            c.SqlDbType = type;
            c.Direction = direction;
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
        public static SqlParameter AddParameterType(this IDbCommand target, string name, SqlDbType type)
        {
            return AddParameterType((SqlCommand)target, name, type);
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
