using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Database.Extensions.SqlClient
{

    /// <summary>
    /// A specialized for SqlClient abstraction for executing commands on a database using best practices and simplified expressive syntax.
    /// </summary>
    public class ExpressiveSqlCommand : ExpressiveDbCommandBase<SqlConnection, SqlCommand, SqlDbType, ExpressiveSqlCommand>
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
       
    }

}
