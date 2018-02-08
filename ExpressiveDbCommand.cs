using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Open.Database.Extensions
{
	/// <summary>
	/// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
	/// </summary>
	public class ExpressiveDbCommand : ExpressiveCommandBase<IDbConnection, IDbCommand, DbType, ExpressiveDbCommand>
    {
		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveDbCommand(
			IDbConnectionFactory<IDbConnection> connFactory,
			CommandType type,
			string command,
			List<Param> @params = null)
			: base(connFactory, type, command, @params)
        {
        }

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveDbCommand(
			IDbConnectionFactory<IDbConnection> connFactory,
			CommandType type,
			string command,
			params Param[] @params)
			: this(connFactory, type, command, @params.ToList())
        {
        }

		/// <summary>
		/// Handles adding the list of parameters to a new command.
		/// </summary>
		/// <param name="command"></param>
		protected override void AddParams(IDbCommand command)
        {
            foreach (var p in Params)
            {
                var np = command.AddParameter(p.Name, p.Value);
                if (p.Type.HasValue) np.DbType = p.Type.Value;
            }
        }
    }
}
