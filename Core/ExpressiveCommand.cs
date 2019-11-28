using System.Collections.Generic;
using System.Data;

namespace Open.Database.Extensions
{
	/// <summary>
	/// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
	/// </summary>
	public class ExpressiveCommand : ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, DbType, ExpressiveCommand>
	{

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveCommand(
			IDbConnectionFactory<IDbConnection> connFactory,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(connFactory, type, command, @params)
		{
		}

		/// <param name="connection">The connection to execute the command on.</param>
		/// <param name="transaction">The optional transaction to execute the command on.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveCommand(
			IDbConnection connection,
			IDbTransaction? transaction,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(connection, transaction, type, command, @params)
		{
		}

		/// <param name="connection">The connection to execute the command on.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveCommand(
			IDbConnection connection,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(connection, null, type, command, @params)
		{
		}

		/// <inheritdoc />
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
