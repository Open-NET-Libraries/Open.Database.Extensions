using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions
{

	/// <summary>
	/// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
	/// </summary>
	public class ExpressiveDbCommand : ExpressiveDbCommandBase<DbConnection, DbCommand, DbDataReader, DbType, ExpressiveDbCommand>
	{
		/// <param name="connectionPool">The pool to acquire connections from.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveDbCommand(
			IDbConnectionPool<DbConnection> connectionPool,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(connectionPool, type, command, @params)
		{
		}

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveDbCommand(
			IDbConnectionFactory<DbConnection> connFactory,
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
		public ExpressiveDbCommand(
			DbConnection connection,
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
		public ExpressiveDbCommand(
			DbConnection connection,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(connection, type, command, @params)
		{
		}

		/// <param name="transaction">The transaction to execute the command on.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		public ExpressiveDbCommand(
			IDbTransaction transaction,
			CommandType type,
			string command,
			IEnumerable<Param>? @params = null)
			: base(transaction, type, command, @params)
		{
		}

		/// <inheritdoc />
		protected override void AddParams(DbCommand command)
		{
			foreach (var p in Params)
			{
				var np = command.AddParameter(p.Name, p.Value);
				if (p.Type.HasValue) np.DbType = p.Type.Value;
			}
		}

	}

}
