namespace Open.Database.Extensions;

/// <summary>
/// A deferred, fluent command for running SQL through a <see cref="SqlConnection"/>, <see cref="SqlTransaction"/>, connection factory, or connection pool, with parameters typed by <see cref="SqlDbType"/>. Nothing touches the database until an execute method is called.
/// </summary>
/// <remarks>
/// <para>
/// Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="SqlConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods (for example <c>ExecuteNonQuery</c>, <c>ExecuteScalar</c>, <c>ExecuteReader</c>, or <c>Map&lt;T&gt;()</c>).
/// </para>
/// <para>
/// Connections follow the same "get in, get out" model as <see cref="ExpressiveDbCommand"/>: each execution takes a connection, opens it only if needed, runs the command, and releases the connection. A connection supplied directly is closed again only if this execution opened it and is never disposed; a connection from a factory is disposed, and one from a pool is given back.
/// </para>
/// </remarks>
/// <seealso cref="ExpressiveDbCommand"/>
/// <seealso cref="SqlConnectionExtensions"/>
public class ExpressiveSqlCommand : ExpressiveDbCommandBase<SqlConnection, SqlCommand, SqlDataReader, SqlDbType, ExpressiveSqlCommand>
{
	/// <summary>
	/// Constructs a command that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionPool">The pool to take connections from. Each execution takes one and gives it back when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbConnectionPool<SqlConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that creates a new connection from a connection factory for each execution.
	/// </summary>
	/// <param name="connFactory">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connFactory"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbConnectionFactory<SqlConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that executes on an existing connection, optionally within a transaction.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="transaction">The optional transaction to execute the command in.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		SqlConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that executes on an existing connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		SqlConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that executes within a transaction, on the transaction's connection.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command in. Its <see cref="IDbTransaction.Connection"/> is used for execution.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Handles adding the list of parameters to a new command.
	/// </summary>
	protected override void AddParams(SqlCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		foreach (Param p in Params)
		{
			SqlParameter np;
			if (p.Value is null)
			{
				np = command.CreateParameter();
				np.ParameterName = p.Name;
			}
			else
			{
				np = command.Parameters.AddWithValue(p.Name, p.Value);
			}

			np.Direction = p.Direction;
			if (p.Type.HasValue)
				np.SqlDbType = p.Type.Value;
		}
	}
}
