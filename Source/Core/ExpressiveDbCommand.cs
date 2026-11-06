namespace Open.Database.Extensions;

/// <summary>
/// A deferred, fluent command for running SQL through any <see cref="DbConnection"/>, <see cref="DbTransaction"/>, connection factory, or connection pool. Unlike <see cref="ExpressiveCommand"/>, it exposes the <see cref="DbCommand"/> and <see cref="DbDataReader"/> types to the callbacks, so the provider-agnostic asynchronous ADO.NET APIs are available without casting.
/// </summary>
/// <remarks>
/// <para>
/// Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="ConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods.
/// </para>
/// <para>
/// Connections follow the same "get in, get out" model as <see cref="ExpressiveCommand"/>: each execution takes a connection, opens it only if needed, runs the command, and releases the connection. A connection supplied directly is closed again only if this execution opened it and is never disposed; a connection from a factory is disposed, and one from a pool is given back.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// List&lt;Customer&gt; customers = await connection
///     .Command("SELECT Id, Name FROM Customers WHERE Region = @region")
///     .AddParam("region", "West")
///     .UseCancellationToken(cancellationToken)
///     .Map&lt;Customer&gt;()
///     .ToListAsync();
/// </code>
/// </example>
/// <seealso cref="ExpressiveCommand"/>
/// <seealso cref="ConnectionExtensions"/>
public class ExpressiveDbCommand : ExpressiveDbCommandBase<DbConnection, DbCommand, DbDataReader, DbType, ExpressiveDbCommand>
{
	/// <summary>
	/// Constructs a command that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionPool">The pool to take connections from. Each execution takes one and gives it back when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text or stored procedure name.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveDbCommand(
		IDbConnectionPool<DbConnection> connectionPool,
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
	public ExpressiveDbCommand(
		IDbConnectionFactory<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that invokes a function to create a new connection for each execution.
	/// </summary>
	/// <param name="connFactory">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connFactory"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveDbCommand(
		Func<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
	: base(connFactory.AsPool(), type, command, @params)
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
	public ExpressiveDbCommand(
		DbConnection connection,
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
	public ExpressiveDbCommand(
		DbConnection connection,
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
		foreach (Param p in Params)
		{
			IDbDataParameter np = command.AddParameter(p.Name, p.Value);
			if (p.Type.HasValue) np.DbType = p.Type.Value;
		}
	}
}
