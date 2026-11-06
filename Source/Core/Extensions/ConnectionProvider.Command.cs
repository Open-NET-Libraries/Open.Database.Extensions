namespace Open.Database.Extensions;

/// <summary>
/// ConnectionExtensions
/// </summary>
/// <remarks>
/// <para>
/// The <c>Command</c> and <c>StoredProcedure</c> extension methods are the entry points to the expressive API.
/// They create an <see cref="ExpressiveCommand"/> (or an <see cref="ExpressiveDbCommand"/> when the source is a <see cref="DbConnection"/>, a <see cref="DbTransaction"/>, or a typed factory or pool) from a connection, a transaction, a connection factory, a connection pool, or a factory function. Creating the command does no work; the database is first touched when an execute method is called on it.
/// </para>
/// <para>
/// The source decides who owns the connection:
/// </para>
/// <list type="bullet">
/// <item><description>A connection or transaction: the command runs on that connection. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</description></item>
/// <item><description>A connection factory or factory function: every execution creates a new connection, opens it if needed, and disposes it when finished.</description></item>
/// <item><description>A connection pool: every execution takes a connection from the pool, opens it if needed, and gives it back when finished.</description></item>
/// </list>
/// </remarks>
public static partial class ConnectionExtensions
{
	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that executes on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	/// <remarks>
	/// The command is not validated against the database when it is created; problems with the SQL surface as provider exceptions when it is executed. A command created from a connection supports one execution at a time.
	/// </remarks>
	/// <example>
	/// <code>
	/// int rows = connection
	///     .Command("UPDATE Customers SET Region = @region WHERE Id = @id")
	///     .AddParam("region", "West")
	///     .AddParam("id", 42)
	///     .ExecuteNonQuery();
	/// </code>
	/// </example>
	/// <seealso cref="StoredProcedure(IDbConnection, string)"/>
	public static ExpressiveCommand Command(
		this IDbConnection connection,
		string command, CommandType type = CommandType.Text)
		=> new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that executes within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveCommand Command(
		this IDbTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that executes on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveDbCommand Command(
	   this DbConnection connection,
	   string command, CommandType type = CommandType.Text)
	   => new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that executes within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveDbCommand Command(
		this DbTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that calls a stored procedure on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that calls a stored procedure within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveCommand StoredProcedure(
		this IDbTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that calls a stored procedure on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveDbCommand StoredProcedure(
		this DbConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that calls a stored procedure within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveDbCommand StoredProcedure(
		this DbTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that creates a new connection from a connection factory for every execution.
	/// </summary>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	/// <seealso cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/>
	public static ExpressiveCommand Command(
		this IDbConnectionFactory connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that calls a stored procedure, creating a new connection from a connection factory for every execution.
	/// </summary>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnectionFactory connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveCommand Command(
		this IDbConnectionPool connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that calls a stored procedure, taking its connections from a connection pool.
	/// </summary>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveCommand StoredProcedure(
		this IDbConnectionPool connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that creates a new connection from a connection factory for every execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveDbCommand Command<TConnection>(
		this IDbConnectionFactory<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that calls a stored procedure, creating a new connection from a connection factory for every execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this IDbConnectionFactory<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that takes its connections from a connection pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveDbCommand Command<TConnection>(
		this IDbConnectionPool<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that calls a stored procedure, taking its connections from a connection pool.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this IDbConnectionPool<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that invokes a function to create a new connection for every execution.
	/// </summary>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveCommand Command(
		this Func<IDbConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> Command(connectionSource.AsPool(), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveCommand"/> that calls a stored procedure, invoking a function to create a new connection for every execution.
	/// </summary>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveCommand StoredProcedure(
		this Func<IDbConnection> connectionSource,
		string procedureName)
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that invokes a function to create a new connection for every execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	/// <example>
	/// <code>
	/// // A new connection is created, opened, and disposed for every execution.
	/// int count = createConnection
	///     .Command("SELECT COUNT(*) FROM Customers")
	///     .ExecuteScalar&lt;int&gt;();
	/// </code>
	/// </example>
	public static ExpressiveDbCommand Command<TConnection>(
		this Func<TConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		where TConnection : DbConnection
		=> Command(connectionSource.AsPool(), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveDbCommand"/> that calls a stored procedure, invoking a function to create a new connection for every execution.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveDbCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveDbCommand StoredProcedure<TConnection>(
		this Func<TConnection> connectionSource,
		string procedureName)
		where TConnection : DbConnection
		=> StoredProcedure(connectionSource.AsPool(), procedureName);
}
