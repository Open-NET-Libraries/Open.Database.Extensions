namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for acquiring and operating on different connection factories.
/// </summary>
[Obsolete(SqlClientObsolete._message)]
public static partial class SqlConnectionExtensions
{
	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that executes on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand Command(
		this SqlConnection connection,
		string command, CommandType type = CommandType.Text)
		=> new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that executes within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand Command(
		this SqlTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure on the given connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure within the given transaction.
	/// </summary>
	/// <param name="transaction">The transaction to execute in. The command runs on the transaction's <see cref="IDbTransaction.Connection"/>.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that creates a new connection from a connection factory for every execution.
	/// </summary>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, that creates a new connection from a connection factory for every execution.
	/// </summary>
	/// <param name="connectionSource">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionSource">The pool to take connections from. Each execution takes one, opens it if needed, and gives it back when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that invokes a function to create a new connection for every execution.
	/// </summary>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="type">The command type. The default is <see cref="CommandType.Text"/>.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand Command(
		this Func<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> Command(DbConnectionFactory.Create(connectionSource), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, that invokes a function to create a new connection for every execution.
	/// </summary>
	/// <param name="connectionSource">The function that creates the connections. Each execution invokes it, opens the connection if needed, and disposes it when finished.</param>
	/// <param name="procedureName">The stored procedure name.</param>
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="procedureName"/> is empty or white space.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this Func<SqlConnection> connectionSource,
		string procedureName)
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);
}
