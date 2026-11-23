namespace Open.Database.Extensions;

/// <summary>
/// Extension methods that create an <see cref="ExpressiveSqlCommand"/> or a <see cref="SqlCommand"/> from a connection, transaction, connection factory, connection pool, or factory function.
/// </summary>
/// <remarks>
/// <para>
/// The <c>Command</c> and <c>StoredProcedure</c> extension methods are the entry points to the expressive API for SQL Server.
/// They create an <see cref="ExpressiveSqlCommand"/> from a <see cref="SqlConnection"/>, a <see cref="SqlTransaction"/>, a connection factory, a connection pool, or a factory function.
/// Creating the command does no work; the database is first touched when an execute method is called on it.
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
public static partial class SqlConnectionExtensions
{
	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that executes on the given connection.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-connection']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='command-text']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="StoredProcedure(SqlConnection, string)"/>
	public static ExpressiveSqlCommand Command(
		this SqlConnection connection,
		string command, CommandType type = CommandType.Text)
		=> new(connection, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that executes within the given transaction.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-transaction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='command-text']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand Command(
		this SqlTransaction transaction,
		string command, CommandType type = CommandType.Text)
		=> new(transaction, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure on the given connection.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-connection']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlConnection connection,
		string procedureName)
		=> new(connection, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure within the given transaction.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-transaction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this SqlTransaction transaction,
		string procedureName)
		=> new(transaction, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that creates a new connection from a connection factory for every execution.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-factory']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='command-text']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, creating a new connection from a connection factory for every execution.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-factory']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionFactory<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that takes its connections from a connection pool.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-pool']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='command-text']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand Command(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> new(connectionSource, type, command);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, taking its connections from a connection pool.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-pool']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this IDbConnectionPool<SqlConnection> connectionSource,
		string procedureName)
		=> new(connectionSource, CommandType.StoredProcedure, procedureName);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that invokes a function to create a new connection for every execution.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-func']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='command-text']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand Command(
		this Func<SqlConnection> connectionSource,
		string command,
		CommandType type = CommandType.Text)
		=> Command(DbConnectionFactory.Create(connectionSource), command, type);

	/// <summary>
	/// Creates an <see cref="ExpressiveSqlCommand"/> that calls a stored procedure, invoking a function to create a new connection for every execution.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='src-func']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='procedure']/*" />
	/// <returns>A new <see cref="ExpressiveSqlCommand"/> with <see cref="CommandType.StoredProcedure"/> to configure with parameters and then execute.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionSource"/> or <paramref name="procedureName"/> is <see langword="null"/>.</exception>
	public static ExpressiveSqlCommand StoredProcedure(
		this Func<SqlConnection> connectionSource,
		string procedureName)
		=> StoredProcedure(DbConnectionFactory.Create(connectionSource), procedureName);
}
