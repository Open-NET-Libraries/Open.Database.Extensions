namespace Open.Database.Extensions;

/// <summary>
/// A connection factory that creates a new <see cref="SqlConnection"/> each time it is asked, either from a factory function or from a connection string.
/// </summary>
/// <remarks>
/// The factory holds no connections. The caller of <c>Create</c> owns the connection it receives and disposes it; the expressive commands do this automatically.
/// </remarks>
/// <seealso cref="DbConnectionFactory{TConnection}"/>
public class SqlConnectionFactory : DbConnectionFactory<SqlConnection>
{
	/// <summary>
	/// Constructs a factory that invokes a function to create each connection.
	/// </summary>
	/// <param name="factory">The function that creates a new connection each time it is called.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	public SqlConnectionFactory(Func<SqlConnection> factory) : base(factory)
	{
	}

	/// <summary>
	/// Constructs a factory that creates a <see cref="SqlConnection"/> from a connection string each time it is asked.
	/// </summary>
	/// <param name="connectionString">The connection string used for every connection the factory creates.</param>
	public SqlConnectionFactory(string connectionString) : base(() => new SqlConnection(connectionString))
	{
	}
}
