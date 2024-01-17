namespace Open.Database.Extensions;

/// <summary>
/// Default SqlConnectionFactory for generating SqlConnections.
/// </summary>
public class SqlConnectionFactory : DbConnectionFactory<SqlConnection>
{
	/// <summary>
	/// Default injectable connection factory constructor.
	/// </summary>
	/// <param name="factory">The factory that generates the connections.</param>
	public SqlConnectionFactory(Func<SqlConnection> factory) : base(factory)
	{
	}

	/// <summary>
	/// Default injectable connection factory constructor that accepts a connection string.
	/// </summary>
	/// <param name="connectionString">Required connection string value.</param>
	public SqlConnectionFactory(string connectionString) : base(() => new SqlConnection(connectionString))
	{
	}
}
