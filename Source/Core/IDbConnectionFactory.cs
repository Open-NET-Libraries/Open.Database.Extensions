namespace Open.Database.Extensions;

/// <summary>
/// Common interface for creating a connection.  Can easily be used with dependency injection.
/// </summary>
public interface IDbConnectionFactory
{
	/// <summary>
	/// Creates a new connection ready for use.
	/// </summary>
	/// <returns>An IDbConnection.</returns>
	IDbConnection Create();
}

/// <inheritdoc />
/// <typeparam name="TConnection">The actual connection type.</typeparam>
public interface IDbConnectionFactory<out TConnection> : IDbConnectionFactory
	where TConnection : IDbConnection
{
	/// <inheritdoc />
	/// <returns>An connection of type <typeparamref name="TConnection"/>.</returns>
	new TConnection Create();
}

/// <summary>
/// Extensions for converting a connection factory into a pool.
/// </summary>
public static class DbConnectionFactoryExtensions
{
	/// <summary>
	/// A struct that represents a connection factory that can be used as a pool.
	/// </summary>
	public readonly struct ConnectionFactoryToPoolAdapter(Func<IDbConnection> factory) : IDbConnectionPool
	{
		/// <summary>
		/// Constructs a connection factory to pool adapter.
		/// </summary>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory factory)
			: this(factory.Create) { }

		/// <inheritdoc />
		public IDbConnection Take()
			=> factory();

		/// <inheritdoc />
		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <inheritdoc cref="ConnectionFactoryToPoolAdapter(Func{IDbConnection})" />/>
	public readonly struct ConnectionFactoryToPoolAdapter<TConnection>(Func<TConnection> factory) : IDbConnectionPool<TConnection>
		where TConnection : IDbConnection
	{
		/// <summary>
		/// Constructs a connection factory to pool adapter.
		/// </summary>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory<TConnection> factory)
			: this(factory.Create) { }

		/// <inheritdoc />
		public TConnection Take()
			=> factory();

		IDbConnection IDbConnectionPool.Take()
			=> Take();

		/// <inheritdoc />
		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <summary>
	/// Provides a connection pool that simply creates from a connection factory and disposes when returned.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns>An <see cref="ConnectionFactoryToPoolAdapter"/> to handle this factory.</returns>
	public static ConnectionFactoryToPoolAdapter AsPool(this IDbConnectionFactory connectionFactory)
		=> new (connectionFactory);

	/// <summary>
	/// Provides a connection pool that simply creates from a connection factory and disposes when returned.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns>An <see cref="ConnectionFactoryToPoolAdapter{TConnection}"/> to handle this factory.</returns>
	public static ConnectionFactoryToPoolAdapter<TConnection> AsPool<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory)
		where TConnection : IDbConnection
		=> new (connectionFactory);

	/// <summary>
	/// Coerces a non-generic connection factory to a generic one.
	/// </summary>
	/// <param name="connectionFactory">The source connection factory.</param>
	/// <returns>The generic version of the source factory.</returns>
	public static IDbConnectionFactory<IDbConnection> AsGeneric(this IDbConnectionFactory connectionFactory)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return connectionFactory is IDbConnectionFactory<IDbConnection> p ? p
			: new DbConnectionFactory<IDbConnection>(connectionFactory.Create);
	}
}
