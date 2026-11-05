namespace Open.Database.Extensions;

/// <summary>
/// A common abstraction for creating database connections. Can easily be used with dependency injection.
/// </summary>
/// <remarks>
/// The factory never reuses connections: every call to <see cref="Create"/> returns a new one that the caller owns. Use <see cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/> to present a factory as an <see cref="IDbConnectionPool"/>, which is how the expressive commands consume it.
/// </remarks>
/// <seealso cref="IDbConnectionFactory{TConnection}"/>
/// <seealso cref="DbConnectionFactory"/>
/// <seealso cref="IDbConnectionPool"/>
public interface IDbConnectionFactory
{
	/// <summary>
	/// Creates a new connection.
	/// </summary>
	/// <returns>A new <see cref="IDbConnection"/>. The caller owns it and is responsible for disposing it.</returns>
	/// <remarks>
	/// The connection is not necessarily open. The expressive commands open it only if needed and dispose it when the command is finished.
	/// </remarks>
	IDbConnection Create();
}

/// <summary>
/// A strongly typed <see cref="IDbConnectionFactory"/> that creates connections of type <typeparamref name="TConnection"/>.
/// </summary>
/// <typeparam name="TConnection">The actual connection type.</typeparam>
public interface IDbConnectionFactory<out TConnection> : IDbConnectionFactory
	where TConnection : IDbConnection
{
	/// <summary>
	/// Creates a new connection of type <typeparamref name="TConnection"/>.
	/// </summary>
	/// <returns>A new <typeparamref name="TConnection"/>. The caller owns it and is responsible for disposing it.</returns>
	/// <inheritdoc cref="IDbConnectionFactory.Create"/>
	new TConnection Create();
}

/// <summary>
/// Extensions for presenting a connection factory as a pool or as a generic factory.
/// </summary>
public static class DbConnectionFactoryExtensions
{
	/// <summary>
	/// Presents a connection factory function as an <see cref="IDbConnectionPool"/>: taking a connection invokes the function and giving it back disposes it.
	/// </summary>
	/// <param name="factory">The function that creates a new connection each time one is taken.</param>
	/// <remarks>
	/// Nothing is pooled. This adapter lets code written against <see cref="IDbConnectionPool"/> accept a plain factory.
	/// </remarks>
	/// <seealso cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/>
	public readonly struct ConnectionFactoryToPoolAdapter(Func<IDbConnection> factory) : IDbConnectionPool
	{
		/// <summary>
		/// Constructs a <see cref="ConnectionFactoryToPoolAdapter"/> from a connection factory.
		/// </summary>
		/// <param name="factory">The factory whose <see cref="IDbConnectionFactory.Create"/> method creates each connection.</param>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory factory)
			: this(factory.Create) { }

		/// <inheritdoc />
		/// <remarks>Creates a new connection by invoking the factory function.</remarks>
		public IDbConnection Take()
			=> factory();

		/// <inheritdoc />
		/// <remarks>Disposes <paramref name="connection"/>; nothing is retained for reuse.</remarks>
		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <summary>
	/// Presents a typed connection factory function as an <see cref="IDbConnectionPool{TConnection}"/>: taking a connection invokes the function and giving it back disposes it.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="factory">The function that creates a new connection each time one is taken.</param>
	/// <remarks>
	/// Nothing is pooled. This adapter lets code written against <see cref="IDbConnectionPool{TConnection}"/> accept a plain factory.
	/// </remarks>
	/// <seealso cref="DbConnectionFactoryExtensions.AsPool{TConnection}(IDbConnectionFactory{TConnection})"/>
	public readonly struct ConnectionFactoryToPoolAdapter<TConnection>(Func<TConnection> factory) : IDbConnectionPool<TConnection>
		where TConnection : IDbConnection
	{
		/// <summary>
		/// Constructs a <see cref="ConnectionFactoryToPoolAdapter{TConnection}"/> from a typed connection factory.
		/// </summary>
		/// <param name="factory">The factory whose <see cref="IDbConnectionFactory{TConnection}.Create"/> method creates each connection.</param>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory<TConnection> factory)
			: this(factory.Create) { }

		/// <inheritdoc />
		/// <remarks>Creates a new connection by invoking the factory function.</remarks>
		public TConnection Take()
			=> factory();

		IDbConnection IDbConnectionPool.Take()
			=> Take();

		/// <inheritdoc />
		/// <remarks>Disposes <paramref name="connection"/>; nothing is retained for reuse.</remarks>
		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <summary>
	/// Presents a connection factory as a connection pool that creates a new connection when one is taken and disposes it when it is given back.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns>A <see cref="ConnectionFactoryToPoolAdapter"/> that handles this factory.</returns>
	/// <remarks>
	/// This is the "get in, get out" model: every command execution creates its own connection, opens it only if needed, and disposes it when the command is finished.
	/// </remarks>
	public static ConnectionFactoryToPoolAdapter AsPool(this IDbConnectionFactory connectionFactory)
		=> new(connectionFactory);

	/// <summary>
	/// Presents a connection factory function as a connection pool that invokes the function when a connection is taken and disposes the connection when it is given back.
	/// </summary>
	/// <param name="connectionFactory">The function that creates a connection.</param>
	/// <returns>A <see cref="ConnectionFactoryToPoolAdapter"/> that handles this function.</returns>
	/// <remarks>
	/// This is the "get in, get out" model: every command execution creates its own connection, opens it only if needed, and disposes it when the command is finished.
	/// </remarks>
	public static ConnectionFactoryToPoolAdapter AsPool(this Func<IDbConnection> connectionFactory)
		=> new(connectionFactory);

	/// <summary>
	/// Presents a typed connection factory as a connection pool that creates a new connection when one is taken and disposes it when it is given back.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns>A <see cref="ConnectionFactoryToPoolAdapter{TConnection}"/> that handles this factory.</returns>
	/// <remarks>
	/// This is the "get in, get out" model: every command execution creates its own connection, opens it only if needed, and disposes it when the command is finished.
	/// </remarks>
	public static ConnectionFactoryToPoolAdapter<TConnection> AsPool<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory)
		where TConnection : IDbConnection
		=> new(connectionFactory);

	/// <summary>
	/// Presents a typed connection factory function as a connection pool that invokes the function when a connection is taken and disposes the connection when it is given back.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates a connection.</param>
	/// <returns>A <see cref="ConnectionFactoryToPoolAdapter{TConnection}"/> that handles this function.</returns>
	/// <remarks>
	/// This is the "get in, get out" model: every command execution creates its own connection, opens it only if needed, and disposes it when the command is finished.
	/// </remarks>
	public static ConnectionFactoryToPoolAdapter<TConnection> AsPool<TConnection>(this Func<TConnection> connectionFactory)
		where TConnection : IDbConnection
		=> new(connectionFactory);

	/// <summary>
	/// Converts a non-generic connection factory to a generic one.
	/// </summary>
	/// <param name="connectionFactory">The source connection factory.</param>
	/// <returns>
	/// <paramref name="connectionFactory"/> itself if it already implements <see cref="IDbConnectionFactory{TConnection}"/> of <see cref="IDbConnection"/>; otherwise a <see cref="DbConnectionFactory{TConnection}"/> that creates connections through it.
	/// </returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static IDbConnectionFactory<IDbConnection> AsGeneric(this IDbConnectionFactory connectionFactory)
	{
		if (connectionFactory is null) throw new ArgumentNullException(nameof(connectionFactory));
		Contract.EndContractBlock();

		return connectionFactory is IDbConnectionFactory<IDbConnection> p ? p
			: new DbConnectionFactory<IDbConnection>(connectionFactory.Create);
	}
}
