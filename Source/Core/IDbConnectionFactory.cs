namespace Open.Database.Extensions;

/// <summary>
/// A common abstraction for creating database connections. Can easily be used with dependency injection.
/// </summary>
/// <remarks>
/// <para>
/// The factory never reuses connections: every call to <see cref="Create"/> returns a new one that the caller owns.
/// Use <see cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/> to present a factory as an
/// <see cref="IDbConnectionPool"/>, which is how the expressive commands consume it.
/// </para>
/// <para>
/// A factory is the usual connection source.
/// A custom <see cref="IDbConnectionPool"/> is only for a pooling scheme of your own.
/// </para>
/// </remarks>
/// <seealso cref="IDbConnectionFactory{TConnection}"/>
/// <seealso cref="DbConnectionFactory"/>
/// <seealso cref="IDbConnectionPool"/>
// The provider's own connection pool does the pooling behind each connection a factory creates, so there is nothing for ODX to pool.
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
/// Identifies what a pool made from a factory (see <see cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/>) stands for.
/// </summary>
interface IConnectionIdentity
{
	/// <summary>
	/// Gets the factory object itself, which remembered names belong to.
	/// </summary>
	object Identity { get; }
}

/// <summary>
/// Extensions for presenting a connection factory as a pool or as a generic factory.
/// </summary>
public static class DbConnectionFactoryExtensions
{
	/// <summary>
	/// Presents a connection factory function as an <see cref="IDbConnectionPool"/>: taking a connection invokes the function and giving it back disposes it.
	/// </summary>
	/// <remarks>
	/// Nothing is pooled. This adapter lets code written against <see cref="IDbConnectionPool"/> accept a plain factory.
	/// </remarks>
	/// <param name="factory">The function that creates a new connection each time one is taken.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	/// <seealso cref="AsPool(IDbConnectionFactory)"/>
	internal readonly struct ConnectionFactoryToPoolAdapter(Func<IDbConnection> factory) : IDbConnectionPool, IConnectionIdentity
	{
		readonly Func<IDbConnection> _factory = factory ?? throw new ArgumentNullException(nameof(factory));

		/// <summary>
		/// Constructs a <see cref="ConnectionFactoryToPoolAdapter"/> from a connection factory.
		/// </summary>
		/// <param name="factory">The factory whose <see cref="IDbConnectionFactory.Create"/> method creates each connection.</param>
		/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory factory)
			: this((factory ?? throw new ArgumentNullException(nameof(factory))).Create) { }

		/// <inheritdoc />
		/// <remarks>Creates a new connection by invoking the factory function.</remarks>
		public IDbConnection Take()
			=> _factory();

		/// <inheritdoc cref="ConnectionFactoryToPoolAdapter{TConnection}.Identity"/>
		public object Identity => _factory.Target ?? _factory;

		object IConnectionIdentity.Identity => Identity;

		/// <inheritdoc />
		/// <remarks>Disposes <paramref name="connection"/>; nothing is retained for reuse.</remarks>
		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <summary>
	/// Presents a typed connection factory function as an <see cref="IDbConnectionPool{TConnection}"/>: taking a connection invokes the function and giving it back disposes it.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <remarks>
	/// Nothing is pooled. This adapter lets code written against <see cref="IDbConnectionPool{TConnection}"/> accept a plain factory.
	/// </remarks>
	/// <param name="factory">The function that creates a new connection each time one is taken.</param>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	/// <seealso cref="AsPool{TConnection}(IDbConnectionFactory{TConnection})"/>
	internal readonly struct ConnectionFactoryToPoolAdapter<TConnection>(Func<TConnection> factory) : IDbConnectionPool<TConnection>, IConnectionIdentity
		where TConnection : IDbConnection
	{
		readonly Func<TConnection> _factory = factory ?? throw new ArgumentNullException(nameof(factory));

		/// <summary>
		/// Constructs a <see cref="ConnectionFactoryToPoolAdapter{TConnection}"/> from a typed connection factory.
		/// </summary>
		/// <param name="factory">The factory whose <see cref="IDbConnectionFactory{TConnection}.Create"/> method creates each connection.</param>
		/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
		public ConnectionFactoryToPoolAdapter(IDbConnectionFactory<TConnection> factory)
			: this((factory ?? throw new ArgumentNullException(nameof(factory))).Create) { }

		/// <inheritdoc />
		/// <remarks>Creates a new connection by invoking the factory function.</remarks>
		public TConnection Take()
			=> _factory();

		/// <summary>
		/// Gets what stays the same from one command to the next: the factory object itself
		/// (the target of its <c>Create</c> method), which is what remembered procedure names belong to.
		/// </summary>
		/// <remarks>
		/// A function with no target (a <see langword="static"/> lambda) has no such identity, and its own delegate is returned.
		/// </remarks>
		public object Identity => _factory.Target ?? _factory;

		object IConnectionIdentity.Identity => Identity;

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
	/// <returns>An <see cref="IDbConnectionPool"/> that handles this factory.</returns>
	/// <include file="Extensions/Connections.docs.xml" path="docs/members[@name='as-pool-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static IDbConnectionPool AsPool(this IDbConnectionFactory connectionFactory)
		// The adapter constructor throws ArgumentNullException for a null source.
		=> new ConnectionFactoryToPoolAdapter(connectionFactory);

	/// <summary>
	/// Presents a connection factory function as a connection pool that invokes the function when a connection is taken and disposes the connection when it is given back.
	/// </summary>
	/// <param name="connectionFactory">The function that creates a connection.</param>
	/// <returns>An <see cref="IDbConnectionPool"/> that handles this function.</returns>
	/// <include file="Extensions/Connections.docs.xml" path="docs/members[@name='as-pool-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static IDbConnectionPool AsPool(this Func<IDbConnection> connectionFactory)
		// The adapter constructor throws ArgumentNullException for a null source.
		=> new ConnectionFactoryToPoolAdapter(connectionFactory);

	/// <summary>
	/// Presents a typed connection factory as a connection pool that creates a new connection when one is taken and disposes it when it is given back.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns>An <see cref="IDbConnectionPool{TConnection}"/> that handles this factory.</returns>
	/// <include file="Extensions/Connections.docs.xml" path="docs/members[@name='as-pool-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static IDbConnectionPool<TConnection> AsPool<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory)
		where TConnection : IDbConnection
		// The adapter constructor throws ArgumentNullException for a null source.
		=> new ConnectionFactoryToPoolAdapter<TConnection>(connectionFactory);

	/// <summary>
	/// Presents a typed connection factory function as a connection pool that invokes the function when a connection is taken and disposes the connection when it is given back.
	/// </summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="connectionFactory">The function that creates a connection.</param>
	/// <returns>An <see cref="IDbConnectionPool{TConnection}"/> that handles this function.</returns>
	/// <include file="Extensions/Connections.docs.xml" path="docs/members[@name='as-pool-remarks']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="connectionFactory"/> is <see langword="null"/>.</exception>
	public static IDbConnectionPool<TConnection> AsPool<TConnection>(this Func<TConnection> connectionFactory)
		where TConnection : IDbConnection
		// The adapter constructor throws ArgumentNullException for a null source.
		=> new ConnectionFactoryToPoolAdapter<TConnection>(connectionFactory);

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
