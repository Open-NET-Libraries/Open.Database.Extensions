namespace Open.Database.Extensions;

/// <summary>
/// A common abstraction for acquiring and releasing database connections. Can easily be used with dependency injection.
/// </summary>
/// <remarks>
/// <para>
/// Every <see cref="Take"/> is paired with exactly one <see cref="Give(IDbConnection)"/>. The expressive commands (for example <see cref="ExpressiveCommand"/>) call <see cref="Give(IDbConnection)"/> in a <see langword="finally"/> block, so a connection is handed back even when the command fails.
/// </para>
/// <para>
/// Implementations decide what giving a connection back means. A pool that hosts a single connection for non-concurrent operations returns the connection to the state it was in when it was taken (a connection that was closed is closed again). A connection factory can pose as a pool through <see cref="DbConnectionFactoryExtensions.AsPool(IDbConnectionFactory)"/>, in which case taking always creates a new connection and giving back always disposes it.
/// </para>
/// </remarks>
/// <seealso cref="IDbConnectionPool{TConnection}"/>
/// <seealso cref="IDbConnectionFactory"/>
/// <seealso cref="ConnectionPoolExtensions.AsGeneric(IDbConnectionPool)"/>
public interface IDbConnectionPool
{
	/// <summary>
	/// Provides a connection that is ready to be handed to a command.
	/// </summary>
	/// <returns>An <see cref="IDbConnection"/> that the caller hands back with <see cref="Give(IDbConnection)"/> when finished.</returns>
	/// <remarks>
	/// The connection is not necessarily open. Callers that execute commands open it only if needed (see <see cref="ConnectionExtensions.EnsureOpen(IDbConnection)"/>) and put it back in its original state when they are done.
	/// </remarks>
	/// <seealso cref="Give(IDbConnection)"/>
	IDbConnection Take();

	/// <summary>
	/// Gives a connection back to the pool.
	/// </summary>
	/// <param name="connection">The connection previously provided by <see cref="Take"/>.</param>
	/// <remarks>
	/// Depending on the implementation, the pool could be full and the connection disposed of immediately, or the pool could restore the connection to the state it was in when it was taken.
	/// </remarks>
	/// <seealso cref="Take"/>
	void Give(IDbConnection connection);
}

/// <summary>
/// A strongly typed <see cref="IDbConnectionPool"/> that provides connections of type <typeparamref name="TConnection"/>.
/// </summary>
/// <typeparam name="TConnection">The actual connection type.</typeparam>
/// <seealso cref="IDbConnectionPool"/>
public interface IDbConnectionPool<out TConnection> : IDbConnectionPool
	where TConnection : IDbConnection
{
	/// <summary>
	/// Provides a connection of type <typeparamref name="TConnection"/> that is ready to be handed to a command.
	/// </summary>
	/// <returns>A <typeparamref name="TConnection"/> that the caller hands back with <see cref="IDbConnectionPool.Give(IDbConnection)"/> when finished.</returns>
	/// <inheritdoc cref="IDbConnectionPool.Take"/>
	new TConnection Take();
}

/// <summary>
/// Extensions for converting a non-generic connection pool into a generic one.
/// </summary>
public static class ConnectionPoolExtensions
{
	class GenericPool(IDbConnectionPool source) : IDbConnectionPool<IDbConnection>
	{
		private readonly IDbConnectionPool _source
			= source ?? throw new ArgumentNullException(nameof(source));

		public IDbConnection Take()
			=> _source.Take();

		IDbConnection IDbConnectionPool.Take()
			=> _source.Take();

		public void Give(IDbConnection connection)
			=> _source.Give(connection);
	}

	/// <summary>
	/// Converts a non-generic connection pool to a generic one.
	/// </summary>
	/// <param name="connectionPool">The source connection pool.</param>
	/// <returns>
	/// <paramref name="connectionPool"/> itself if it already implements <see cref="IDbConnectionPool{TConnection}"/> of <see cref="IDbConnection"/>; otherwise a wrapper that forwards <see cref="IDbConnectionPool.Take"/> and <see cref="IDbConnectionPool.Give(IDbConnection)"/> to it.
	/// </returns>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> is <see langword="null"/>.</exception>
	public static IDbConnectionPool<IDbConnection> AsGeneric(this IDbConnectionPool connectionPool)
		=> connectionPool is IDbConnectionPool<IDbConnection> p ? p : new GenericPool(connectionPool);
}
