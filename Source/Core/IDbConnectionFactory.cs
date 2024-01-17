using System;
using System.Data;

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
	class PoolFromFactory(IDbConnectionFactory connectionFactory) : IDbConnectionPool
	{
		private readonly IDbConnectionFactory _connectionFactory
			= connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));

		public IDbConnection Take()
			=> _connectionFactory.Create();

		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	class PoolFromFactory<TConnection>(IDbConnectionFactory<TConnection> connectionFactory) : IDbConnectionPool<TConnection>
		where TConnection : IDbConnection
	{
		private readonly IDbConnectionFactory<TConnection> _connectionFactory
			= connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));

		public TConnection Take()
			=> _connectionFactory.Create();

		IDbConnection IDbConnectionPool.Take()
			=> Take();

		public void Give(IDbConnection connection)
			=> connection.Dispose();
	}

	/// <summary>
	/// Provides a connection pool that simply creates from a connection factory and disposes when returned.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns></returns>
	public static IDbConnectionPool AsPool(this IDbConnectionFactory connectionFactory)
		=> new PoolFromFactory(connectionFactory);

	/// <summary>
	/// Provides a connection pool that simply creates from a connection factory and disposes when returned.
	/// </summary>
	/// <param name="connectionFactory">The connection factory to generate connections from.</param>
	/// <returns></returns>
	public static IDbConnectionPool<TConnection> AsPool<TConnection>(this IDbConnectionFactory<TConnection> connectionFactory)
		where TConnection : IDbConnection
		=> new PoolFromFactory<TConnection>(connectionFactory);

	/// <summary>
	/// Coerces a non-generic connection factory to a generic one.
	/// </summary>
	/// <param name="connectionFactory">The source connection factory.</param>
	/// <returns>The generic version of the source factory.</returns>
	public static IDbConnectionFactory<IDbConnection> AsGeneric(this IDbConnectionFactory connectionFactory)
		=> (connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory))) is IDbConnectionFactory<IDbConnection> p ? p
		: new DbConnectionFactory<IDbConnection>(() => connectionFactory.Create());
}
