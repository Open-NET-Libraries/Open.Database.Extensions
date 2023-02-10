using System;
using System.Data;

namespace Open.Database.Extensions;

/// <summary>
/// A unifying common interface for creating/managing connections. Can easily be
/// used with dependency injection. Commonly a pool will simply host a single
/// connection for nonconcurrent operations where giving back to the pool
/// guarantees the connection returns to the state it was in before it was
/// taken. Connection factories can pose as pools where taking always creates a
/// new connection, and giving back always disposes.
/// </summary>
public interface IDbConnectionPool
{
	/// <summary>
	/// Provides a connection ready for use.  The connection state may or may not be closed depending on how the pool is being used.
	/// </summary>
	/// <returns>An <see cref="IDbConnection"/>.</returns>
	IDbConnection Take();

	/// <summary>
	/// Gives the connection to the pool.
	/// Depending on implementation,
	/// the pool could be full,
	/// and the connection disposed of immediately.
	/// </summary>
	/// <param name="connection">The connection to be received by the pool.</param>
	void Give(IDbConnection connection);
}

/// <inheritdoc />
/// <typeparam name="TConnection">The actual connection type.</typeparam>
public interface IDbConnectionPool<out TConnection> : IDbConnectionPool
	where TConnection : IDbConnection
{
	/// <inheritdoc />
	/// <returns>An connection of type <typeparamref name="TConnection"/>.</returns>
	new TConnection Take();
}

/// <summary>
/// Extensions for getting generic versions on non-generic connection pools..
/// </summary>
public static class ConnectionPoolExtensions
{
	class GenericPool : IDbConnectionPool<IDbConnection>
	{
		private readonly IDbConnectionPool _source;

		public GenericPool(IDbConnectionPool source)
		{
			_source = source ?? throw new ArgumentNullException(nameof(source));
		}

		public IDbConnection Take()
			=> _source.Take();

		IDbConnection IDbConnectionPool.Take()
			=> _source.Take();

		public void Give(IDbConnection connection)
			=> _source.Give(connection);
	}

	/// <summary>
	/// Converts a non-generic connection factory to a generic one.
	/// </summary>
	/// <param name="connectionPool">The source connection factory.</param>
	/// <returns>The generic version of the source factory.</returns>
	public static IDbConnectionPool<IDbConnection> AsGeneric(this IDbConnectionPool connectionPool)
		=> connectionPool is IDbConnectionPool<IDbConnection> p ? p : new GenericPool(connectionPool);
}
