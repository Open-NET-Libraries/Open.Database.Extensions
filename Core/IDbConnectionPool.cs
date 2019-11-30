using System;
using System.Data;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Simplified interface with IDbConnection as the generic type.
	/// </summary>
	public interface IDbConnectionPool
	{
		/// <summary>
		/// Provides a connection of declared generic type.
		/// </summary>
		/// <returns>An IDbConnection.</returns>
		IDbConnection Take();

		/// <summary>
		/// Gives the connection to the pool.
		/// Depending on implementation, the pool could be full, and the connection disposed of immediately.
		/// </summary>
		/// <param name="connection">The connection to be received by the pool.</param>
		void Give(IDbConnection connection);
	}

	/// <summary>
	/// Base interface for creating connections.
	/// Useful for dependency injection.
	/// </summary>
	/// <typeparam name="TConnection">The actual connection type.</typeparam>
	public interface IDbConnectionPool<TConnection> : IDbConnectionPool
		where TConnection : IDbConnection
	{
		/// <summary>
		/// Generates a new connection of declared generic type.
		/// </summary>
		/// <returns>An connection of type <typeparamref name="TConnection"/>.</returns>
		new TConnection Take();
	}

	/// <summary>
	/// Extensions for getting generic versions on non-generic connection pools..
	/// </summary>
	public static class ConnectionPoolExtensions {

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
		/// Coerces a non-generic connection factory to a generic one.
		/// </summary>
		/// <param name="connectionPool">The source connection factory.</param>
		/// <returns>The generic version of the source factory.</returns>
		public static IDbConnectionPool<IDbConnection> AsGeneric(this IDbConnectionPool connectionPool)
			=> connectionPool is IDbConnectionPool<IDbConnection> p ? p : new GenericPool(connectionPool);
	}
}
