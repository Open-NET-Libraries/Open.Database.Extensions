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
}
