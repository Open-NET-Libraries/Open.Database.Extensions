using System.Data;

namespace Open.Database.Extensions
{
    /// <summary>
    /// Simplified interface with IDbConnection as the generic type.
    /// </summary>
    public interface IDbConnectionFactory
    {
        /// <summary>
        /// Generates a new connection of declared generic type.
        /// </summary>
        /// <returns></returns>
        IDbConnection Create();
    }

    /// <summary>
    /// Base interface for creating connections.
    /// Useful for dependency injection.
    /// </summary>
    /// <typeparam name="TConnection">The actual connection type.</typeparam>
    public interface IDbConnectionFactory<out TConnection> : IDbConnectionFactory
        where TConnection : IDbConnection
    {
		/// <summary>
		/// Generates a new connection of declared generic type.
		/// </summary>
		/// <returns></returns>
		new TConnection Create();
    }


}