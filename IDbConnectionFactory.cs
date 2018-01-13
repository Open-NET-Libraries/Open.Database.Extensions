using System.Data;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Base interface for creating connections.
	/// Useful for dependency injection.
	/// </summary>
	/// <typeparam name="TConnection">The actual connection type.</typeparam>
    public interface IDbConnectionFactory<out TConnection>
        where TConnection : IDbConnection
    {
		/// <summary>
		/// Generates a new connection of declared generic type.
		/// </summary>
		/// <returns></returns>
		TConnection Create();
    }

	/// <summary>
	/// Simplified interface with IDbConnection as the generic type.
	/// </summary>
    public interface IDbConnectionFactory
		: IDbConnectionFactory<IDbConnection>
	{
		
    }
}