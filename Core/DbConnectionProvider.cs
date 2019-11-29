using System;
using System.Data;
using System.Diagnostics.Contracts;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Simplifies handling connections.
	/// </summary>
	/// <typeparam name="TConnection"></typeparam>
	class DbConnectionProvider<TConnection> : IDbConnectionPool<TConnection>
		where TConnection : class, IDbConnection
	{
		public DbConnectionProvider(TConnection connection)
		{
			Connection = connection ?? throw new ArgumentNullException(nameof(connection));
		}

		private TConnection Connection { get; }

		private ConnectionState? TakenConnectionState;

		public TConnection Take()
		{
			if (TakenConnectionState.HasValue)
				throw new InvalidOperationException("Concurrent use of a single connection is not supported.");

			TakenConnectionState = Connection.State;
			return Connection;
		}

		IDbConnection IDbConnectionPool.Take() => Take();

		public void Give(IDbConnection connection)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));
			if (connection != Connection) throw new ArgumentException("Does not belong to this provider.", nameof(connection));
			Contract.EndContractBlock();

			if (TakenConnectionState == ConnectionState.Closed)
				connection.Close();

			TakenConnectionState = null;
		}
	}
}
