namespace Open.Database.Extensions.Core;

/// <summary>
/// Simplifies handling connections.
/// </summary>
internal class DbConnectionProvider<TConnection>(TConnection connection)
	: IDbConnectionPool<TConnection>
	where TConnection : class, IDbConnection
{
	private TConnection Connection { get; } = connection ?? throw new ArgumentNullException(nameof(connection));

	private ConnectionState? _takenConnectionState;

	/// <inheritdoc />
	public TConnection Take()
	{
		if (_takenConnectionState.HasValue)
			throw new InvalidOperationException("Concurrent use of a single connection is not supported.");

		_takenConnectionState = Connection.State;
		return Connection;
	}

	IDbConnection IDbConnectionPool.Take() => Take();

	/// <inheritdoc />
	public void Give(IDbConnection connection)
	{
		if (connection is null)
			throw new ArgumentNullException(nameof(connection));
		if (connection != Connection)
			throw new ArgumentException("Does not belong to this provider.", nameof(connection));
		Contract.EndContractBlock();

		if (_takenConnectionState == ConnectionState.Closed)
			connection.Close();

		_takenConnectionState = null;
	}
}

internal static class DbConnectionProvider
{
	public static DbConnectionProvider<TConnection> Create<TConnection>(TConnection connection)
		where TConnection : class, IDbConnection
		=> new(connection);
}
