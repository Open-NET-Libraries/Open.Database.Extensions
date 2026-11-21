namespace Open.Database.Extensions.Core;

/// <summary>
/// Hands out one connection and takes it back as it was found.
/// </summary>
/// <remarks>
/// <see cref="Give(IDbConnection)"/> closes the connection when it needed
/// opening at the time of <see cref="Take"/>, so whoever opened it for the
/// caller closes it again.
/// The connection can be taken by one caller at a time.
/// </remarks>
/// <typeparam name="TConnection">The type of the connection.</typeparam>
/// <param name="connection">The connection to hand out.</param>
internal class DbConnectionProvider<TConnection>(TConnection connection)
	: IDbConnectionPool<TConnection>
	where TConnection : class, IDbConnection
{
	private TConnection Connection { get; } = connection ?? throw new ArgumentNullException(nameof(connection));

	private ConnectionState? _takenConnectionState;

	/// <inheritdoc />
	/// <exception cref="InvalidOperationException">
	/// The connection was taken and not given back.
	/// </exception>
	public TConnection Take()
	{
		if (_takenConnectionState.HasValue)
			throw new InvalidOperationException("Concurrent use of a single connection is not supported.");

		_takenConnectionState = Connection.State;
		return Connection;
	}

	IDbConnection IDbConnectionPool.Take() => Take();

	/// <inheritdoc />
	/// <exception cref="ArgumentNullException">
	/// <paramref name="connection"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="ArgumentException">
	/// <paramref name="connection"/> is not the connection this provider hands out.
	/// </exception>
	public void Give(IDbConnection connection)
	{
		if (connection is null)
			throw new ArgumentNullException(nameof(connection));
		if (connection != Connection)
			throw new ArgumentException("Does not belong to this provider.", nameof(connection));
		Contract.EndContractBlock();

		// Not usable as it was found: whoever opened it for the caller closes it again.
		if (_takenConnectionState is { } taken
			&& ConnectionExtensions.NeedsOpen(taken))
		{
			connection.Close();
		}

		_takenConnectionState = null;
	}
}

/// <summary>
/// Creates <see cref="DbConnectionProvider{TConnection}"/> instances.
/// </summary>
internal static class DbConnectionProvider
{
	/// <summary>
	/// Creates a provider that hands out <paramref name="connection"/>.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="connection">The connection to hand out.</param>
	/// <returns>A new <see cref="DbConnectionProvider{TConnection}"/>.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="connection"/> is <see langword="null"/>.
	/// </exception>
	public static DbConnectionProvider<TConnection> Create<TConnection>(TConnection connection)
		where TConnection : class, IDbConnection
		=> new(connection);
}
