namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <returns>The prior connection state.</returns>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	public static ConnectionState EnsureOpen(this IDbConnection connection)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		ConnectionState state = connection.State;

		if ((state & ConnectionState.Broken) != 0)
			connection.Close();

		if ((connection.State & ConnectionState.Open) == 0)
			connection.Open();

		return state;
	}

	/// <summary>
	/// If the connection isn't open, opens the connection.<br/>
	/// If the connection is in neither open or close, first closes the connection, then opens it.<br/>
	/// Always awaits with <c>ConfigureAwait(false)</c>: the caller decides whether to return to its context.
	/// </summary>
	/// <param name="connection">The connection to transact with.</param>
	/// <param name="cancellationToken">An cancellation token to cancel opening.</param>
	/// <returns>A task containing the prior connection state.</returns>
	public static async ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();

		ConnectionState state = connection.State;
		if ((state & ConnectionState.Broken) != 0)
			connection.Close();

		if ((connection.State & ConnectionState.Open) != 0)
			return state;

		await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

		// ConnectionState.Closed is zero, so HasFlag(Closed) is always true: test the state itself.
		if (cancellationToken.IsCancellationRequested && connection.State != ConnectionState.Closed)
		{
			connection.Close(); // Fake finally...
			cancellationToken.ThrowIfCancellationRequested();
		}

		return state;
	}

	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	public static async ValueTask<ConnectionState> EnsureOpenAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is DbConnection c)
			return await c.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);

		cancellationToken.ThrowIfCancellationRequested();
		return connection.EnsureOpen();
	}

	/// <summary>
	/// If the <paramref name="connection"/> derives from <see cref="DbConnection"/>, this will call <see cref="DbConnection.OpenAsync(CancellationToken)"/>;
	/// otherwise it will call <see cref="IDbConnection.Open()"/>.
	/// </summary>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	public static async ValueTask OpenAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken = default)
	{
		if (connection is DbConnection c)
			await c.OpenAsync(cancellationToken).ConfigureAwait(false);
		else
			connection.Open();
	}

	const string ObsoleteMessage = "The library always uses ConfigureAwait(false), so this argument is ignored. Call the overload without it.";

	/// <summary>The <paramref name="configureAwait"/> argument is ignored: the library always uses <c>ConfigureAwait(false)</c>.</summary>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	[Obsolete(ObsoleteMessage)]
	public static ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.EnsureOpenAsync(cancellationToken);

	/// <summary>The <paramref name="configureAwait"/> argument is ignored: the library always uses <c>ConfigureAwait(false)</c>.</summary>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	[Obsolete(ObsoleteMessage)]
	public static ValueTask<ConnectionState> EnsureOpenAsync(
		this IDbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.EnsureOpenAsync(cancellationToken);

	/// <summary>The <paramref name="configureAwait"/> argument is ignored: the library always uses <c>ConfigureAwait(false)</c>.</summary>
	/// <inheritdoc cref="OpenAsync(IDbConnection, CancellationToken)"/>
	[Obsolete(ObsoleteMessage)]
	public static ValueTask OpenAsync(
		this IDbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.OpenAsync(cancellationToken);
}
