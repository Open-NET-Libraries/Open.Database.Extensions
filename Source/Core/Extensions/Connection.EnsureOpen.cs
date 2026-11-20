namespace Open.Database.Extensions;

public static partial class ConnectionExtensions
{
	/// <summary>
	/// True when a connection found in this state has to be opened by us
	/// (see <see cref="EnsureOpen(IDbConnection)"/>), so whoever opens it
	/// closes it again afterwards.
	/// </summary>
	/// <remarks>
	/// <see cref="ConnectionState"/> is a <see cref="FlagsAttribute">[Flags]</see>
	/// enum: Closed = 0, Open = 1, Connecting = 2, Executing = 4,
	/// Fetching = 8, Broken = 16. An open connection that is busy is
	/// <c>Open | Executing</c> or <c>Open | Fetching</c>, which is why the
	/// Open <em>bit</em> is tested and the state is never compared for equality.
	/// Closed is zero, so it has no bit: it is found as "the Open bit is not set".
	/// </remarks>
	internal static bool NeedsOpen(ConnectionState state)
		// Open bit not set: the connection is closed (or still connecting), so it has to be opened.
		=> (state & ConnectionState.Open) == 0
		// Broken bit set: the connection was open and broke; ADO.NET says it may be closed and re-opened,
		// so it is closed first, then opened.
		|| IsBroken(state);

	// The Broken bit. Kept in one place so that the check that closes a broken connection first
	// and the one that decides it needs opening cannot disagree.
	static bool IsBroken(ConnectionState state)
		=> (state & ConnectionState.Broken) != 0;

	/// <returns>The prior connection state.</returns>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	public static ConnectionState EnsureOpen(this IDbConnection connection)
	{
		if (connection is null) throw new ArgumentNullException(nameof(connection));
		Contract.EndContractBlock();

		ConnectionState state = connection.State;

		// Already open: nothing to do, and nobody should close it.
		if (!NeedsOpen(state)) return state;

		// Broken: it is closed first, then opened.
		if (IsBroken(state))
			connection.Close();

		connection.Open();
		return state;
	}

	/// <summary>
	/// Opens the connection if it is not open (a broken connection is closed first)
	/// and returns the prior state, so you can close it afterwards if it was not open.<br/>
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

		// Already open: nothing to do, and nobody should close it.
		if (!NeedsOpen(state)) return state;

		// Broken: it is closed first, then opened.
		if (IsBroken(state))
			connection.Close();

		await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

		// ConnectionState.Closed is zero (it has no bit), so compare the state itself.
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
	[SuppressMessage("Roslynator", "RCS1163:Unused parameter")]
	public static ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.EnsureOpenAsync(cancellationToken);

	/// <summary>The <paramref name="configureAwait"/> argument is ignored: the library always uses <c>ConfigureAwait(false)</c>.</summary>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>
	[Obsolete(ObsoleteMessage)]
	[SuppressMessage("Roslynator", "RCS1163:Unused parameter")]
	public static ValueTask<ConnectionState> EnsureOpenAsync(
		this IDbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.EnsureOpenAsync(cancellationToken);

	/// <summary>The <paramref name="configureAwait"/> argument is ignored: the library always uses <c>ConfigureAwait(false)</c>.</summary>
	/// <inheritdoc cref="OpenAsync(IDbConnection, CancellationToken)"/>
	[Obsolete(ObsoleteMessage)]
	[SuppressMessage("Roslynator", "RCS1163:Unused parameter")]
	public static ValueTask OpenAsync(
		this IDbConnection connection,
		bool configureAwait,
		CancellationToken cancellationToken = default)
		=> connection.OpenAsync(cancellationToken);
}
