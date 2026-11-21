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
	/// Opens the connection if it is not open (a broken connection is closed first) and
	/// returns a <see cref="ConnectionScope"/> that puts it back when disposed: a connection
	/// this call opened is closed again, and one that was already open is left open.
	/// </summary>
	/// <remarks>
	/// The same rule as <see cref="EnsureOpen(IDbConnection)"/>, which returns the prior state
	/// for a caller that wants to do the closing itself; the scope does it, without a
	/// <c>try</c>/<c>finally</c>. It is safe to call on a connection that is already open.
	/// Prefer <see cref="EnsureOpenScopeAsync(IDbConnection, CancellationToken)"/>.
	/// </remarks>
	/// <param name="connection">The connection to open.</param>
	/// <returns>The connection and the state it was found in, restored by <see cref="ConnectionScope.Dispose"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	public static ConnectionScope EnsureOpenScope(this IDbConnection connection)
		=> new(connection, connection.EnsureOpen());

	/// <summary>
	/// Opens the connection if it is not open (a broken connection is closed first) and
	/// returns a <see cref="ConnectionScope"/> that puts it back when disposed: a connection
	/// this call opened is closed again, and one that was already open is left open.
	/// </summary>
	/// <remarks>
	/// <code>
	/// await using var scope = await connection.EnsureOpenScopeAsync(cancellationToken);
	/// </code>
	/// The same rule as <see cref="EnsureOpenAsync(DbConnection, CancellationToken)"/>, which returns the
	/// prior state for a caller that wants to do the closing itself. It opens with
	/// <see cref="DbConnection.OpenAsync(CancellationToken)"/> when the connection is a
	/// <see cref="DbConnection"/>, and always awaits with <c>ConfigureAwait(false)</c>.
	/// The scope's <see cref="ConnectionScope.DisposeAsync"/> closes with <c>CloseAsync</c>
	/// where the target has it (not netstandard2.0, which closes synchronously). If the token
	/// is cancelled the connection it opened is closed again and no scope is returned.
	/// </remarks>
	/// <param name="connection">The connection to open.</param>
	/// <param name="cancellationToken">A cancellation token to cancel opening.</param>
	/// <returns>The connection and the state it was found in, restored by <see cref="ConnectionScope.DisposeAsync"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	public static async ValueTask<ConnectionScope> EnsureOpenScopeAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken = default)
		=> new(
			connection,
			await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false));
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
