using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for database connections.
/// </summary>
public static partial class ConnectionExtensions
{
	/// <returns>The prior connection state.</returns>
	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, bool, CancellationToken)"/>
	public static ConnectionState EnsureOpen(this IDbConnection connection)
    {
        if (connection is null) throw new ArgumentNullException(nameof(connection));
        Contract.EndContractBlock();

        var state = connection.State;

        if (state.HasFlag(ConnectionState.Broken))
            connection.Close();

        if (!connection.State.HasFlag(ConnectionState.Open))
            connection.Open();

        return state;
    }

    /// <summary>
    /// If the connection isn't open, opens the connection.<br/>
    /// If the connection is in neither open or close, first closes the connection, then opens it.
    /// </summary>
    /// <param name="connection">The connection to transact with.</param>
    /// <param name="configureAwait">If true (default) will retain the context after opening.</param>
    /// <param name="cancellationToken">An cancellation token to cancel opening.</param>
    /// <returns>A task containing the prior connection state.</returns>
    public static async ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		bool configureAwait = true,
		CancellationToken cancellationToken = default)
    {
        if (connection is null) throw new ArgumentNullException(nameof(connection));
        Contract.EndContractBlock();

        cancellationToken.ThrowIfCancellationRequested();

        var state = connection.State;
        if (state.HasFlag(ConnectionState.Broken))
            connection.Close();

        if (connection.State.HasFlag(ConnectionState.Open))
            return state;

        await connection.OpenAsync(cancellationToken).ConfigureAwait(configureAwait);

        if (cancellationToken.IsCancellationRequested && !state.HasFlag(ConnectionState.Closed))
        {
            connection.Close(); // Fake finally...
            cancellationToken.ThrowIfCancellationRequested();
        }

        return state;
    }

	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, bool, CancellationToken)"/>
	public static ValueTask<ConnectionState> EnsureOpenAsync(
		this DbConnection connection,
		CancellationToken cancellationToken)
        => connection.EnsureOpenAsync(true, cancellationToken);

	/// <inheritdoc cref="EnsureOpenAsync(DbConnection, bool, CancellationToken)"/>
	internal static async ValueTask<ConnectionState> EnsureOpenAsync(
		this IDbConnection connection,
		CancellationToken cancellationToken)
    {
        if (connection is DbConnection c)
            return await c.EnsureOpenAsync(true, cancellationToken).ConfigureAwait(true);

        cancellationToken.ThrowIfCancellationRequested();
        return connection.EnsureOpen();
    }
}
