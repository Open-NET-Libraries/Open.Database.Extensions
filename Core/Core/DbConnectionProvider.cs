using System;
using System.Data;
using System.Diagnostics.Contracts;

namespace Open.Database.Extensions.Core;

/// <summary>
/// Simplifies handling connections.
/// </summary>
/// <typeparam name="TConnection"></typeparam>
internal class DbConnectionProvider<TConnection> : IDbConnectionPool<TConnection>
        where TConnection : class, IDbConnection
{
    public DbConnectionProvider(TConnection connection)
    {
        Connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    private TConnection Connection { get; }

    private ConnectionState? TakenConnectionState;

    /// <inheritdoc />
    public TConnection Take()
    {
        if (TakenConnectionState.HasValue)
            throw new InvalidOperationException("Concurrent use of a single connection is not supported.");

        TakenConnectionState = Connection.State;
        return Connection;
    }

    IDbConnection IDbConnectionPool.Take() => Take();

    /// <inheritdoc />
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

internal static class DbConnectionProvider
{
    public static DbConnectionProvider<TConnection> Create<TConnection>(TConnection connection)
        where TConnection : class, IDbConnection
        => new(connection);
}
