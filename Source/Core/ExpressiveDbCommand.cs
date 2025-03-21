﻿namespace Open.Database.Extensions;

/// <summary>
/// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
/// </summary>
public class ExpressiveDbCommand : ExpressiveDbCommandBase<DbConnection, DbCommand, DbDataReader, DbType, ExpressiveDbCommand>
{
	/// <summary>Constructs a <see cref="ExpressiveDbCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveDbCommand(
		IDbConnectionPool<DbConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveDbCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveDbCommand(
		IDbConnectionFactory<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <inheritdoc cref="ExpressiveDbCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveDbCommandBase(IDbConnectionFactory{TConnection}, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)"/>
	public ExpressiveDbCommand(
		Func<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
	: base(connFactory.AsPool(), type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveDbCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveDbCommand(
		DbConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveDbCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveDbCommand(
		DbConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveDbCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveDbCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <inheritdoc />
	protected override void AddParams(DbCommand command)
	{
		foreach (Param p in Params)
		{
			IDbDataParameter np = command.AddParameter(p.Name, p.Value);
			if (p.Type.HasValue) np.DbType = p.Type.Value;
		}
	}
}
