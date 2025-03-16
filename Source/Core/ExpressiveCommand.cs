namespace Open.Database.Extensions;

/// <summary>
/// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
/// </summary>
public class ExpressiveCommand : ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, DbType, ExpressiveCommand>
{
	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(IDbConnectionPool{TConnection}, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		IDbConnectionPool connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool.AsGeneric(), type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(IDbConnectionFactory{TConnection}, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		IDbConnectionFactory connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory.AsGeneric(), type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(IDbConnectionFactory{TConnection}, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		Func<IDbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(TConnection, IDbTransaction?, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		IDbConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(IDbTransaction, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveCommand"/>.</summary>
	/// <inheritdoc cref="ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.ExpressiveCommandBase(TConnection, CommandType, string, IEnumerable{ExpressiveCommandBase{TConnection, TCommand, TReader, TDbType, TThis}.Param}?)" />
	public ExpressiveCommand(
		IDbConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <inheritdoc />
	protected override void AddParams(IDbCommand command)
	{
		foreach (Param p in Params)
		{
			IDbDataParameter np = command.AddParameter(p.Name, p.Value);
			if (p.Type.HasValue) np.DbType = p.Type.Value;
		}
	}
}
