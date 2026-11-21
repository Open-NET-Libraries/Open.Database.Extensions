namespace Open.Database.Extensions;

/// <summary>
/// A deferred, fluent command for running SQL through any <see cref="IDbConnection"/>, <see cref="IDbTransaction"/>, connection factory, or connection pool. Nothing touches the database until an execute method is called.
/// </summary>
/// <remarks>
/// <para>
/// Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="ConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods (for example <c>ExecuteNonQuery</c>, <c>ExecuteScalar</c>, <c>ExecuteReader</c>, or <c>Map&lt;T&gt;()</c>).
/// </para>
/// <para>
/// Connections follow a "get in, get out" model. Each execution takes a connection, opens it only if it is not already open, builds the database command, runs it, and releases the connection. A connection supplied directly is closed again only if this execution opened it and is never disposed. A connection that comes from a factory is disposed after the execution, and one that comes from a pool is given back to the pool.
/// </para>
/// <para>
/// A command built on a single connection supports one execution at a time; overlapping executions throw <see cref="InvalidOperationException"/>. Commands built on a factory or pool use a separate connection for each execution.
/// </para>
/// <para>
/// <c>ExecuteScalarAsync</c> and <c>ExecuteNonQueryAsync</c> are on <see cref="ExpressiveDbCommand"/> (a <see cref="System.Data.Common.DbConnection"/>).
/// </para>
/// </remarks>
/// <example>
/// <code language="csharp">
/// List&lt;Post&gt; posts = await connection
///     .Command("SELECT Id, Title FROM Posts WHERE AuthorId = @authorId")
///     .AddParam(authorId)
///     .Map&lt;Post&gt;()
///     .ToListAsync(cancellationToken);
/// </code>
/// </example>
/// <seealso cref="ExpressiveDbCommand"/>
/// <seealso cref="ConnectionExtensions"/>
/// <seealso cref="IExecuteCommand"/>
/// <seealso cref="IExecuteReader"/>
public class ExpressiveCommand : ExpressiveCommandBase<IDbConnection, IDbCommand, IDataReader, DbType, ExpressiveCommand>
{
	/// <summary>
	/// Initializes a new instance that takes its connections from a connection pool.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorPool']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		IDbConnectionPool connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool.AsGeneric(), type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that creates a new connection from a connection factory for each execution.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorFactory']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		IDbConnectionFactory connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory.AsGeneric(), type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that invokes a function to create a new connection for each execution.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorFunc']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		Func<IDbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes on an existing connection, optionally within a transaction.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorConnectionTx']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		IDbConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes within a transaction, on the transaction's connection.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorTx']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes on an existing connection.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorConnection']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveCommand(
		IDbConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>
	/// Gets the parameter names a stored procedure declares through the ADO.NET schema collection <c>ProcedureParameters</c> when the connection is a <see cref="DbConnection"/> whose provider supports it (SQL Server does), so parameters added with <c>AddParam(value)</c> find their real names.
	/// </summary>
	/// <remarks>Any other connection gives no names, and the last member of each argument is used as written. See <see cref="ExpressiveDbCommand"/> for what is looked up and what is thrown.</remarks>
	/// <inheritdoc />
	protected override ImmutableArray<string> GetProcedureParameterNames(IDbConnection connection, string procedure)
		=> connection is DbConnection db ? ProcedureSchemaNames.Get(db, procedure) : default;

	/// <inheritdoc />
	protected override void AddParams(IDbCommand command)
	{
		foreach (Param p in Params)
		{
			IDbDataParameter np = command.AddParameter(p.Name, p.Value);
			np.Direction = p.Direction;
			if (p.Type.HasValue) np.DbType = p.Type.Value;
			else if (p.NullDbType is { } nullType) np.DbType = nullType; // a NULL carries no type, so give it the argument's
		}
	}
}
