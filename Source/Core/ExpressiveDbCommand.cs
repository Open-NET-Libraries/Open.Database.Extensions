namespace Open.Database.Extensions;

// Unlike ExpressiveCommand, the callbacks need no casts to reach the asynchronous ADO.NET APIs.
/// <summary>
/// A deferred, fluent command for running SQL through any <see cref="DbConnection"/>, <see cref="DbTransaction"/>, connection factory, or connection pool, whose callbacks receive the <see cref="DbCommand"/> and <see cref="DbDataReader"/>.
/// </summary>
/// <remarks>
/// <para>
/// Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="ConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods.
/// </para>
/// <para>
/// Connection ownership is as for <see cref="ExpressiveCommand"/>: a connection supplied directly is closed again only if this execution opened it and is never disposed; one from a factory is disposed, and one from a pool is given back.
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
/// <seealso cref="ExpressiveCommand"/>
/// <seealso cref="ConnectionExtensions"/>
public class ExpressiveDbCommand : ExpressiveDbCommandBase<DbConnection, DbCommand, DbDataReader, DbType, ExpressiveDbCommand>
{
	/// <summary>
	/// Initializes a new instance that takes its connections from a connection pool.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorPool']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		IDbConnectionPool<DbConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that creates a new connection from a connection factory for each execution.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorFactory']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		IDbConnectionFactory<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that invokes a function to create a new connection for each execution.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorFunc']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		Func<DbConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
	: base(connFactory.AsPool(), type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes on an existing connection, optionally within a transaction.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorConnectionTx']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		DbConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes on an existing connection.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorConnection']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		DbConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes within a transaction, on the transaction's connection.
	/// </summary>
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorTx']/*" />
	/// <include file="Commands.docs.xml" path="docs/members[@name='ctorCommon']/*" />
	public ExpressiveDbCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	// The failure is not wrapped, so a retry policy still sees the provider's exception.
	/// <summary>
	/// Gets the parameter names a stored procedure declares through the ADO.NET schema collection <c>ProcedureParameters</c>, so parameters added with <c>AddParam(value)</c> find their real names on any provider that supports it (SQL Server does).
	/// </summary>
	/// <remarks>
	/// <para>
	/// A name with a database part (<c>otherdb.dbo.proc</c>) is not looked up, because the lookup runs in the connection's current database; a name that exists in more than one schema (when no schema is given) is not known either, and neither is anything from a provider that does not implement the collection:
	/// in each case the last member of each argument is used as written.
	/// </para>
	/// <para>
	/// A failure of the lookup is thrown as it is, a <see cref="DbException"/> staying one, with the procedure's name added to <see cref="Exception.Data"/> under the key <c>Procedure</c>.
	/// </para>
	/// </remarks>
	/// <inheritdoc />
	protected override ImmutableArray<string> GetProcedureParameterNames(DbConnection connection, string procedure)
		=> ProcedureSchemaNames.Get(connection, procedure);

	/// <summary>
	/// Asynchronously gets the parameter names a stored procedure declares
	/// through the ADO.NET schema collection <c>ProcedureParameters</c>.
	/// </summary>
	/// <param name="connection">An open connection to run the lookup on.</param>
	/// <param name="procedure">The procedure name as given to the command.</param>
	/// <param name="cancellationToken">A token that can cancel the lookup.</param>
	/// <inheritdoc cref="GetProcedureParameterNames(DbConnection, string)" path="/*[not(self::param) and not(self::summary)]"/>
	protected override ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(DbConnection connection, string procedure, CancellationToken cancellationToken)
		=> ProcedureSchemaNames.GetAsync(connection, procedure, cancellationToken);

	/// <inheritdoc />
	protected override void AddParams(DbCommand command)
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
