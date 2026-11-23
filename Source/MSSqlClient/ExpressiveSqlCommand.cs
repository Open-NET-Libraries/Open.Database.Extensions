using System.Collections.Immutable;

namespace Open.Database.Extensions;

/// <summary>
/// A deferred, fluent command for running SQL through a <see cref="SqlConnection"/>, <see cref="SqlTransaction"/>, connection factory, or connection pool, with parameters typed by <see cref="SqlDbType"/>.
/// </summary>
/// <remarks>
/// <para>
/// Nothing touches the database until an execute method is called. Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="SqlConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods (for example <c>ExecuteNonQuery</c>, <c>ExecuteScalar</c>, <c>ExecuteReader</c>, or <c>Map&lt;T&gt;()</c>).
/// </para>
/// <para>
/// Connections follow the same "get in, get out" model as <see cref="ExpressiveDbCommand"/>: each execution takes a connection, opens it only if needed, runs the command, and releases the connection. A connection supplied directly is closed again only if this execution opened it and is never disposed; a connection from a factory is disposed, and one from a pool is given back.
/// </para>
/// </remarks>
/// <example>
/// <code language="csharp">
/// List&lt;Post&gt; posts = await connection
///     .Command("SELECT Id, Title FROM Posts WHERE AuthorId = @authorId")
///     .AddParam("authorId", authorId, SqlDbType.Int)
///     .Map&lt;Post&gt;()
///     .ToListAsync(cancellationToken);
/// </code>
/// </example>
/// <seealso cref="ExpressiveDbCommand"/>
/// <seealso cref="SqlConnectionExtensions"/>
public class ExpressiveSqlCommand : ExpressiveDbCommandBase<SqlConnection, SqlCommand, SqlDataReader, SqlDbType, ExpressiveSqlCommand>
{
	/// <summary>
	/// Initializes a new instance that takes its connections from a connection pool.
	/// </summary>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-pool']/*" />
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-common']/*" />
	public ExpressiveSqlCommand(
		IDbConnectionPool<SqlConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that creates a new connection from a connection factory for each execution.
	/// </summary>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-factory']/*" />
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-common']/*" />
	public ExpressiveSqlCommand(
		IDbConnectionFactory<SqlConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes on an existing connection, optionally within a transaction.
	/// </summary>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-connection-tx']/*" />
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-common']/*" />
	public ExpressiveSqlCommand(
		SqlConnection connection,
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
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-connection']/*" />
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-common']/*" />
	public ExpressiveSqlCommand(
		SqlConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>
	/// Initializes a new instance that executes within a transaction, on the transaction's connection.
	/// </summary>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-tx']/*" />
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='ctor-common']/*" />
	public ExpressiveSqlCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	// The database type of this command is SqlDbType, so without this overload a DbType argument would be taken as the value of the parameter.
	// The direction has no default on purpose: this overload hides the inherited AddParam overloads wherever it applies, and a literal 0 converts to any enum, which would turn AddParam("@count", 0) into a parameter with no value.
	/// <summary>
	/// Adds a parameter of a <see cref="DbType"/> with no value, for example an output parameter: <c>AddParam("@id", DbType.Int32, ParameterDirection.Output)</c>.
	/// </summary>
	/// <remarks>
	/// A constant <c>0</c> with a direction (<c>AddParam("@n", 0, ParameterDirection.InputOutput)</c>) binds to this overload as a <see cref="DbType"/>: write <c>(object)0</c> or use a variable.
	/// </remarks>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The database type of the parameter.</param>
	/// <param name="direction">The direction of the parameter. An input or input-output parameter with no value is sent as a NULL of that type.</param>
	/// <returns>This instance for use in method chaining.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space, or SQL Server has no type for <paramref name="type"/>.</exception>
	public ExpressiveSqlCommand AddParam(string name, DbType type, ParameterDirection direction)
		=> AddParam(name, new SqlParameter { DbType = type }.SqlDbType, direction);

	/// <summary>
	/// Conditionally adds a parameter of a <see cref="DbType"/> with no value.
	/// </summary>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='condition']/*" />
	/// <exception cref="ArgumentNullException">The parameter is added and <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">The parameter is added and <paramref name="name"/> is empty or white space, or SQL Server has no type for <paramref name="type"/>.</exception>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The database type of the parameter.</param>
	/// <param name="direction">The direction of the parameter. An input or input-output parameter with no value is sent as a NULL of that type.</param>
	/// <inheritdoc cref="AddParam(string, DbType, ParameterDirection)" path="/*[not(self::summary) and not(self::exception) and not(self::param)]"/>
	public ExpressiveSqlCommand AddParamIf(bool condition, string name, DbType type, ParameterDirection direction)
		=> condition ? AddParam(name, type, direction) : this;

	/// <summary>
	/// Adds the command's parameters to a new <see cref="SqlCommand"/>.
	/// </summary>
	/// <remarks>
	/// A parameter that has a value is added with it, so the provider infers the type from the value.
	/// A parameter that has no value (an output or return-value parameter) is still added, so that it reaches the command.
	/// The direction is applied, and the database type when one was given; otherwise a database null that came from an argument of a static type is sent with the <see cref="DbType"/> of that argument, as a null carries no type of its own.
	/// </remarks>
	/// <param name="command">The command to add the parameters to.</param>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	protected override void AddParams(SqlCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		foreach (Param p in Params)
		{
			SqlParameter np;
			if (p.Value is null)
			{
				np = command.CreateParameter();
				np.ParameterName = p.Name;
				command.Parameters.Add(np); // a parameter with no value (output, return value) must still reach the command
			}
			else
			{
				np = command.Parameters.AddWithValue(p.Name, p.Value);
			}

			np.Direction = p.Direction;
			if (p.Type.HasValue)
				np.SqlDbType = p.Type.Value;
			else if (p.NullDbType is { } nullType)
				np.DbType = nullType; // a NULL carries no type, so give it the argument's
		}
	}

	/// <summary>
	/// Gets the parameter names a stored procedure declares by reading <c>sys.all_parameters</c>, so parameters added with <c>AddParam(value)</c> find their real names.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The procedure is looked up in the connection's current database; a name that does not resolve there (a missing procedure, no permission to see it) gives no names, and a name with a database part (<c>otherdb.dbo.proc</c>) is not looked up at all, because the lookup runs in the current database: in both cases the last member of each argument is used as written.
	/// </para>
	/// <para>
	/// The lookup runs with the command's timeout (<c>SetTimeout</c>).
	/// </para>
	/// <include file="Extensions/MSSqlClient.docs.xml" path="docs/members[@name='names-failure']/*" />
	/// </remarks>
	/// <inheritdoc />
	protected override ImmutableArray<string> GetProcedureParameterNames(SqlConnection connection, string procedure)
	{
		try
		{
			using SqlCommand command = ProcedureNamesCommand(connection, procedure);
			using SqlDataReader reader = command.ExecuteReader();
			return ReadNames(reader);
		}
		catch (Exception ex) when (ex is not OperationCanceledException)
		{
			ex.Data["Procedure"] = procedure;
			throw;
		}
	}

	/// <summary>
	/// Asynchronously gets the parameter names a stored procedure declares by reading <c>sys.all_parameters</c>.
	/// </summary>
	/// <remarks>
	/// The failure of the lookup is annotated as in <see cref="GetProcedureParameterNames(SqlConnection, string)"/>, unless it is an <see cref="OperationCanceledException"/> or <paramref name="cancellationToken"/> has been canceled.
	/// </remarks>
	/// <inheritdoc />
	protected override async ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(SqlConnection connection, string procedure, CancellationToken cancellationToken)
	{
		try
		{
#if NET10_0_OR_GREATER
			SqlCommand command = ProcedureNamesCommand(connection, procedure);
			await using var commandDisposal = command.ConfigureAwait(false);
			SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
			await using var readerDisposal = reader.ConfigureAwait(false);
#else
			using SqlCommand command = ProcedureNamesCommand(connection, procedure);
			using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
#endif
			return await ReadNamesAsync(reader, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
		{
			ex.Data["Procedure"] = procedure;
			throw;
		}
	}

	/// <summary>
	/// Creates the command that looks up a procedure's parameter names, in the command's own transaction when it has one.
	/// </summary>
	/// <param name="connection">The connection to create the command from.</param>
	/// <param name="procedure">The procedure name as given to the command.</param>
	/// <returns>A new <see cref="SqlCommand"/> that the caller disposes.</returns>
	SqlCommand ProcedureNamesCommand(SqlConnection connection, string procedure)
	{
		SqlCommand command = connection.CreateCommand();
		command.CommandText = "select name from sys.all_parameters where object_id = object_id(@procedure) and parsename(@procedure, 3) is null and parameter_id > 0 order by parameter_id";
		command.CommandTimeout = Timeout; // the command's own timeout (SetTimeout), not the provider's default
		command.Parameters.Add("@procedure", SqlDbType.NVarChar, 776).Value = procedure;
		if (Transaction is SqlTransaction transaction) command.Transaction = transaction;
		return command;
	}

	static ImmutableArray<string> ReadNames(SqlDataReader reader)
	{
		ImmutableArray<string>.Builder names = ImmutableArray.CreateBuilder<string>();
		while (reader.Read()) names.Add(reader.GetString(0));
		return names.ToImmutable();
	}

	static async Task<ImmutableArray<string>> ReadNamesAsync(SqlDataReader reader, CancellationToken cancellationToken)
	{
		ImmutableArray<string>.Builder names = ImmutableArray.CreateBuilder<string>();
		while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) names.Add(reader.GetString(0));
		return names.ToImmutable();
	}
}
