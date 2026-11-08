using System.Collections.Immutable;

namespace Open.Database.Extensions;

/// <summary>
/// A deferred, fluent command for running SQL through a <see cref="SqlConnection"/>, <see cref="SqlTransaction"/>, connection factory, or connection pool, with parameters typed by <see cref="SqlDbType"/>. Nothing touches the database until an execute method is called.
/// </summary>
/// <remarks>
/// <para>
/// Create one with the <c>Command</c> and <c>StoredProcedure</c> extension methods in <see cref="SqlConnectionExtensions"/>, add parameters with <c>AddParam</c>, optionally set a timeout and cancellation token, then run it with one of the execute methods (for example <c>ExecuteNonQuery</c>, <c>ExecuteScalar</c>, <c>ExecuteReader</c>, or <c>Map&lt;T&gt;()</c>).
/// </para>
/// <para>
/// Connections follow the same "get in, get out" model as <see cref="ExpressiveDbCommand"/>: each execution takes a connection, opens it only if needed, runs the command, and releases the connection. A connection supplied directly is closed again only if this execution opened it and is never disposed; a connection from a factory is disposed, and one from a pool is given back.
/// </para>
/// </remarks>
/// <seealso cref="ExpressiveDbCommand"/>
/// <seealso cref="SqlConnectionExtensions"/>
public class ExpressiveSqlCommand : ExpressiveDbCommandBase<SqlConnection, SqlCommand, SqlDataReader, SqlDbType, ExpressiveSqlCommand>
{
	/// <summary>
	/// Constructs a command that takes its connections from a connection pool.
	/// </summary>
	/// <param name="connectionPool">The pool to take connections from. Each execution takes one and gives it back when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connectionPool"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbConnectionPool<SqlConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that creates a new connection from a connection factory for each execution.
	/// </summary>
	/// <param name="connFactory">The factory that creates the connections. Each execution creates a connection, opens it if needed, and disposes it when finished.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connFactory"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbConnectionFactory<SqlConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that executes on an existing connection, optionally within a transaction.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="transaction">The optional transaction to execute the command in.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
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
	/// Constructs a command that executes on an existing connection.
	/// </summary>
	/// <param name="connection">The connection to execute on. It is opened only if it is closed, closed again afterwards if the command opened it, and never disposed.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		SqlConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>
	/// Constructs a command that executes within a transaction, on the transaction's connection.
	/// </summary>
	/// <param name="transaction">The transaction to execute the command in. Its <see cref="IDbTransaction.Connection"/> is used for execution.</param>
	/// <param name="type">The command type.</param>
	/// <param name="command">The command text, or the stored procedure name when <paramref name="type"/> is <see cref="CommandType.StoredProcedure"/>.</param>
	/// <param name="params">The initial parameters. They are copied, so later changes to the source collection do not affect the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="command"/> is empty or white space.</exception>
	public ExpressiveSqlCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Adds a parameter of a <see cref="DbType"/> with no value, for example an output parameter: <c>AddParam("@id", DbType.Int32, ParameterDirection.Output)</c>.
	/// </summary>
	/// <remarks>
	/// The database type of this command is <see cref="SqlDbType"/>, so without this overload a <see cref="DbType"/> argument would be taken as the VALUE of the parameter. It is converted to the <see cref="SqlDbType"/> that the provider maps it to.
	/// The direction has no default on purpose: this overload is declared on the command, so it hides the inherited ones wherever it applies, and a literal 0 converts to any enum, which would turn <c>AddParam("@count", 0)</c> into a parameter with no value.
	/// </remarks>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The database type of the parameter.</param>
	/// <param name="direction">The direction of the parameter. An input or input-output parameter with no value is sent as a NULL of that type.</param>
	/// <returns>This instance for use in method chaining.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is blank, or SQL Server has no type for <paramref name="type"/>.</exception>
	public ExpressiveSqlCommand AddParam(string name, DbType type, ParameterDirection direction)
		=> AddParam(name, new SqlParameter { DbType = type }.SqlDbType, direction);

	/// <summary>
	/// Conditionally adds a parameter of a <see cref="DbType"/> with no value. See <see cref="AddParam(string, DbType, ParameterDirection)"/>.
	/// </summary>
	/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The database type of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>This instance for use in method chaining.</returns>
	/// <exception cref="ArgumentException">The parameter is added and <paramref name="name"/> is blank, or SQL Server has no type for <paramref name="type"/>.</exception>
	public ExpressiveSqlCommand AddParamIf(bool condition, string name, DbType type, ParameterDirection direction)
		=> condition ? AddParam(name, type, direction) : this;

	/// <summary>
	/// Handles adding the list of parameters to a new command.
	/// </summary>
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
	/// Reads the parameter names a stored procedure declares from <c>sys.all_parameters</c>, so parameters added with <c>AddParam(value)</c> find their real names.
	/// </summary>
	/// <remarks>The procedure is looked up in the connection's current database; a name that does not resolve there (a missing procedure, no permission to see it) gives no names, and a name with a database part (<c>otherdb.dbo.proc</c>) is not looked up at all, because the lookup runs in the current database: in both cases the last member of each argument is used as written.
	/// The lookup runs with the command's timeout (<c>SetTimeout</c>). A failure of the lookup is thrown as it is (a <see cref="SqlException"/> stays a <see cref="SqlException"/>, so a retry policy still sees it), with the procedure's name added to <see cref="Exception.Data"/> under the key <c>Procedure</c>.</remarks>
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

	/// <inheritdoc cref="GetProcedureParameterNames(SqlConnection, string)"/>
	protected override async ValueTask<ImmutableArray<string>> GetProcedureParameterNamesAsync(SqlConnection connection, string procedure, CancellationToken cancellationToken)
	{
		try
		{
#if NETSTANDARD2_0
			using SqlCommand command = ProcedureNamesCommand(connection, procedure);
			using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
#else
			await using SqlCommand command = ProcedureNamesCommand(connection, procedure);
			await using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
#endif
			return await ReadNamesAsync(reader, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex) when (ex is not OperationCanceledException && !cancellationToken.IsCancellationRequested)
		{
			ex.Data["Procedure"] = procedure;
			throw;
		}
	}

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
