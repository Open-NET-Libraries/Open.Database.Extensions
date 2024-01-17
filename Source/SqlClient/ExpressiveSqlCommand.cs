using System.Data.SqlClient;

namespace Open.Database.Extensions;

/// <summary>
/// A specialized for SqlClient abstraction for executing commands on a database using best practices and simplified expressive syntax.
/// </summary>
public class ExpressiveSqlCommand : ExpressiveDbCommandBase<SqlConnection, SqlCommand, SqlDataReader, SqlDbType, ExpressiveSqlCommand>
{
	/// <summary>Constructs a <see cref="ExpressiveSqlCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveSqlCommand(
		IDbConnectionPool<SqlConnection> connectionPool,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connectionPool, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveSqlCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveSqlCommand(
		IDbConnectionFactory<SqlConnection> connFactory,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connFactory, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveSqlCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveSqlCommand(
		SqlConnection connection,
		IDbTransaction? transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, transaction, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveSqlCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveSqlCommand(
		SqlConnection connection,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(connection, type, command, @params)
	{
	}

	/// <summary>Constructs a <see cref="ExpressiveSqlCommand"/>.</summary>
	/// <inheritdoc />
	public ExpressiveSqlCommand(
		IDbTransaction transaction,
		CommandType type,
		string command,
		IEnumerable<Param>? @params = null)
		: base(transaction, type, command, @params)
	{
	}

	/// <summary>
	/// Handles adding the list of parameters to a new command.
	/// </summary>
	protected override void AddParams(SqlCommand command)
	{
		if (command is null) throw new System.ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		foreach (var p in Params)
		{
			var np = command
				.Parameters
				.AddWithValue(p.Name, p.Value);

			if (p.Type.HasValue)
				np.SqlDbType = p.Type.Value;
		}
	}
}
