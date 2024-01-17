using System.Data.SqlClient;
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace Open.Database.Extensions;

/// <summary>
/// SqlClient extensions for building a command and retrieving data using best practices.
/// </summary>
public static partial class SqlCommandExtensions
{
	/// <summary>
	/// Shortcut for adding command parameter.
	/// </summary>
	/// <param name="target">The command to add a parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter.</param>
	/// <param name="type">The DbType of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created IDbDataParameter.</returns>
	public static SqlParameter AddParameter(
		this SqlCommand target,
		string name,
		object? value,
		SqlDbType type,
		ParameterDirection direction = ParameterDirection.Input)
	{
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		var p = AddParameterType(target, name, type, direction);
		p.Value = value;
		return p;
	}

	/// <summary>
	/// Shortcut for adding command a typed (non-input) parameter.
	/// </summary>
	/// <param name="target">The command to add a parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The SqlDbType of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created IDbDataParameter.</returns>
	public static SqlParameter AddParameterType(
		this SqlCommand target,
		string? name,
		SqlDbType type,
		ParameterDirection direction = ParameterDirection.Input)
	{
		if (target is null) throw new ArgumentNullException(nameof(target));
		if (direction != ParameterDirection.ReturnValue && name == null)
			throw new ArgumentNullException(nameof(name), "Parameter names can only be null for a return parameter.");
		else if (name != null && string.IsNullOrWhiteSpace(name))
			throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
		Contract.EndContractBlock();

		var c = target.CreateParameter();
		c.ParameterName = name;
		c.SqlDbType = type;
		c.Direction = direction;
		target.Parameters.Add(c);
		return c;
	}

	/// <summary>
	/// Shortcut for adding command a typed (non-input) parameter.
	/// </summary>
	/// <param name="target">The command to add a parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The SqlDbType of the parameter.</param>
	/// <returns>The created IDbDataParameter.</returns>
	public static SqlParameter AddParameterType(this IDbCommand target,
		string name, SqlDbType type)
		=> AddParameterType((SqlCommand)target, name, type);

	/// <summary>
	/// Shortcut for adding command a typed return parameter.
	/// </summary>
	/// <param name="target">The command to add a parameter to.</param>
	/// <param name="type">The SqlDbType of the parameter.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <returns>The created IDbDataParameter.</returns>
	public static SqlParameter AddReturnParameter(this SqlCommand target,
		SqlDbType type, string? name = null)
		=> AddParameterType(target, name, type, ParameterDirection.ReturnValue);
}
