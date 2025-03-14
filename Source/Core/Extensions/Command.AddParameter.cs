namespace Open.Database.Extensions;

public static partial class CommandExtensions
{
	private const string ParameterNamesNotEmptyMessage = "Parameter names cannot be empty or white space.";
	private const string ParameterNamesOnlyNullForReturn = "Parameter names can only be null for a return parameter.";

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		object? value = null)
	{
		if (target is null)
			throw new ArgumentNullException(nameof(target));
		if (name is null)
			throw new ArgumentNullException(nameof(name));
		else if (string.IsNullOrWhiteSpace(name))
			throw new ArgumentException(ParameterNamesNotEmptyMessage, nameof(name));
		Contract.EndContractBlock();

		IDbDataParameter c = target.CreateParameter();
		c.ParameterName = name;
		c.Value = value;
		target.Parameters.Add(c);
		return c;
	}

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		string? value) => AddParameter(target, name, value, DbType.String);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		int value) => AddParameter(target, name, value, DbType.Int32);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		long value) => AddParameter(target, name, value, DbType.Int64);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		short value) => AddParameter(target, name, value, DbType.Int16);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		DateTime value) => AddParameter(target, name, value, DbType.DateTime);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		double value) => AddParameter(target, name, value, DbType.Double);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		decimal value) => AddParameter(target, name, value, DbType.Decimal);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		Guid value) => AddParameter(target, name, value, DbType.Guid);

	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		DateTimeOffset value) => AddParameter(target, name, value, DbType.DateTimeOffset);

	/// <summary>
	/// Shortcut for adding a command parameter.
	/// </summary>
	/// <param name="target">The command to add a parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter.</param>
	/// <param name="type">The <see cref="DbType"/> of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created <see cref="IDbDataParameter"/>.</returns>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		object? value,
		DbType type,
		ParameterDirection direction = ParameterDirection.Input)
	{
		IDbDataParameter p = target.AddParameterType(name, type);
		p.Value = value;
		p.Direction = direction;
		return p;
	}

	/// <summary>
	/// Shortcut for adding a typed (non-input) command parameter.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameterType(
		this IDbCommand target,
		string? name,
		DbType type,
		ParameterDirection direction = ParameterDirection.Input)
	{
		if (target is null)
			throw new ArgumentNullException(nameof(target));
		if (direction != ParameterDirection.ReturnValue && name == null)
			throw new ArgumentNullException(nameof(name), ParameterNamesOnlyNullForReturn);
		else if (name != null && string.IsNullOrWhiteSpace(name))
			throw new ArgumentException(ParameterNamesNotEmptyMessage, nameof(name));
		Contract.EndContractBlock();

		IDbDataParameter c = target.CreateParameter();
		if (name != null) c.ParameterName = name;
		c.DbType = type;
		c.Direction = direction;
		target.Parameters.Add(c);
		return c;
	}

	/// <summary>
	/// Shortcut for adding command a typed return parameter.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddReturnParameter(
		this IDbCommand target,
		DbType type,
		string? name = null)
		=> target.AddParameterType(name, type, ParameterDirection.ReturnValue);

	/// <summary>
	/// Shortcut for adding command a return parameter.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddReturnParameter(this IDbCommand target,
		string? name = null)
	{
		if (target is null)
			throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		IDbDataParameter c = target.CreateParameter();
		if (!string.IsNullOrWhiteSpace(name)) c.ParameterName = name;
		c.Direction = ParameterDirection.ReturnValue;
		target.Parameters.Add(c);
		return c;
	}
}
