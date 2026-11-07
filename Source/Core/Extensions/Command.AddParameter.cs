namespace Open.Database.Extensions;

public static partial class CommandExtensions
{
	private const string ParameterNamesNotEmptyMessage = "Parameter names cannot be empty or white space.";
	private const string ParameterNamesOnlyNullForReturn = "Parameter names can only be null for a return parameter.";

	/// <summary>
	/// Adds a parameter with the given name and value to the command, leaving the provider to infer the database type from the value.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter. It is assigned as given: a <see langword="null"/> is not converted to <see cref="DBNull.Value"/>, and providers differ in how they treat it, so pass <see cref="DBNull.Value"/> to send a database null.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
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

	/// <summary>
	/// Adds a string parameter (<see cref="DbType.String"/>) with the given name and value to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter. A <see langword="null"/> is assigned as given and is not converted to <see cref="DBNull.Value"/>.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	/// <seealso cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		string? value) => AddParameter(target, name, value, DbType.String);

	/// <summary>
	/// Adds a 32-bit integer parameter (<see cref="DbType.Int32"/>) with the given name and value to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	/// <seealso cref="AddParameter(IDbCommand, string, object, DbType, ParameterDirection)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		int value) => AddParameter(target, name, value, DbType.Int32);

	/// <summary>
	/// Adds a 64-bit integer parameter (<see cref="DbType.Int64"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		long value) => AddParameter(target, name, value, DbType.Int64);

	/// <summary>
	/// Adds a 16-bit integer parameter (<see cref="DbType.Int16"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		short value) => AddParameter(target, name, value, DbType.Int16);

	/// <summary>
	/// Adds a date and time parameter (<see cref="DbType.DateTime"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		DateTime value) => AddParameter(target, name, value, DbType.DateTime);

	/// <summary>
	/// Adds a double-precision floating point parameter (<see cref="DbType.Double"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		double value) => AddParameter(target, name, value, DbType.Double);

	/// <summary>
	/// Adds a decimal parameter (<see cref="DbType.Decimal"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		decimal value) => AddParameter(target, name, value, DbType.Decimal);

	/// <summary>
	/// Adds a GUID parameter (<see cref="DbType.Guid"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		Guid value) => AddParameter(target, name, value, DbType.Guid);

	/// <summary>
	/// Adds a date and time with offset parameter (<see cref="DbType.DateTimeOffset"/>) with the given name and value to the command.
	/// </summary>
	/// <inheritdoc cref="AddParameter(IDbCommand, string, int)"/>
	public static IDbDataParameter AddParameter(
		this IDbCommand target,
		string name,
		DateTimeOffset value) => AddParameter(target, name, value, DbType.DateTimeOffset);

	/// <summary>
	/// Adds a typed parameter with the given name, value, and direction to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter. It is assigned as given: a <see langword="null"/> is not converted to <see cref="DBNull.Value"/>, so pass <see cref="DBNull.Value"/> to send a database null.</param>
	/// <param name="type">The <see cref="DbType"/> of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
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
	/// Adds a parameter of the given type and direction to the command without assigning a value.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter. It can be <see langword="null"/> only when <paramref name="direction"/> is <see cref="ParameterDirection.ReturnValue"/>.</param>
	/// <param name="type">The <see cref="DbType"/> of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Set its value afterwards if it is an input parameter.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>, or <paramref name="name"/> is <see langword="null"/> and <paramref name="direction"/> is not <see cref="ParameterDirection.ReturnValue"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
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
	/// Adds a typed return-value parameter to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="type">The <see cref="DbType"/> of the parameter.</param>
	/// <param name="name">The optional name of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Read its value after the command has executed.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	public static IDbDataParameter AddReturnParameter(
		this IDbCommand target,
		DbType type,
		string? name = null)
		=> target.AddParameterType(name, type, ParameterDirection.ReturnValue);

	/// <summary>
	/// Adds a return-value parameter to the command, leaving the provider to determine its type.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The optional name of the parameter. It is ignored if it is <see langword="null"/>, empty, or white space.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Read its value after the command has executed.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
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
