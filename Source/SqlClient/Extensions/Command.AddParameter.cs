namespace Open.Database.Extensions;

public static partial class SqlCommandExtensions
{
	/// <summary>
	/// Adds a typed parameter with the given name, value, and direction to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value of the parameter. It is assigned as given: a <see langword="null"/> is not converted to <see cref="DBNull.Value"/>, so pass <see cref="DBNull.Value"/> to send a database null.</param>
	/// <param name="type">The <see cref="SqlDbType"/> of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> or <paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	public static SqlParameter AddParameter(
		this SqlCommand target,
		string name,
		object? value,
		SqlDbType type,
		ParameterDirection direction = ParameterDirection.Input)
	{
		if (target is null) throw new ArgumentNullException(nameof(target));
		Contract.EndContractBlock();

		SqlParameter p = AddParameterType(target, name, type, direction);
		p.Value = value;
		return p;
	}

	/// <summary>
	/// Adds a parameter of the given type and direction to the command without assigning a value.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="name">The name of the parameter. It can be <see langword="null"/> only when <paramref name="direction"/> is <see cref="ParameterDirection.ReturnValue"/>.</param>
	/// <param name="type">The <see cref="SqlDbType"/> of the parameter.</param>
	/// <param name="direction">The direction of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Set its value afterwards if it is an input parameter.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>, or <paramref name="name"/> is <see langword="null"/> and <paramref name="direction"/> is not <see cref="ParameterDirection.ReturnValue"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
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

		SqlParameter c = target.CreateParameter();
		c.ParameterName = name;
		c.SqlDbType = type;
		c.Direction = direction;
		target.Parameters.Add(c);
		return c;
	}

	/// <summary>
	/// Adds an input parameter of the given type to a command that must be a <see cref="SqlCommand"/>, without assigning a value.
	/// </summary>
	/// <param name="target">The command to add the parameter to. It must be a <see cref="SqlCommand"/>.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="type">The <see cref="SqlDbType"/> of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection.</returns>
	/// <exception cref="InvalidCastException"><paramref name="target"/> is not a <see cref="SqlCommand"/>.</exception>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	public static SqlParameter AddParameterType(this IDbCommand target,
		string name, SqlDbType type)
		=> AddParameterType((SqlCommand)target, name, type);

	/// <summary>
	/// Adds a typed return-value parameter to the command.
	/// </summary>
	/// <param name="target">The command to add the parameter to.</param>
	/// <param name="type">The <see cref="SqlDbType"/> of the parameter.</param>
	/// <param name="name">The optional name of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Read its value after the command has executed.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
	public static SqlParameter AddReturnParameter(this SqlCommand target,
		SqlDbType type, string? name = null)
		=> AddParameterType(target, name, type, ParameterDirection.ReturnValue);
}
