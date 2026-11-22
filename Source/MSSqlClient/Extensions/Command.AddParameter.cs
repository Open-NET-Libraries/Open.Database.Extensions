namespace Open.Database.Extensions;

public static partial class SqlCommandExtensions
{
	/// <summary>
	/// Adds a typed parameter with the given name, value, and direction to the command.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-target']/*" />
	/// <param name="name">The name of the parameter. It can be <see langword="null"/> only when <paramref name="direction"/> is <see cref="ParameterDirection.ReturnValue"/>.</param>
	/// <param name="value">The value of the parameter. It is assigned as given: a <see langword="null"/> is not converted to <see cref="DBNull.Value"/>, so pass <see cref="DBNull.Value"/> to send a database null.</param>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-type']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-direction']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-created']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>, or <paramref name="name"/> is <see langword="null"/> and <paramref name="direction"/> is not <see cref="ParameterDirection.ReturnValue"/>.</exception>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='blank-name']/*" />
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
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-target']/*" />
	/// <param name="name">The name of the parameter. It can be <see langword="null"/> only when <paramref name="direction"/> is <see cref="ParameterDirection.ReturnValue"/>.</param>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-type']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-direction']/*" />
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Set its value afterwards if it is an input parameter.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>, or <paramref name="name"/> is <see langword="null"/> and <paramref name="direction"/> is not <see cref="ParameterDirection.ReturnValue"/>.</exception>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='blank-name']/*" />
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
	/// Adds a typed return-value parameter to the command.
	/// </summary>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-target']/*" />
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='param-type']/*" />
	/// <param name="name">The optional name of the parameter.</param>
	/// <returns>The created parameter, which has already been added to the command's parameter collection. Read its value after the command has executed.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
	/// <include file="MSSqlClient.docs.xml" path="docs/members[@name='blank-name']/*" />
	public static SqlParameter AddReturnParameter(this SqlCommand target,
		SqlDbType type, string? name = null)
		=> AddParameterType(target, name, type, ParameterDirection.ReturnValue);
}
