namespace Open.Database.Extensions;

public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
		where TConnection : class, IDbConnection
		where TCommand : class, IDbCommand
		where TReader : class, IDataReader
		where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
{
	/// <summary>
	/// A parameter to be created on the database command each time the expressive command executes.
	/// <typeparamref name="TDbType"/> accommodates the difference between <see cref="DbType"/> and provider-specific enums such as <c>SqlDbType</c>.
	/// </summary>
	/// <remarks>
	/// For <see cref="ParameterDirection.Input"/> and <see cref="ParameterDirection.InputOutput"/> parameters, a <see langword="null"/> value is stored as <see cref="DBNull.Value"/> so the parameter is sent as a database null. For output and return-value parameters the value is stored as given.
	/// </remarks>
	public readonly record struct Param

	{
		/// <summary>
		/// Initializes a new instance of the <see cref="Param"/> struct.
		/// </summary>
		/// <param name="name">The name of the parameter. Cannot be <see langword="null"/>, empty, or white space.</param>
		/// <param name="value">The value of the parameter. A <see langword="null"/> input value is converted to <see cref="DBNull.Value"/>.</param>
		/// <param name="type">The optional database type of the parameter. When <see langword="null"/>, the provider infers it from the value.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
		/// <exception cref="ArgumentException"><paramref name="name"/> is empty or white space.</exception>
		public Param(
			string name,
			object? value,
			TDbType? type = null,
			ParameterDirection direction = ParameterDirection.Input)
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			Name = name;
			Type = type;
			Direction = direction;

			bool isInput = direction is ParameterDirection.Input or ParameterDirection.InputOutput;
			Value = isInput ? (value ?? DBNull.Value) : value;
		}

		/// <summary>
		/// Initializes a new instance for a parameter that is added without a name, with
		/// <c>AddParam(value)</c>, or from an <c>AddParams</c> object.
		/// </summary>
		/// <remarks>
		/// <paramref name="expression"/> is the text of the argument, whose real
		/// name is resolved when the command executes.
		/// Until then <see cref="Name"/> holds the last member of the
		/// expression as written, which is also the name used when the
		/// procedure's real names are not known.
		/// </remarks>
		/// <param name="name">The name of the parameter, as written until it is resolved.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="expression">The text of the argument, or <see langword="null"/> once the name is final.</param>
		/// <param name="fromObject">Whether the parameter is a member of an <c>AddParams</c> object.</param>
		/// <param name="clrType">The CLR type of the argument.</param>
		/// <param name="type">The database type, or <see langword="null"/> to let the provider infer it.</param>
		/// <param name="direction">The direction of the parameter.</param>
		private Param(string name, object? value, string? expression, bool fromObject, Type? clrType, TDbType? type = null, ParameterDirection direction = ParameterDirection.Input)
			: this(name, value, type, direction)
		{
			Expression = expression;
			FromObject = fromObject;
			ClrType = clrType;
		}

		/// <summary>
		/// Makes a parameter whose name is derived from the argument
		/// <paramref name="expression"/>.
		/// </summary>
		/// <param name="name">The last member of the expression, as written.</param>
		/// <param name="expression">The text of the argument.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="clrType">The CLR type of the argument.</param>
		/// <returns>The parameter.</returns>
		internal static Param Derived(string name, string expression, object? value, Type clrType)
			=> new(name, value, expression, fromObject: false, clrType);

		/// <summary>
		/// Makes a parameter named by the caller whose argument has a static
		/// type.
		/// </summary>
		/// <remarks>
		/// A <c>NULL</c> value is then sent with that type
		/// (see <see cref="NullDbType"/>) instead of leaving the provider to
		/// guess one. An explicit database type still wins.
		/// </remarks>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="clrType">The CLR type of the argument.</param>
		/// <param name="type">The database type, or <see langword="null"/> to let the provider infer it.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The parameter.</returns>
		internal static Param Typed(string name, object? value, Type clrType, TDbType? type, ParameterDirection direction)
			=> new(name, value, expression: null, fromObject: false, clrType, type, direction);

		/// <summary>
		/// Makes a parameter from a member of an object given to
		/// <c>AddParams(object)</c>.
		/// </summary>
		/// <remarks>
		/// Its expression is the member's name, and when the procedure's real
		/// names are known a member that none of them matches is simply not
		/// sent.
		/// </remarks>
		/// <param name="member">The name of the member.</param>
		/// <param name="value">The value of the member.</param>
		/// <param name="clrType">The type of the member.</param>
		/// <returns>The parameter.</returns>
		internal static Param OfObject(string member, object? value, Type clrType)
			=> new(member, value, member, fromObject: true, clrType);

		/// <summary>
		/// Makes the same parameter once its real name is known: no longer
		/// waiting for a name, but still knowing the CLR type of the argument.
		/// </summary>
		/// <param name="name">The real name.</param>
		/// <returns>The parameter under its real name.</returns>
		internal Param Resolved(string name)
			=> new(name, Value, expression: null, fromObject: false, ClrType, Type, Direction);

		/// <summary>Gets the CLR type of the argument a derived parameter came from, so a <c>NULL</c> can be sent with a type.</summary>
		internal Type? ClrType { get; }

		/// <summary>Gets the database type to send a <c>NULL</c> derived parameter as when no type was given; <see langword="null"/> to let the provider infer.</summary>
		internal DbType? NullDbType => Value is DBNull && !Type.HasValue && ClrType is { } clr ? ClrDbTypes.From(clr) : null;

		/// <summary>Gets the argument expression a name is still to be resolved from, or <see langword="null"/> once the name is final.</summary>
		internal string? Expression { get; }

		/// <summary>Gets a value indicating whether this is a member of an object given to <c>AddParams</c>, which the procedure may not take.</summary>
		internal bool FromObject { get; }

		/// <summary>
		/// Gets the name of the parameter.
		/// </summary>
		/// <value>The name of the parameter.</value>
		public string Name { get; }

		/// <summary>
		/// Gets the value of the parameter. Input and input/output parameters hold <see cref="DBNull.Value"/> instead of <see langword="null"/>.
		/// </summary>
		/// <value>The value of the parameter, or <see langword="null"/> for an output or return-value parameter that was given none.</value>
		public object? Value { get; }

		/// <summary>
		/// Gets the database type the caller gave, or <see langword="null"/> if the provider is to infer it from the value. (A NULL made by a typed <c>AddParam</c> overload reports <see langword="null"/> here and is still sent with the database type of its argument.)
		/// </summary>
		/// <value>The database type of the parameter, or <see langword="null"/> if the provider is to infer it from the value.</value>
		public TDbType? Type { get; }

		/// <summary>
		/// Gets the direction of the parameter.
		/// </summary>
		/// <value>One of the <see cref="ParameterDirection"/> values.</value>
		public ParameterDirection Direction { get; }
	}
}
