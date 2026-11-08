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
		/// Constructs a <see cref="Param"/>.
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

		// A parameter added without a name, AddParam(value): Expression is the text of the argument, whose real name is resolved when the command executes.
		// Until then Name holds the last member of the expression as written, which is also the name used when the procedure's real names are not known.
		private Param(string name, object? value, string? expression, bool fromObject, Type? clrType, TDbType? type = null, ParameterDirection direction = ParameterDirection.Input)
			: this(name, value, type, direction)
		{
			Expression = expression;
			FromObject = fromObject;
			ClrType = clrType;
		}

		internal static Param Derived(string name, string expression, object? value, Type clrType)
			=> new(name, value, expression, fromObject: false, clrType);

		// A member of an object given to AddParams(object): its expression is the member's name, and when the procedure's real names are known a member
		// that none of them matches is simply not sent.
		internal static Param OfObject(string member, object? value, Type clrType)
			=> new(member, value, member, fromObject: true, clrType);

		// The same parameter once its real name is known: no longer waiting for a name, but still knowing the CLR type of the argument.
		internal Param Resolved(string name)
			=> new(name, Value, expression: null, fromObject: false, ClrType, Type, Direction);

		/// <summary>The CLR type of the argument a derived parameter came from, so a <see langword="NULL"/> can be sent with a type.</summary>
		internal Type? ClrType { get; }

		/// <summary>The database type to send a <see langword="NULL"/> derived parameter as when no type was given; <see langword="null"/> to let the provider infer.</summary>
		internal DbType? NullDbType => Value is DBNull && !Type.HasValue && ClrType is { } clr ? ClrDbTypes.From(clr) : null;

		/// <summary>The argument expression a name is still to be resolved from, or <see langword="null"/> once the name is final.</summary>
		internal string? Expression { get; }

		/// <summary>True for a member of an object given to <c>AddParams</c>, which the procedure may not take.</summary>
		internal bool FromObject { get; }

		/// <summary>
		/// Gets the name of the parameter.
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Gets the value of the parameter. Input and input/output parameters hold <see cref="DBNull.Value"/> instead of <see langword="null"/>.
		/// </summary>
		public object? Value { get; }

		/// <summary>
		/// Gets the database type of the parameter, or <see langword="null"/> if the provider is to infer it from the value.
		/// </summary>
		public TDbType? Type { get; }

		/// <summary>
		/// Gets the direction of the parameter.
		/// </summary>
		public ParameterDirection Direction { get; }
	}
}
