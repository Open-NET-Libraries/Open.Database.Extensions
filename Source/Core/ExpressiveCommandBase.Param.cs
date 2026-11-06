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
