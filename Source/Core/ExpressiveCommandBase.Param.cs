namespace Open.Database.Extensions;

public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
		where TConnection : class, IDbConnection
		where TCommand : class, IDbCommand
		where TReader : class, IDataReader
		where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
{
	/// <summary>
	/// A struct that represents the param to be created when the command is executed.
	/// TDbType facilitates the difference between DbType and SqlDbType.
	/// </summary>
	public readonly record struct Param

	{
		/// <summary>
		/// Constructs a <see cref="Param"/>.
		/// </summary>
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
		/// The name of the param.
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// The value of the param.
		/// </summary>
		public object? Value { get; }

		/// <summary>
		/// The DbType of the param.
		/// </summary>
		public TDbType? Type { get; }

		/// <summary>
		/// The direction of the param.
		/// </summary>
		public ParameterDirection Direction { get; }
	}
}
