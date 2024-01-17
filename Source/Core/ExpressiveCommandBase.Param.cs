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
	public struct Param : IEquatable<Param>

	{
		/// <summary>
		/// The name of the param.
		/// </summary>
		public string Name { get; set; }

		/// <summary>
		/// The value of the param.
		/// </summary>
		public object Value { get; set; }

		/// <summary>
		/// The DbType of the param.
		/// </summary>
		public TDbType? Type { get; set; }

		/// <inheritdoc />
		public readonly bool Equals(Param other)
			=> Name == other.Name
			&& EqualityComparer<object>.Default.Equals(Value, other.Value)
			&& EqualityComparer<TDbType?>.Default.Equals(Type, other.Type);

		/// <inheritdoc />
		public override readonly bool Equals(object? obj)
			=> obj is Param o && Equals(o);

		/// <inheritdoc />
#if NETSTANDARD2_0
		public override readonly int GetHashCode()
		{
			var hashCode = 1477810893;
			hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Name);
			hashCode = hashCode * -1521134295 + EqualityComparer<object>.Default.GetHashCode(Value);
			hashCode = hashCode * -1521134295 + EqualityComparer<TDbType?>.Default.GetHashCode(Type);
			return hashCode;
		}
#else
		public override readonly int GetHashCode()
			=> HashCode.Combine(Name, Value, Type);
#endif

		/// <summary>
		/// Equality operator.
		/// </summary>
		public static bool operator ==(Param left, Param right) => left.Equals(right);

		/// <summary>
		/// Inequality operator.
		/// </summary>
		public static bool operator !=(Param left, Param right) => !left.Equals(right);
	}
}
