using System;
using System.Collections.Generic;
using System.Data;

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
        public bool Equals(Param other)
            => Name == other.Name
            && EqualityComparer<object>.Default.Equals(Value, other.Value)
            && EqualityComparer<TDbType?>.Default.Equals(Type, other.Type);

        /// <inheritdoc />
        public override bool Equals(object obj)
            => obj is Param o && Equals(o);

        /// <inheritdoc />
#if NETSTANDARD2_1
        public override int GetHashCode()
            => HashCode.Combine(Name, Value, Type);
#else
        public override int GetHashCode()
        {
            var hashCode = 1477810893;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Name);
            hashCode = hashCode * -1521134295 + EqualityComparer<object>.Default.GetHashCode(Value);
            hashCode = hashCode * -1521134295 + EqualityComparer<TDbType?>.Default.GetHashCode(Type);
            return hashCode;
        }
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
