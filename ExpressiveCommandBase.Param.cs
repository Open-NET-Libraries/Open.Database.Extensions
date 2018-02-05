using System.Collections.Generic;
using System.Data;

namespace Open.Database.Extensions
{
    public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
		where TConnection : class, IDbConnection
        where TCommand : class, IDbCommand
        where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
	{
		/// <summary>
		/// A struct that represents the param to be created when the command is exectued.
		/// </summary>
        public struct Param
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

			/// <summary>
			/// Determines whether the specified param is equal to the current one.
			/// </summary>
			/// <param name="obj">Param to compare against.</param>
			/// <returns>True if properties are equal.</returns>
			public override bool Equals(object obj) => obj is Param o
				&& Name == o.Name
				&& EqualityComparer<object>.Default.Equals(Value, o.Value)
				&& EqualityComparer<TDbType?>.Default.Equals(Type, o.Type);

			/// <summary>
			/// Equality operator.
			/// </summary>
			public static bool operator ==(Param left, Param right) => left.Equals(right);
			
			/// <summary>
			/// Inequality operator.
			/// </summary>
			public static bool operator !=(Param left, Param right) => !(left == right);
		}

	}
}
