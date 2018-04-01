
using System;
using System.Data;
using System.Data.SqlClient;

namespace Open.Database.Extensions.SqlClient
{
    /// <summary>
    /// SqlClient extensions for building a command and retrieving data using best practices.
    /// </summary>
    public static partial class Extensions
    {

		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static SqlParameter AddParameter(this SqlCommand target,
            string name, object value, SqlDbType type, ParameterDirection direction = ParameterDirection.Input)
        {
            var p = target.AddParameterType(name, type, direction);
            p.Value = value;
            return p;
        }

        /// <summary>
        /// Shortcut for adding command a typed (non-input) parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="type">The SqlDbType of the parameter.</param>
        /// <param name="direction">The direction of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static SqlParameter AddParameterType(this SqlCommand target,
            string name, SqlDbType type, ParameterDirection direction = ParameterDirection.Input)
        {
            var c = target.CreateParameter();
            c.ParameterName = name;
            c.SqlDbType = type;
            c.Direction = direction;
            target.Parameters.Add(c);
            return c;
        }

        /// <summary>
        /// Shortcut for adding command a typed (non-input) parameter.
        /// </summary>
        /// <param name="target">The command to add a parameter to.</param>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="type">The SqlDbType of the parameter.</param>
        /// <returns>The created IDbDataParameter.</returns>
        public static SqlParameter AddParameterType(this IDbCommand target, string name, SqlDbType type)
        {
            return AddParameterType((SqlCommand)target, name, type);
        }
		
    }
}
