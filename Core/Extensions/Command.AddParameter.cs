using System;
using System.Data;
using System.Diagnostics.Contracts;

namespace Open.Database.Extensions
{
	public static partial class CommandExtensions
	{
		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameter(this IDbCommand target,
			string name, object? value = null)
		{
			if (name is null)
				throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			var c = target.CreateParameter();
			c.ParameterName = name;
			if (value != null) // DBNull.Value is allowed.
				c.Value = value;
			target.Parameters.Add(c);
			return c;
		}

		/// <summary>
		/// Shortcut for adding command parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameter(this IDbCommand target,
			string name, object value, DbType type, ParameterDirection direction = ParameterDirection.Input)
		{
			var p = target.AddParameterType(name, type);
			p.Value = value;
			p.Direction = direction;
			return p;
		}

		/// <summary>
		/// Shortcut for adding command a typed (non-input) parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <param name="direction">The direction of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddParameterType(this IDbCommand target,
			string? name, DbType type, ParameterDirection direction = ParameterDirection.Input)
		{
			if (direction != ParameterDirection.ReturnValue && name == null)
				throw new ArgumentNullException(nameof(name), "Parameter names can only be null for a return parameter.");
			else if (name != null && string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			var c = target.CreateParameter();
			if (name != null) c.ParameterName = name;
			c.DbType = type;
			c.Direction = direction;
			target.Parameters.Add(c);
			return c;
		}


		/// <summary>
		/// Shortcut for adding command a typed return parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="type">The DbType of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddReturnParameter(this IDbCommand target,
			DbType type, string? name = null)
			=> target.AddParameterType(name, type, ParameterDirection.ReturnValue);

		/// <summary>
		/// Shortcut for adding command a return parameter.
		/// </summary>
		/// <param name="target">The command to add a parameter to.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <returns>The created IDbDataParameter.</returns>
		public static IDbDataParameter AddReturnParameter(this IDbCommand target,
			string? name = null)
		{
			var c = target.CreateParameter();
			if (!string.IsNullOrWhiteSpace(name)) c.ParameterName = name;
			c.Direction = ParameterDirection.ReturnValue;
			target.Parameters.Add(c);
			return c;
		}
	}
}
