using System.Data;

namespace Open.Database.Extensions
{
	public static partial class Extensions
    {
        public static IDbDataParameter AddParameter(this IDbCommand target, string name, object value = null)
        {
            var c = target.CreateParameter();
            c.ParameterName = name;
            if (value != null) // DBNull.Value is allowed.
                c.Value = value;
            target.Parameters.Add(c);
            return c;
        }

        public static IDbDataParameter AddParameterType(this IDbCommand target, string name, DbType value)
        {
            var c = target.CreateParameter();
            c.ParameterName = name;
            c.DbType = value;
            target.Parameters.Add(c);
            return c;
        }

        public static IDbCommand CreateCommand(this IDbConnection conn,
            CommandType type, string commandText, int secondsTimeout = 30)
        {
            var command = conn.CreateCommand();
            command.CommandType = type;
            command.CommandText = commandText;
            command.CommandTimeout = secondsTimeout;

            return command;
        }

        public static ExpressiveDbCommand Command(
			this IDbConnectionFactory<IDbConnection> target,
			string command, CommandType type = CommandType.Text)
        {
            return new ExpressiveDbCommand(target, type, command);
		}

		public static ExpressiveDbCommand StoredProcedure(
			this IDbConnectionFactory<IDbConnection> target,
			string command)
		{
			return new ExpressiveDbCommand(target, CommandType.StoredProcedure, command);
		}
	}
}
 