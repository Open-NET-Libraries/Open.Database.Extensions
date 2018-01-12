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
            CommandType type, string commandText, int secondsTimeout = 600)
        {
            var command = conn.CreateCommand();
            command.CommandType = type;
            command.CommandText = commandText;
            command.CommandTimeout = secondsTimeout;

            return command;
        }

        public static DataTable LoadTable(this IDbConnectionFactory factory,
            CommandType type, string commandText, int millisecondsTimeout = 600)
        {
            var data = new DataTable();
            using (var con = factory.Create())
            using (var com = con.CreateCommand(type, commandText))
            using (var reader = com.ExecuteReader())
            {
                data.Load(reader);
            }
            return data;
        }

        // This is begging for an async producer consumer queue... (Dataflow?)

        public static DbStoredProcedure NewStoredProcedure(this IDbConnectionFactory target, string name)
        {
            return new DbStoredProcedure(target, name);
        }
    }
}
 