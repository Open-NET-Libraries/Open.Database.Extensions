using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

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


        public static void Iterate(this IDataReader reader, Action<IDataRecord> handler)
        {
            while (reader.Read()) handler(reader);
        }

        public static IEnumerable<T> Iterate<T>(this IDataReader reader, Func<IDataRecord, T> handler)
        {
            while (reader.Read())
                yield return handler(reader);
        }

        public static List<T> ToList<T>(this IDbCommand command, Func<IDataRecord, T> handler)
        {
            using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
                return reader.Iterate(handler).ToList();
        }

        public static DataTable ToDataTable(this IDbCommand command)
        {
            using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
            {
                var table = new DataTable();
                table.Load(reader);
                return table;
            }
        }

        public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> handler)
        {
            while (reader.Read() && handler(reader));
        }

        public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
                handler(reader);
        }

        public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader,T> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
               return handler(reader);
        }

        public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
                reader.Iterate(handler);
        }

        internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, Func<IDataRecord,T> transform, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
            {
                while(reader.Read())
                {
                    yield return transform(reader);
                }
            }
        }

        internal static IEnumerable<Dictionary<string,object>> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
            {
                while (reader.Read())
                {
                    yield return reader.ToDictionary();
                }
            }
        }

        internal static IEnumerable<Dictionary<string, object>> IterateReaderInternal(IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, r => r.ToDictionary(columnNames));

        public static void IterateReaderWhile(this IDbCommand command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = command.ExecuteReader(behavior))
                reader.IterateWhile(handler);
        }

        public static Dictionary<string, object> ToDictionary(this IDataRecord record, HashSet<string> columnNames)
        {
            var e = new Dictionary<string, object>();
            if (columnNames != null && columnNames.Count != 0)
            {
                for (var i = 0; i < record.FieldCount; i++)
                {
                    var n = record.GetName(i);
                    if (columnNames.Contains(n))
                        e.Add(n, record.GetValue(i));
                }
            }
            return e;
        }

        public static Dictionary<string, object> ToDictionary(this IDataRecord record, params string[] columnNames)
        {
            if (columnNames.Length != 0)
                return ToDictionary(record, new HashSet<string>(columnNames));

            var e = new Dictionary<string, object>();
            for (var i = 0; i < record.FieldCount; i++)
            {
                var n = record.GetName(i);
                e.Add(n, record.GetValue(i));
            }
            return e;
        }

        public static Dictionary<string, object> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
        {
            return ToDictionary(record, new HashSet<string>(columnNames));
        }

        public static List<Dictionary<string, object>> ToList(this IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, columnNames).ToList();

        public static List<Dictionary<string, object>> ToList(this IDbCommand command, IEnumerable<string> columnNames)
            => ToList(command, new HashSet<string>(columnNames));

        public static List<Dictionary<string, object>> ToList(this IDbCommand command, params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal(command).ToList()
                : ToList(command, new HashSet<string>(columnNames));

        public static Dictionary<string, object>[] ToArray(this IDbCommand command, HashSet<string> columnNames)
            => IterateReaderInternal(command, columnNames).ToArray();

        public static Dictionary<string, object>[] ToArray(this IDbCommand command, IEnumerable<string> columnNames)
            => ToArray(command, new HashSet<string>(columnNames));

        public static Dictionary<string, object>[] ToArray(this IDbCommand command, params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal(command).ToArray()
                : ToArray(command, new HashSet<string>(columnNames));


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
