
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.SqlClient
{
    public static class Extensions
    {
        public static IDbDataParameter AddParameterType(this IDbCommand target, string name, SqlDbType value)
        {
            var c = (SqlParameter)target.CreateParameter();
            c.ParameterName = name;
            c.SqlDbType = value;
            target.Parameters.Add(c);
            return c;
        }

        public static SqlCommand CreateCommand(this SqlConnection conn,
            CommandType type, string commandText, int secondsTimeout = 30)
        {
            var command = conn.CreateCommand();
            command.CommandType = type;
            command.CommandText = commandText;
            command.CommandTimeout = secondsTimeout;

            return command;
        }



        public static async Task ToTargetBlock<T>(this SqlDataReader reader,
            Func<IDataRecord, T> transform,
            ITargetBlock<T> target)
        {
            while (await reader.ReadAsync() && target.Post(transform(reader))) ;
            target.Complete();
        }

        public static async Task<List<T>> ToListAsync<T>(this SqlDataReader reader,
            Func<IDataRecord, T> transform)
        {
            var list = new List<T>();
            while (await reader.ReadAsync()) list.Add(transform(reader));
            return list;
        }

        public static async Task<List<T>> ToListAsync<T>(this SqlCommand command,
            Func<IDataRecord, T> transform)
        {
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                return await reader.ToListAsync(transform);
        }

        public static async Task IterateReaderAsync<T>(this SqlCommand command,
            Func<IDataRecord, T> transform,
            ITargetBlock<T> target)
        {
            using (var reader = await command.ExecuteReaderAsync())
                await reader.ToTargetBlock(transform, target);
        }

        public static async Task<List<Dictionary<string, object>>> ToListAsync(this SqlCommand command, HashSet<string> columnNames)
        {
            var list = new List<Dictionary<string, object>>();
            await IterateReaderAsync(command, r => list.Add(r.ToDictionary(columnNames)));
            return list;
        }

        public static Task<List<Dictionary<string, object>>> ToListAsync(this SqlCommand command, IEnumerable<string> columnNames)
            => ToListAsync(command, new HashSet<string>(columnNames));

        public static async Task<List<Dictionary<string, object>>> ToListAsync(this SqlCommand command, params string[] columnNames)
        {
            // Probably an unnecessary check, but need to be sure.
            if (columnNames.Length != 0)
                return await ToListAsync(command, new HashSet<string>(columnNames));

            var list = new List<Dictionary<string, object>>();
            await IterateReaderAsync(command, r => list.Add(r.ToDictionary()));
            return list;
        }

        public static async Task IterateReaderAsync(this SqlCommand command, Action<IDataRecord> handler, CancellationToken? token = null)
        {
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                if (token.HasValue)
                {
                    var t = token.Value;
                    while (!t.IsCancellationRequested && await reader.ReadAsync())
                        handler(reader);
                }
                else
                {
                    while (await reader.ReadAsync())
                        handler(reader);
                }
            }
        }

        public static async Task IterateReaderAsyncWhile(this SqlCommand command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.CloseConnection)
        {
            using (var reader = await command.ExecuteReaderAsync(behavior))
                while (await reader.ReadAsync() && handler(reader));
        }

        public static ISourceBlock<T> AsSourceBlock<T>(this SqlDataReader reader,
            Func<IDataRecord, T> transform)
        {
            var source = new BufferBlock<T>();
            ToTargetBlock(reader, transform, source).ConfigureAwait(false);
            return source;
        }

        public static ISourceBlock<T> AsSourceBlock<T>(this SqlCommand command,
            Func<IDataRecord, T> transform)
        {
            var source = new BufferBlock<T>();
            Task.Run(async () =>
            {
                using (var reader = await command.ExecuteReaderAsync())
                    await ToTargetBlock(reader, transform, source);
            });
            return source;
        }

        public static ExpressiveSqlCommand Command(
            this IDbConnectionFactory<SqlConnection> target,
            string command, CommandType type = CommandType.Text)
        {
            return new ExpressiveSqlCommand(target, type, command);
        }

        public static ExpressiveSqlCommand StoredProcedure(
            this IDbConnectionFactory<SqlConnection> target,
            string command)
        {
            return new ExpressiveSqlCommand(target, CommandType.StoredProcedure, command);
        }
    }
}
