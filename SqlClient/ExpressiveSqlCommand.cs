using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions.SqlClient
{

    public class ExpressiveSqlCommand : ExpressiveAsyncCommandBase<SqlConnection, SqlCommand, SqlDbType, ExpressiveSqlCommand>
    {
        public ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string name, List<Param> @params = null)
            : base(connFactory, type, name, @params)
        {
        }

        protected ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string name, params Param[] @params)
            : this(connFactory, type, name, @params.ToList())
        {
        }

        protected override void AddParams(SqlCommand command)
        {
            foreach (var p in Params)
            {
                var np = command.Parameters.AddWithValue(p.Name, p.Value);
                if (p.Type.HasValue) np.SqlDbType = p.Type.Value;
            }
        }

        public override async Task ExecuteAsync(Func<SqlCommand, Task> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(
                Type, Command, Timeout))
            {
                AddParams(cmd);
                await con.OpenAsync();
                await handler(cmd);
            }
        }

        public override async Task<T> ExecuteAsync<T>(Func<SqlCommand, Task<T>> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(
                Type, Command, Timeout))
            {
                AddParams(cmd);
                await con.OpenAsync();
                return await handler(cmd);
            }
        }

        public Task ExecuteReaderAsync(Func<SqlDataReader, Task> handler, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await handler(await command.ExecuteReaderAsync(behavior)));

        public Task<T> ExecuteReaderAsync<T>(Func<SqlDataReader, Task<T>> handler, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await handler(await command.ExecuteReaderAsync(behavior)));

        public override Task<int> ExecuteNonQueryAsync()
            => ExecuteAsync(command => command.ExecuteNonQueryAsync());

        public override Task<object> ExecuteScalarAsync()
            => ExecuteAsync(command => command.ExecuteScalarAsync());

        public override Task<List<T>> ToListAsync<T>(Func<IDataRecord, T> transform)
            => ExecuteAsync(command => command.ToListAsync(transform));

        public override Task IterateReaderAsync(Action<IDataRecord> handler, CancellationToken? token = null)
            => ExecuteAsync(command => command.IterateReaderAsync(handler, token));

        public override Task IterateReaderAsyncWhile(Func<IDataRecord, bool> predicate)
            => ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));

    }

}
