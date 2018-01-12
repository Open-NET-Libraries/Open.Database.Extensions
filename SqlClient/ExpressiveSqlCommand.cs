using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Database.Extensions.SqlClient
{

	public class ExpressiveSqlCommand : ExpressiveCommandBase<SqlConnection, SqlDbType, ExpressiveSqlCommand>
	{
		public ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string name, List<Param> @params = null) : base(connFactory, type, name, @params)
		{
		}

		protected ExpressiveSqlCommand(IDbConnectionFactory<SqlConnection> connFactory, CommandType type, string name, params Param[] @params) : this(connFactory, type, name, @params.ToList())
		{
		}

		protected override void AddParams(IDbCommand command)
		{
			var c = (SqlCommand)command;
			foreach (var p in Params)
			{
				var np = c.Parameters.AddWithValue(p.Name, p.Value);
				if (p.Type.HasValue) np.SqlDbType = p.Type.Value;
			}
		}

		public async Task<int> ExecuteNonQueryAsync()
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(
				Type, Command, Timeout))
			{
				AddParams(cmd);
				await con.OpenAsync();
				return await cmd.ExecuteNonQueryAsync();
			}
		}

		public async Task<object> ExecuteScalarAsync()
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(
				Type, Command, Timeout))
			{
				AddParams(cmd);
				await con.OpenAsync();
				return await cmd.ExecuteScalarAsync();
			}
		}

		public async Task<T> ExecuteScalarAsync<T>()
		{
			return (T)(await ExecuteScalarAsync());
		}

		public async Task IterateReaderAsync(Action<IDataRecord> handler)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				await con.OpenAsync();
				using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
					while (await reader.ReadAsync())
						handler(reader);
			}
		}

		public async Task IterateReaderAsync<T>(
			Func<IDataRecord, T> transform,
			Action<T> whileWaitingForNext)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				await con.OpenAsync();
				using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
					await reader.IterateReaderAsync(transform, whileWaitingForNext);
			}
		}

		public async Task<List<T>> ToListAsync<T>(Func<IDataRecord, T> transform)
		{
			var list = new List<T>();
			await IterateReaderAsync(
				transform,
				t => list.Add(t));
			return list;
		}

		public Task<List<T>> ToListAsync<T>()
			where T : new()
		{
			var x = new Transformer<T>();
			return ToListAsync(record => x.Transform(record));
		}

	}

}
