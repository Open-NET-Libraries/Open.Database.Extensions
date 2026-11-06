using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class Transactions
{
	public static (bool Committed, long RowsAfter) Conditional(IDbConnectionFactory<SqliteConnection> connectionFactory, bool shouldCommit)
	{
		#region transaction
		// The factory opens a connection and starts a transaction. Returning true commits; false (or a throw) rolls back.
		bool committed = connectionFactory.Using(connection =>
			connection.ExecuteTransactionConditional(transaction =>
			{
				transaction
					.Command("insert into posts (id, author, title, body, views) values (100, 1, 'Temporary', null, 0)")
					.ExecuteNonQuery();

				return shouldCommit;
			}));
		#endregion

		long rows = connectionFactory.Command("select count(*) from posts where id = 100").ExecuteScalar<long>();
		connectionFactory.Command("delete from posts where id = 100").ExecuteNonQuery();
		return (committed, rows);
	}
}
