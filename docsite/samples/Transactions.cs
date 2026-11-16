using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class Transactions
{
	public static async Task<(bool Committed, long RowsAfter)> ConditionalAsync(
		IDbConnectionFactory<SqliteConnection> connectionFactory,
		bool shouldCommit,
		CancellationToken cancellationToken)
	{
		#region transaction
		// The factory creates the connection;
		// ExecuteTransactionConditionalAsync opens it if it is closed
		// and starts the transaction.
		// Returning true commits; false (or a throw) rolls back.
		// Here the caller decides: shouldCommit is a bool passed in.
		bool committed = await connectionFactory.UsingAsync(connection
			=> connection.ExecuteTransactionConditionalAsync(
				async transaction =>
				{
					await transaction
						.Command("""
							insert into posts (id, author, title, body, views)
							values (100, 1, 'Temporary', null, 0)
							""")
						.ExecuteNonQueryAsync(cancellationToken);

					return shouldCommit;
				},
				cancellationToken));
		#endregion

		long rows = await connectionFactory
			.Command("select count(*) from posts where id = 100")
			.ExecuteScalarAsync<long>(cancellationToken);
		await connectionFactory
			.Command("delete from posts where id = 100")
			.ExecuteNonQueryAsync(cancellationToken);
		return (committed, rows);
	}

	public static async Task<long> Async(IDbConnectionFactory<SqliteConnection> connectionFactory, CancellationToken cancellationToken)
	{
		#region transaction_async
		// The token is checked after the action returns:
		// a token cancelled by then never commits.
		// The commit itself is sent without the token.
		await connectionFactory.UsingAsync(connection
			=> connection.ExecuteTransactionAsync(async transaction =>
			{
				await transaction
					.Command("""
						insert into posts (id, author, title, body, views)
						values (100, 1, 'Temporary', null, 0)
						""")
					.ExecuteNonQueryAsync(cancellationToken);
			}, cancellationToken));
		#endregion

		long rows = await connectionFactory
			.Command("select count(*) from posts where id = 100")
			.ExecuteScalarAsync<long>(cancellationToken);
		await connectionFactory
			.Command("delete from posts where id = 100")
			.ExecuteNonQueryAsync(cancellationToken);
		return rows;
	}
}

// Check-only (not shown in the documentation): records the token each commit of an async transaction receives.
public sealed class CommitTokenConnection(string connectionString) : SqliteConnection(connectionString)
{
	public List<CancellationToken> CommitTokens { get; } = [];

	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
		=> new RecordingTransaction(base.BeginDbTransaction(isolationLevel), this);

	sealed class RecordingTransaction(DbTransaction inner, CommitTokenConnection owner) : DbTransaction
	{
		public override IsolationLevel IsolationLevel => inner.IsolationLevel;
		protected override DbConnection? DbConnection => owner;
		public override void Commit() => inner.Commit();
		public override void Rollback() => inner.Rollback();

		public override Task CommitAsync(CancellationToken cancellationToken)
		{
			owner.CommitTokens.Add(cancellationToken);
			return inner.CommitAsync(cancellationToken);
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) inner.Dispose();
			base.Dispose(disposing);
		}
	}
}
