using System.Data.Common;

#nullable enable

namespace Open.Database.Extensions.Tests;

// A command made for a transaction belongs to the caller only once it is returned.
// If the provider refuses the transaction, the command that was already created is disposed and the refusal is thrown as it was.
[ExcludeFromCodeCoverage]
public static class TransactionCreateCommandLeakTests
{
	sealed class RejectingCommand : DbCommand
	{
		public int Disposes { get; private set; }

		[AllowNull]
		public override string CommandText { get; set; } = "";
		public override int CommandTimeout { get; set; }
		public override CommandType CommandType { get; set; }
		public override bool DesignTimeVisible { get; set; }
		public override UpdateRowSource UpdatedRowSource { get; set; }
		protected override DbConnection? DbConnection { get; set; }
		protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();

		protected override DbTransaction? DbTransaction
		{
			get => null;
			set => throw new NotSupportedException("The command refuses this transaction.");
		}

		public override void Cancel() { }
		public override int ExecuteNonQuery() => throw new NotSupportedException();
		public override object? ExecuteScalar() => throw new NotSupportedException();
		public override void Prepare() { }
		protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

		protected override void Dispose(bool disposing)
		{
			if (disposing) Disposes++;
			base.Dispose(disposing);
		}
	}

	sealed class RejectingConnection : DbConnection
	{
		public RejectingCommand? Command { get; private set; }

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "rejecting";
		public override string DataSource => "rejecting";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Open;
		public override void ChangeDatabase(string databaseName) { }
		public override void Close() { }
		public override void Open() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new RejectingTransaction(this);
		protected override DbCommand CreateDbCommand() => Command = new RejectingCommand();
	}

	sealed class RejectingTransaction(RejectingConnection connection) : DbTransaction
	{
		public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
		protected override DbConnection DbConnection => connection;
		public override void Commit() { }
		public override void Rollback() { }
	}

	[Fact]
	public static void CreateCommand_OnADbTransaction_TheProviderRefusesTheTransaction_DisposesTheCommand()
	{
		var connection = new RejectingConnection();
		using DbTransaction transaction = connection.BeginTransaction();

		var ex = Assert.Throws<NotSupportedException>(
			() => transaction.CreateCommand(CommandType.Text, "select 1"));

		Assert.Equal("The command refuses this transaction.", ex.Message);
		Assert.Equal(1, connection.Command!.Disposes);
	}

	[Fact]
	public static void CreateCommand_OnAnIDbTransaction_TheProviderRefusesTheTransaction_DisposesTheCommand()
	{
		var connection = new RejectingConnection();
		using DbTransaction transaction = connection.BeginTransaction();

		var ex = Assert.Throws<NotSupportedException>(
			() => ((IDbTransaction)transaction).CreateCommand(CommandType.Text, "select 1"));

		Assert.Equal("The command refuses this transaction.", ex.Message);
		Assert.Equal(1, connection.Command!.Disposes);
	}
}
