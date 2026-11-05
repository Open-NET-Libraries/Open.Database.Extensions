using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A stateful <see cref="IDbConnection"/> / <see cref="IDbCommand"/> substitute pair that records, in order,
/// what the library does to them (<see cref="Calls"/>) and which <see cref="CommandBehavior"/> it asked for
/// (<see cref="Behaviors"/>). It deliberately models nothing a provider would do on its own
/// (for example it does not close the connection when <see cref="CommandBehavior.CloseConnection"/> is requested),
/// so every recorded call is a decision made by the library under test.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class ConnectionProbe
{
	public ConnectionProbe(ConnectionState initial = ConnectionState.Closed, bool dbConnection = false)
	{
		State = initial;
		Command = Substitute.For<IDbCommand>();

		if (dbConnection)
		{
			DbConnection db = Substitute.For<DbConnection>();
			db.OpenAsync(Arg.Any<CancellationToken>()).Returns(_ =>
			{
				Calls.Add("OpenAsync");
				State = ConnectionState.Open;
				return Task.CompletedTask;
			});
			Db = db;
			Connection = db;
		}
		else
		{
			Connection = Substitute.For<IDbConnection>();
			Connection.CreateCommand().Returns(_ =>
			{
				Calls.Add("CreateCommand");
				return Command;
			});
			Connection.When(c => c.Dispose()).Do(_ => Calls.Add("DisposeConnection"));
		}

		Connection.State.Returns(_ => State);
		Connection.When(c => c.Open()).Do(_ =>
		{
			Calls.Add("Open");
			State = ConnectionState.Open;
		});
		Connection.When(c => c.Close()).Do(_ =>
		{
			Calls.Add("Close");
			State = ConnectionState.Closed;
		});

		Command.Connection.Returns(Connection);
		Command.When(c => c.Dispose()).Do(_ => Calls.Add("DisposeCommand"));

		var parameters = Substitute.For<IDataParameterCollection>();
		parameters.When(p => p.Add(Arg.Any<object>())).Do(ci => Parameters.Add((IDbDataParameter)ci.Arg<object>()));
		Command.Parameters.Returns(parameters);
		Command.CreateParameter().Returns(_ => Substitute.For<IDbDataParameter>());

		Command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(ci =>
		{
			Calls.Add("ExecuteReader");
			Behaviors.Add(ci.Arg<CommandBehavior>());
			IDataReader reader = ReaderFactory();
			reader.When(r => r.Dispose()).Do(_ => Calls.Add("DisposeReader"));
			return reader;
		});
		Command.ExecuteNonQuery().Returns(_ =>
		{
			Calls.Add("ExecuteNonQuery");
			return 7;
		});
		Command.ExecuteScalar().Returns(_ =>
		{
			Calls.Add("ExecuteScalar");
			return 42;
		});
	}

	/// <summary>The state the (fake) connection currently reports.</summary>
	public ConnectionState State { get; set; }

	public IDbConnection Connection { get; }

	/// <summary>The same connection as a <see cref="DbConnection"/>; only set when constructed with <c>dbConnection: true</c>.</summary>
	public DbConnection? Db { get; }

	public IDbCommand Command { get; }

	/// <summary>Everything the library did, in order.</summary>
	public List<string> Calls { get; } = [];

	/// <summary>The behavior passed to each <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/> call.</summary>
	public List<CommandBehavior> Behaviors { get; } = [];

	/// <summary>The parameters added to <see cref="Command"/>, in order.</summary>
	public List<IDbDataParameter> Parameters { get; } = [];

	/// <summary>Creates the reader handed out by each ExecuteReader call (one row, one "id" column by default).</summary>
	public Func<IDataReader> ReaderFactory { get; set; }
		= () => FakeReader.Create(["id"], [1]);

	/// <summary><see cref="Calls"/> joined by commas, for compact whole-sequence assertions.</summary>
	public string Log => string.Join(",", Calls);

	/// <summary>A pool whose Take/Give are recorded in <see cref="Calls"/> and which hands out <see cref="Connection"/>.</summary>
	public IDbConnectionPool CreatePool()
	{
		var pool = Substitute.For<IDbConnectionPool>();
		pool.Take().Returns(_ =>
		{
			Calls.Add("Take");
			return Connection;
		});
		pool.When(p => p.Give(Arg.Any<IDbConnection>())).Do(_ => Calls.Add("Give"));
		return pool;
	}

	/// <summary>A factory whose Create is recorded in <see cref="Calls"/> and which hands out <see cref="Connection"/>.</summary>
	public IDbConnectionFactory CreateFactory()
	{
		var factory = Substitute.For<IDbConnectionFactory>();
		factory.Create().Returns(_ =>
		{
			Calls.Add("Create");
			return Connection;
		});
		return factory;
	}
}
