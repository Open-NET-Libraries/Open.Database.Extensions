﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Base class for developing expressive commands.
	/// Includes methods for use with IDbConnection and IDbCommand types.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection to be used.</typeparam>
	/// <typeparam name="TCommand">The type of the commands generated by the connection.</typeparam>
	/// <typeparam name="TReader">The type of reader created by the command.</typeparam>
	/// <typeparam name="TDbType">The DB type enum to use for parameters.</typeparam>
	/// <typeparam name="TThis">The type of this class in order to facilitate proper expressive notation.</typeparam>
	public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
		: IExecuteCommand<TCommand>, IExecuteReader<TReader>
		where TConnection : class, IDbConnection
		where TCommand : class, IDbCommand
		where TReader : class, IDataReader
		where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TReader, TDbType, TThis>
	{
		/// <summary>
		/// Utility for simplifying param concatenation.
		/// </summary>
		/// <typeparam name="T">The type of the enumerable.</typeparam>
		/// <param name="first">The first value.</param>
		/// <param name="remaining">The remaining values.</param>
		/// <returns></returns>
		protected static IEnumerable<T> Concat<T>(T first, ICollection<T> remaining)
			=> (remaining == null || remaining.Count == 0) ? new T[] { first } : Enumerable.Repeat(first, 1).Concat(remaining);

		/// <summary>
		/// The connection factory to use to generate connections and commands.
		/// </summary>
		protected IDbConnectionFactory<TConnection>? ConnectionFactory { get; }

		/// <summary>
		/// The connection to execute commands on if not using a connection factory.
		/// </summary>
		protected TConnection? Connection { get; }

		/// <summary>
		/// The transaction to execute commands on if not using a connection factory.
		/// </summary>
		protected IDbTransaction? Transaction { get; }

		ExpressiveCommandBase(
			CommandType type,
			string command,
			IEnumerable<Param>? @params)
		{
			Type = type;
			Command = command ?? throw new ArgumentNullException(nameof(command));
			Params = @params?.ToList() ?? new List<Param>();
			Timeout = CommandTimeout.DEFAULT_SECONDS;
		}

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveCommandBase(
			IDbConnectionFactory<TConnection> connFactory,
			CommandType type,
			string command,
			IEnumerable<Param>? @params)
			: this(type, command, @params)
		{
			ConnectionFactory = connFactory ?? throw new ArgumentNullException(nameof(connFactory));
			Contract.EndContractBlock();
		}

		/// <param name="connection">The connection to execute the command on.</param>
		/// <param name="transaction">The optional transaction to execute the command on.</param>
		/// <param name="type">The command type.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveCommandBase(
			TConnection connection,
			IDbTransaction? transaction,
			CommandType type,
			string command,
			IEnumerable<Param>? @params)
			: this(type, command, @params)
		{
			Connection = connection ?? throw new ArgumentNullException(nameof(connection));
			Contract.EndContractBlock();
			Transaction = transaction;
		}

		/// <summary>
		/// The command text or procedure name to use.
		/// </summary>
		public string Command { get; set; }

		/// <summary>
		/// The command type.
		/// </summary>
		public CommandType Type { get; set; }

		/// <summary>
		/// The list of params to apply to the command before execution.
		/// </summary>
		public List<Param> Params { get; }

		/// <summary>
		/// The command timeout value.
		/// </summary>
		public ushort Timeout { get; set; }


		/// <summary>
		/// The optional cancellation token to use with supported methods.
		/// </summary>
		public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

		CancellationToken IExecuteReader.CancellationToken => throw new NotImplementedException();

		/// <summary>
		/// Sets the cancellation token.
		/// </summary>
		public TThis UseCancellationToken(CancellationToken token)
		{
			CancellationToken = token;
			return (TThis)this;
		}

		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The database type of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam(string name, object value, TDbType type)
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			Params.Add(new Param
			{
				Name = name,
				Value = value,
				Type = type
			});

			return (TThis)this;
		}

		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam(string name, object value)
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			Params.Add(new Param
			{
				Name = name,
				Value = value ?? DBNull.Value
			});
			return (TThis)this;
		}


		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The database type of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam<T>(string name, T? value, TDbType type)
			where T : struct
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			var p = new Param { Name = name, Type = type };
			if (value.HasValue) p.Value = value.Value;
			else p.Value = DBNull.Value;

			Params.Add(p);
			return (TThis)this;
		}

		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam<T>(string name, T? value)
			where T : struct
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			Contract.EndContractBlock();

			var p = new Param { Name = name };
			if (value.HasValue) p.Value = value.Value;
			else p.Value = DBNull.Value;

			Params.Add(p);
			return (TThis)this;
		}

		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam(string name)
		{
			if (name is null) throw new ArgumentNullException(nameof(name));
			else if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Parameter names cannot be empty or white space.", nameof(name));
			Contract.EndContractBlock();

			Params.Add(new Param
			{
				Name = name
			});

			return (TThis)this;
		}


		/// <summary>
		/// Conditionally adds a parameter to the params list.
		/// </summary>
		/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParamIf<T>(bool condition, string name, T? value)
			where T : struct
			=> condition ? AddParam(name, value) : (TThis)this;

		/// <summary>
		/// Conditionally adds a parameter to the params list.
		/// </summary>
		/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParamIf(bool condition, string name, object value)
			=> condition ? AddParam(name, value) : (TThis)this;

		/// <summary>
		/// Conditionally adds a parameter to the params list.
		/// </summary>
		/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The database type of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParamIf(bool condition, string name, object value, TDbType type)
			=> condition ? AddParam(name, value, type) : (TThis)this;

		/// <summary>
		/// Conditionally adds a parameter to the params list.
		/// </summary>
		/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The database type of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParamIf<T>(bool condition, string name, T? value, TDbType type)
			where T : struct
			=> condition ? AddParam(name, value, type) : (TThis)this;

		/// <summary>
		/// Conditionally adds a parameter to the params list.
		/// </summary>
		/// <param name="condition">The condition to add the param by.  Only adds if true.</param>
		/// <param name="name">The name of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParamIf(bool condition, string name)
			=> condition ? AddParam(name) : (TThis)this;

		/// <summary>
		/// Sets the timeout value.
		/// </summary>
		/// <param name="seconds">The number of seconds to wait before the connection times out.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis SetTimeout(ushort seconds)
		{
			Timeout = seconds;
			return (TThis)this;
		}

		/// <summary>
		/// Handles adding the list of parameters to a new command.
		/// </summary>
		/// <param name="command">The command to add parameters to.</param>
		protected abstract void AddParams(TCommand command);

		/// <summary>
		/// Handles providing the connection for use with the command.
		/// </summary>
		/// <param name="action">The handler for use with the connection.</param>
		protected void UsingConnection(Action<TConnection, IDbTransaction?> action)
		{
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			if (Connection != null)
			{
				action(Connection, Transaction);
			}
			else
			{
				// The construction configuration will only allow either the Connection or the ConnectionFactory to be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
				using var conn = ConnectionFactory.Create();
#pragma warning restore CS8602 // Dereference of a possibly null reference.
				action(conn, null);
			}
		}

		/// <summary>
		/// Handles providing the connection for use with the command.
		/// </summary>
		/// <param name="action">The handler for use with the connection.</param>
		protected T UsingConnection<T>(Func<TConnection, IDbTransaction?, T> action)
		{
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			if (Connection != null)
				return action(Connection, Transaction);

			// The construction configuration will only allow either the Connection or the ConnectionFactory to be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
			using var conn = ConnectionFactory.Create();
#pragma warning restore CS8602 // Dereference of a possibly null reference.
			return action(conn, null);
		}

		/// <summary>
		/// Handles providing the connection for use with the command.
		/// </summary>
		/// <param name="action">The handler for use with the connection.</param>
		protected async ValueTask UsingConnectionAsync(Func<TConnection, IDbTransaction?, ValueTask> action)
		{
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			if (Connection != null)
			{
				await action(Connection, Transaction);
			}
			else
			{
				// The construction configuration will only allow either the Connection or the ConnectionFactory to be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
				using var conn = ConnectionFactory.Create();
#pragma warning restore CS8602 // Dereference of a possibly null reference.
				await action(conn, null);
			}
		}

		/// <summary>
		/// Handles providing the connection for use with the command.
		/// </summary>
		/// <param name="action">The handler for use with the connection.</param>
		protected async ValueTask<T> UsingConnectionAsync<T>(Func<TConnection, IDbTransaction?, ValueTask<T>> action)
		{
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			if (Connection != null)
			{
				return await action(Connection, Transaction);
			}
			else
			{
				// The construction configuration will only allow either the Connection or the ConnectionFactory to be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
				using var conn = ConnectionFactory.Create();
#pragma warning restore CS8602 // Dereference of a possibly null reference.
				return await action(conn, null);
			}
		}

		/// <inheritdocs />
		public void Execute(Action<TCommand> action)
		{
			if (action is null) throw new ArgumentNullException(nameof(action));
			Contract.EndContractBlock();

			UsingConnection((con, t) =>
			{
				var state = con.EnsureOpen(); // MUST occur before command creation as some DbCommands require it.
				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null)
						c.Transaction = t;

					AddParams(c);
					action(c);
				}
				finally
				{
					if (state == ConnectionState.Closed) con.Close();
				}
			});
		}

		/// <inheritdocs />
		public T Execute<T>(Func<TCommand, T> transform)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return UsingConnection((con, t) =>
			{
				var state = con.EnsureOpen(); // MUST occur before command creation as some DbCommands require it.
				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null)
						c.Transaction = t;
					AddParams(c);

					return transform(c);
				}
				finally
				{
					if (state == ConnectionState.Closed) con.Close();
				}
			});

		}

		/// <inheritdocs />
		public virtual ValueTask ExecuteAsync(Func<TCommand, ValueTask> handler)
		{
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			CancellationToken.ThrowIfCancellationRequested(); // Since cancelled awaited tasks throw, we will follow the same pattern here.

			return UsingConnectionAsync(async (con, _) =>
			{
				// MUST occur before command creation as some DbCommands require it.
				var state = con is DbConnection dbc
					? await dbc.EnsureOpenAsync(CancellationToken)
					: con.EnsureOpen();

				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException(
							$"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");

					AddParams(c);
					await handler(c).ConfigureAwait(false);
				}
				finally
				{
					if (state == ConnectionState.Closed)
						con.Close();
				}
			});
		}

		/// <inheritdocs />
		public virtual ValueTask<T> ExecuteAsync<T>(Func<TCommand, ValueTask<T>> transform)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			CancellationToken.ThrowIfCancellationRequested(); // Since cancelled awaited tasks throw, we will follow the same pattern here.

			return UsingConnectionAsync(async (con, _) =>
			{
				// MUST occur before command creation as some DbCommands require it.
				var state = con is DbConnection dbc
					? await dbc.EnsureOpenAsync(CancellationToken)
					: con.EnsureOpen();

				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");

					AddParams(c);
					return await transform(c).ConfigureAwait(false);
				}
				finally
				{
					if (state == ConnectionState.Closed)
						con.Close();
				}
			});
		}

		void IExecuteCommand.Execute(Action<IDbCommand> action)
			=> Execute(command => action(command));

		T IExecuteCommand.Execute<T>(Func<IDbCommand, T> transform)
			=> Execute(command => transform(command));

		ValueTask IExecuteCommand.ExecuteAsync(Func<IDbCommand, ValueTask> handler)
			=> ExecuteAsync(command => handler(command));

		ValueTask<T> IExecuteCommand.ExecuteAsync<T>(Func<IDbCommand, ValueTask<T>> transform)
			=> ExecuteAsync(command => transform(command));

		/// <summary>
		/// Validates and properly acquires the expected type of the reader.
		/// </summary>
		/// <typeparam name="TActual">The actual type of the reader.</typeparam>
		/// <param name="reader">The reader to cast.</param>
		/// <returns>The expected reader.</returns>
		protected static TReader EnsureReaderType<TActual>(TActual reader)
			where TActual : IDataReader
			=> reader is TReader r ? r : throw new InvalidCastException($"Expected reader type of ({typeof(TReader)}).  Actual: ({reader.GetType()})");

		/// <inhericdoc />
		public void ExecuteReader(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			Execute(command => command.ExecuteReader(reader => handler(EnsureReaderType(reader)), behavior));
		}

		/// <inhericdoc />
		public T ExecuteReader<T>(Func<TReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			return Execute(command => command.ExecuteReader(reader => transform(EnsureReaderType(reader)), behavior));
		}

		/// <inhericdoc />
		public ValueTask ExecuteReaderAsync(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			return ExecuteAsync(command => command.ExecuteReaderAsync(ExecuteReaderAsyncCore, behavior, CancellationToken));

			ValueTask ExecuteReaderAsyncCore(IDataReader reader)
			{
				handler(EnsureReaderType(reader));
				return new ValueTask();
			}
		}

		/// <inhericdoc />
		public ValueTask<T> ExecuteReaderAsync<T>(Func<TReader, T> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			return ExecuteAsync(command => command.ExecuteReaderAsync(ExecuteReaderAsyncCore, behavior, CancellationToken));

			ValueTask<T> ExecuteReaderAsyncCore(IDataReader reader)
				=> new ValueTask<T>(handler(EnsureReaderType(reader)));
		}

		/// <inhericdoc />
		public ValueTask ExecuteReaderAsync(Func<TReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			return ExecuteAsync(command => command.ExecuteReaderAsync(reader => handler(EnsureReaderType(reader)), behavior, CancellationToken));
		}


		void IExecuteReader.ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior)
			=> ExecuteReader(reader => handler(reader), behavior);

		T IExecuteReader.ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior)
			=> ExecuteReader(reader => transform(reader), behavior);

		ValueTask IExecuteReader.ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior)
			=> ExecuteReaderAsync(reader => handler(reader), behavior);

		ValueTask<T> IExecuteReader.ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior)
			=> ExecuteReaderAsync(reader => transform(reader), behavior);

		/// <inhericdoc />
		public ValueTask<T> ExecuteReaderAsync<T>(Func<TReader, ValueTask<T>> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (Connection == null || Connection.State == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			return ExecuteAsync(command => command.ExecuteReaderAsync(reader => handler(EnsureReaderType(reader)), behavior, CancellationToken));
		}

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public object ExecuteReturn()
			=> UsingConnection((con, t) =>
			{
				var state = con.EnsureOpen(); // MUST occur before command creation as some DbCommands require it.
				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null)
						c.Transaction = t;

					AddParams(c);
					var returnParameter = c.AddReturnParameter();

					c.ExecuteNonQuery();
					return returnParameter.Value;
				}
				finally
				{
					if (state == ConnectionState.Closed) con.Close();
				}
			});

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public T ExecuteReturn<T>()
			=> (T)ExecuteReturn();

		/// <summary>
		/// Calls ExecuteNonQueryAsync on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public ValueTask<object> ExecuteReturnAsync()
		{
			CancellationToken.ThrowIfCancellationRequested();

			return UsingConnectionAsync(async (con, t) =>
			{
				// MUST occur before command creation as some DbCommands require it.
				var state = con is DbConnection dbc
					? await dbc.EnsureOpenAsync(CancellationToken)
					: con.EnsureOpen();

				try
				{
					using var cmd = con.CreateCommand(Type, Command, Timeout);
					if (!(cmd is TCommand c))
						throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");

					AddParams(c);
					var returnParameter = c.AddReturnParameter();

					if (c is DbCommand dbCommand)
						await dbCommand.ExecuteNonQueryAsync(CancellationToken).ConfigureAwait(false);
					else
						c.ExecuteNonQuery();

					return returnParameter.Value;
				}
				finally
				{
					if (state == ConnectionState.Closed)
						con.Close();
				}
			});
		}

		/// <summary>
		/// Calls ExecuteNonQueryAsync on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public async ValueTask<T> ExecuteReturnAsync<T>()
			=> (T)await ExecuteReturnAsync().ConfigureAwait(false);

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command.
		/// </summary>
		/// <returns>The integer response from the method. (Records updated.)</returns>
		public int ExecuteNonQuery()
			=> Execute(command => command.ExecuteNonQuery());

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <returns>The value returned from the method.</returns>
		public object ExecuteScalar()
			=> Execute(command => command.ExecuteScalar());

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <typeparam name="T">The type expected.</typeparam>
		/// <returns>The value returned from the method.</returns>
		public T ExecuteScalar<T>()
			=> (T)ExecuteScalar();

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <typeparam name="T">The type expected.</typeparam>
		/// <returns>The value returned from the method.</returns>
		public T ExecuteScalar<T>(Func<object, T> transform)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return transform(ExecuteScalar());
		}
	}
}
