﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Base class for developing expressive commands.
	/// Includes methods for use with IDbConnection and IDbCommand types.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection to be used.</typeparam>
	/// <typeparam name="TCommand">The type of the commands generated by the connection.</typeparam>
	/// <typeparam name="TDbType">The DB type enum to use for parameters.</typeparam>
	/// <typeparam name="TThis">The type of this class in order to facilitate proper expressive notation.</typeparam>
	public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
		where TConnection : class, IDbConnection
		where TCommand : class, IDbCommand
		where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
	{
		/// <summary>
		/// The connection factory to use to generate connections and commands.
		/// </summary>
		protected readonly IDbConnectionFactory<TConnection> ConnectionFactory;

		/// <summary>
		/// The connection to execute commands on if not using a connection factory.
		/// </summary>
		protected readonly TConnection Connection;

		/// <summary>
		/// The transaction to execute commands on if not using a connection factory.
		/// </summary>
		protected readonly IDbTransaction Transaction;

		ExpressiveCommandBase(
			CommandType type,
			string command,
			IEnumerable<Param> @params)
		{
			Type = type;
			Command = command ?? throw new ArgumentNullException(nameof(command));
			Params = @params?.ToList() ?? new List<Param>();
			Timeout = CommandTimeout.DEFAULT_SECONDS;
		}

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveCommandBase(
			IDbConnectionFactory<TConnection> connFactory,
			CommandType type,
			string command,
			IEnumerable<Param> @params)
			: this(type, command, @params)
		{
			ConnectionFactory = connFactory ?? throw new ArgumentNullException(nameof(connFactory));
		}

		/// <param name="connection">The connection to execute the command on.</param>
		/// <param name="transaction">The optional transaction to execute the command on.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveCommandBase(
			TConnection connection,
			IDbTransaction transaction,
			CommandType type,
			string command,
			IEnumerable<Param> @params)
			: this(type, command, @params)
		{
			Connection = connection ?? throw new ArgumentNullException(nameof(connection));
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
		public List<Param> Params { get; protected set; }

		/// <summary>
		/// The command timeout value.
		/// </summary>
		public ushort Timeout { get; set; }

		/// <summary>
		/// Adds a parameter to the params list.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="value">The value of the parameter.</param>
		/// <param name="type">The database type of the parameter.</param>
		/// <returns>This instance for use in method chaining.</returns>
		public TThis AddParam(string name, object value, TDbType type)
		{
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
			var p = new Param { Name = name };
			if (value != null) p.Value = value;
			else p.Value = DBNull.Value;

			Params.Add(p);
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
		protected void UsingConnection(Action<TConnection, IDbTransaction> action)
		{
			if (Connection != null)
			{
				action(Connection, Transaction);
			}
			else
			{
				using (var conn = ConnectionFactory.Create())
				{
					action(conn, null);
				}
			}
		}

		/// <summary>
		/// Handles providing the connection for use with the command.
		/// </summary>
		/// <param name="action">The handler for use with the connection.</param>
		protected T UsingConnection<T>(Func<TConnection, IDbTransaction, T> action)
		{
			if (Connection != null)
			{
				return action(Connection, Transaction);
			}
			else
			{
				using (var conn = ConnectionFactory.Create())
				{
					return action(conn, null);
				}
			}
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void Execute(Action<TCommand> handler)
			=> UsingConnection((con, t) =>
			{
				using (var cmd = con.CreateCommand(Type, Command, Timeout))
				{
					var c = cmd as TCommand;
					if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null) c.Transaction = t;
					AddParams(c);
					con.EnsureOpen();
					handler(c);
				}
			});


		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		public T Execute<T>(Func<TCommand, T> transform)
			=> UsingConnection((con, t) =>
			{
				using (var cmd = con.CreateCommand(Type, Command, Timeout))
				{
					var c = cmd as TCommand;
					if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null) c.Transaction = t;
					AddParams(c);
					con.EnsureOpen();
					return transform(c);
				}
			});


		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public object ExecuteReturn()
			=> UsingConnection((con, t) =>
			{
				using (var cmd = con.CreateCommand(Type, Command, Timeout))
				{
					var c = cmd as TCommand;
					if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					if (t != null) c.Transaction = t;
					AddParams(c);
					var returnParameter = c.CreateParameter();
					returnParameter.Direction = ParameterDirection.ReturnValue;
					c.Parameters.Add(returnParameter);
					con.EnsureOpen();
					c.ExecuteNonQuery();
					return returnParameter.Value;
				}
			});

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command but sets up a return parameter and returns that value.
		/// </summary>
		/// <returns>The value from the return parameter.</returns>
		public T ExecuteReturn<T>()
			=> (T)ExecuteReturn();

		/// <summary>
		/// Internal reader for simplifying iteration.  If exposed publicly could potentially hold connections open because an iteration may have not completed.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results of each transformation.</returns>
		protected IEnumerable<T> IterateReaderInternal<T>(Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			TConnection con = Connection ?? ConnectionFactory.Create();
			try
			{
				using (var cmd = con.CreateCommand(Type, Command, Timeout))
				{
					var c = cmd as TCommand;
					if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
					AddParams(c);
					con.EnsureOpen();
					using (var reader = c.ExecuteReader(behavior | CommandBehavior.SingleResult | CommandBehavior.CloseConnection))
					{
						while (reader.Read())
							yield return transform(reader);
					}
				}
			}
			finally
			{
				if (Connection == null) con.Dispose();
			}
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for the data reader.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
			=> Execute(command => command.ExecuteReader(handler, behavior | CommandBehavior.CloseConnection));

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The result of the transform.</returns>
		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> Execute(command => command.ExecuteReader(transform, behavior | CommandBehavior.CloseConnection));

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void IterateReader(Action<IDataRecord> handler)
			=> Execute(command => command.IterateReader(CommandBehavior.SingleResult | CommandBehavior.CloseConnection, handler));

		/// <summary>
		/// Iterates a reader on a command while the handler function returns true.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void IterateReaderWhile(Func<IDataRecord, bool> handler)
			=> Execute(command => command.IterateReaderWhile(handler, CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <returns>The result of the transform.</returns>
		public TResult IterateReader<TEntity, TResult>(
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
			=> Execute(command => command.IterateReader(CommandBehavior.SingleResult | CommandBehavior.CloseConnection, transform, selector));

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Throws if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T First<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).First(), CommandBehavior.SingleRow | CommandBehavior.SingleResult);

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T FirstOrDefault<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).FirstOrDefault(), CommandBehavior.SingleRow | CommandBehavior.SingleResult);

		/// <summary>
		/// Iterates a IDataReader and returns the first result through a transform funciton.  Throws if none or more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T Single<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).Single(), CommandBehavior.SingleResult);

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.  Throws if more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T SingleOrDefault<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).SingleOrDefault(), CommandBehavior.SingleResult);

		/// <summary>
		/// Iterates an IDataReader and returns the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="count">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform limited by the take count.</returns>
		public List<T> Take<T>(int count, Func<IDataRecord, T> transform)
			=> Execute(command => command.Take<T>(count, transform, CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Iterates an IDataReader and skips the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="count">The number of records to skip.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform after the skip count.</returns>
		public List<T> Skip<T>(int count, Func<IDataRecord, T> transform)
			=> Execute(command => command.Skip<T>(count, transform, CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="skip">The number of entries to skip before starting to take results.</param>
		/// <param name="take">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the skip, transform and take operation.</returns>
		public List<T> SkipThenTake<T>(int skip, int take, Func<IDataRecord, T> transform)
			=> Execute(command => command.SkipThenTake<T>(skip, take, transform, CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command.
		/// </summary>
		/// <returns>The integer response from the method. (Records updated.)</returns>
		public int ExecuteNonQuery()
			=> Execute(command => command.ExecuteNonQuery());

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <returns>The varlue returned from the method.</returns>
		public object ExecuteScalar()
			=> Execute(command => command.ExecuteScalar());

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <typeparam name="T">The type expected.</typeparam>
		/// <returns>The varlue returned from the method.</returns>
		public T ExecuteScalar<T>()
			=> (T)ExecuteScalar();

		/// <summary>
		/// Calls ExecuteScalar on the underlying command.
		/// </summary>
		/// <typeparam name="T">The type expected.</typeparam>
		/// <returns>The varlue returned from the method.</returns>
		public T ExecuteScalar<T>(Func<object, T> transform)
			=> transform(ExecuteScalar());

		/// <summary>
		/// Imports all data using an IDataReader into a DataTable.
		/// </summary>
		/// <returns>The resultant DataTabel.</returns>
		public DataTable LoadTable()
			=> Execute(command => command.ToDataTable(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTables.
		/// Calls .NextResult() to check for more results.
		/// </summary>
		/// <returns>The resultant list of DataTables.</returns>
		public List<DataTable> LoadTables()
			=> Execute(command => command.ToDataTables(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection));

		/// <summary>
		/// Converts all IDataRecords into a list using a transform function.
		/// </summary>
		/// <typeparam name="T">The expected return type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The list of transformed records.</returns>
		public List<T> ToList<T>(Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader(record => record.Iterate(transform).ToList(), behavior | CommandBehavior.SingleResult);

		/// <summary>
		/// Converts all IDataRecords into an array using a transform function.
		/// </summary>
		/// <typeparam name="T">The expected return type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The array of transformed records.</returns>
		public T[] ToArray<T>(Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader(record => record.Iterate(transform).ToArray(), behavior | CommandBehavior.SingleResult);

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public QueryResult<Queue<object[]>> Retrieve()
			=> Execute(command => command.Retrieve(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.CloseConnection));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public QueryResult<Queue<object[]>> Retrieve(IEnumerable<int> ordinals)
			=> Execute(command => command.Retrieve(ordinals));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public QueryResult<Queue<object[]>> Retrieve(int n, params int[] others)
			=> Execute(command => command.Retrieve(n, others));

		/// <summary>
		/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="columnNames">The column names to select.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public QueryResult<Queue<object[]>> Retrieve(IEnumerable<string> columnNames)
			=> Execute(command => command.Retrieve(columnNames));

		/// <summary>
		/// Iterates all records within the current result set using an IDataReader and returns the desired results.
		/// DBNull values are left unchanged (retained).
		/// </summary>
		/// <param name="c">The first column name to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining column names to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public QueryResult<Queue<object[]>> Retrieve(string c, params string[] others)
			=> Execute(command => command.Retrieve(c, others));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public IEnumerable<T> Results<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> Execute(command => command.Results<T>(fieldMappingOverrides));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public IEnumerable<T> Results<T>(IEnumerable<(string Field, string Column)> fieldMappingOverrides)
			where T : new()
			=> Execute(command => command.Results<T>(fieldMappingOverrides));

		/// <summary>
		/// Reads the first column from every record and returns the results as a list..
		/// DBNull values are converted to null.
		/// </summary>
		/// <returns>The list of transformed records.</returns>
		public IEnumerable<object> FirstOrdinalResults()
			=> Execute(command => command.FirstOrdinalResults(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection));


		/// <summary>
		/// Reads the first column from every record..
		/// DBNull values are converted to null.
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public IEnumerable<T0> FirstOrdinalResults<T0>()
			=> Execute(command => command.FirstOrdinalResults<T0>(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection));

		/// <summary>
		/// Iterates each record and attempts to map the fields to type T.
		/// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>The enumerable to pull the transformed results from.</returns>
		public IEnumerable<T> Results<T>(params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> Execute(command => command.Results<T>(fieldMappingOverrides));

		/// <summary>
		/// Posts all records to a target block using the transform function.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public void ToTargetBlock<T>(ITargetBlock<T> target, Func<IDataRecord, T> transform)
			=> IterateReaderWhile(r => target.Post(transform(r)));

		/// <summary>
		/// Returns a buffer block that will contain the results.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronusly and all data is acquired before the method returns.
		/// If set to false (default) the data is recieved asynchronously (deferred: data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>The buffer block that will contain the results.</returns>
		public ISourceBlock<T> AsSourceBlock<T>(Func<IDataRecord, T> transform, bool synchronousExecution = false)
		{
			var source = new BufferBlock<T>();
			void i()
			{
				ToTargetBlock(source, transform);
				source.Complete();
			};
			if (synchronousExecution) i();
			else Task.Run(() => i());
			return source;
		}

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronusly and all data is acquired before the method returns.
		/// If set to false (default) the data is recieved asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>A transform block that is recieving the results.</returns>
		public ISourceBlock<T> AsSourceBlock<T>(IEnumerable<(string Field, string Column)> fieldMappingOverrides, bool synchronousExecution = false)
		   where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var cn = x.ColumnNames;
			var q = x.Results(out Action<QueryResult<IEnumerable<object[]>>> deferred);

			void i() => ExecuteReader(reader =>
			{
				// Ignores fields that don't match.
				var columns = reader.GetMatchingOrdinals(cn)
					.OrderBy(c => c.Ordinal)
					.ToArray();

				var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
				deferred(new QueryResult<IEnumerable<object[]>>(
					ordinalValues,
					columns.Select(c => c.Name).ToArray(),
					reader.AsEnumerable(ordinalValues)));

			});

			if (synchronousExecution) i();
			else Task.Run(() => i());
			return q;
		}

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronusly and all data is acquired before the method returns.
		/// If set to false (default) the data is recieved asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>A transform block that is recieving the results.</returns>
		public ISourceBlock<T> AsSourceBlock<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides, bool synchronousExecution = false)
		   where T : new()
			=> AsSourceBlock<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), synchronousExecution);

		/// <summary>
		/// Provides a transform block as the source of records.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>A transform block that is recieving the results.</returns>
		public ISourceBlock<T> AsSourceBlock<T>(params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> AsSourceBlock<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

	}
}
