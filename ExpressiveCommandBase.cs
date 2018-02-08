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
		/// THe connection factory to use to generate connections and commands.
		/// </summary>
		protected readonly IDbConnectionFactory<TConnection> ConnectionFactory;

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveCommandBase(
			IDbConnectionFactory<TConnection> connFactory,
			CommandType type,
			string command,
			List<Param> @params = null)
		{
			ConnectionFactory = connFactory ?? throw new ArgumentNullException(nameof(connFactory));
			Type = type;
			Command = command ?? throw new ArgumentNullException(nameof(command));
			Params = @params ?? new List<Param>();
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
			params Param[] @params)
			: this(connFactory, type, command, @params.ToList())
		{

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
		/// <param name="command"></param>
		protected abstract void AddParams(TCommand command);

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void Execute(Action<TCommand> handler)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				var c = cmd as TCommand;
				if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
				AddParams(c);
				con.Open();
				handler(c);
			}
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		public T Execute<T>(Func<TCommand, T> transform)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				var c = cmd as TCommand;
				if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
				AddParams(c);
				con.Open();
				return transform(c);
			}
		}

		/// <summary>
		/// Internal reader for simplifying iteration.  If exposed publicly could potentially hold connections open because an iteration may have not completed.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The results of each transformation.</returns>
		protected IEnumerable<T> IterateReaderInternal<T>(Func<IDataRecord, T> transform)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				var c = cmd as TCommand;
				if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
				AddParams(c);
				con.Open();
				using (var reader = c.ExecuteReader(CommandBehavior.CloseConnection))
				{
					while (reader.Read())
						yield return transform(reader);
				}
			}
		}

        /// <summary>
        /// Executes a reader on a command with a handler function.
        /// </summary>
        /// <param name="handler">The handler function for the data reader.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
			=> Execute(command => command.ExecuteReader(handler, behavior));

        /// <summary>
        /// Executes a reader on a command with a transform function.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function for each IDataRecord.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        /// <returns>The result of the transform.</returns>
        public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> Execute(command => command.ExecuteReader(transform, behavior));

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void IterateReader(Action<IDataRecord> handler)
			=> Execute(command => command.IterateReader(handler));

		/// <summary>
		/// Iterates a reader on a command while the handler function returns true.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public void IterateReaderWhile(Func<IDataRecord, bool> handler)
			=> Execute(command => command.IterateReaderWhile(handler));

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
			=> Execute(command => command.IterateReader(transform, selector));

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Throws if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T First<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).First());

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T FirstOrDefault<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).FirstOrDefault());

		/// <summary>
		/// Iterates a IDataReader and returns the first result through a transform funciton.  Throws if none or more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T Single<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).Single());

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform funciton.  Returns default(T) if none.  Throws if more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The value from the transform.</returns>
		public T SingleOrDefault<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(reader => reader.Iterate(transform).SingleOrDefault());

		/// <summary>
		/// Iterates an IDataReader and returns the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="count">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform limited by the take count.</returns>
		public List<T> Take<T>(int count, Func<IDataRecord, T> transform)
			=> Execute(command => command.Take<T>(count, transform));

		/// <summary>
		/// Iterates an IDataReader and skips the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="count">The number of records to skip.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the transform after the skip count.</returns>
		public List<T> Skip<T>(int count, Func<IDataRecord, T> transform)
			=> Execute(command => command.Skip<T>(count, transform));

		/// <summary>
		/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="skip">The number of entries to skip before starting to take results.</param>
		/// <param name="take">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>The results from the skip, transform and take operation.</returns>
		public List<T> SkipThenTake<T>(int skip, int take, Func<IDataRecord, T> transform)
			=> Execute(command => command.SkipThenTake<T>(skip, take, transform));

		/// <summary>
		/// Calls ExecuteNonQuery on the underlying command.
		/// </summary>
		/// <returns>The integer responise from the method.</returns>
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
		/// Imports all data using an IDataReader into a DataTable.
		/// </summary>
		/// <returns>The resultant DataTabel.</returns>
		public DataTable LoadTable()
			=> Execute(command => command.ToDataTable());

		/// <summary>
		/// Internal reader for simplifying iteration.  If exposed publicly could potentially hold connections open because an iteration may have not completed.
		/// </summary>
		/// <returns>The enumerable with the data records stored in a dictionary..</returns>
		protected IEnumerable<Dictionary<string, object>> IterateReaderInternal()
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                con.Open();
                using (var reader = c.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    if(reader.Read())
                    {
                        yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes);
                        while (reader.Read())
                            yield return reader.ToDictionary(columnIndexes);
                    }
                }
            }
        }

		/// <summary>
		/// Internal reader for simplifying iteration.  If exposed publicly could potentially hold connections open because an iteration may have not completed.
		/// </summary>
		/// <returns>The enumerable with the data records stored in a dictionary.  Only the column names requested will be returned.</returns>
		protected IEnumerable<Dictionary<string, object>> IterateReaderInternal(ISet<string> columnNames)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                con.Open();
                using (var reader = c.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    if (reader.Read())
                    {
                        yield return reader.ToDictionaryOutIndexes(out IReadOnlyList<KeyValuePair<int, string>> columnIndexes, columnNames);
                        while (reader.Read())
                            yield return reader.ToDictionary(columnIndexes);
                    }
                }
            }
        }

        /// <summary>
        /// Converts all IDataRecords into a list using a transform function.
        /// </summary>
        /// <typeparam name="T">The expected return type.</typeparam>
        /// <param name="transform">The transform function.</param>
        /// <returns>The list of transformed records.</returns>
        public List<T> ToList<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(record => record.Iterate(transform).ToList());

		/// <summary>
		/// Converts all IDataRecords into an array using a transform function.
		/// </summary>
		/// <typeparam name="T">The expected return type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <returns>The array of transformed records.</returns>
		public T[] ToArray<T>(Func<IDataRecord, T> transform)
			=> ExecuteReader(record => record.Iterate(transform).ToArray());

		/// <summary>
		/// Returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>The list of results.</returns>
		public List<Dictionary<string, object>> Retrieve(ISet<string> columnNames)
			=> IterateReaderInternal(columnNames).ToList();

		/// <summary>
		/// Returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>The list of results.</returns>
		public List<Dictionary<string, object>> Retrieve(IEnumerable<string> columnNames)
			=> Retrieve(new HashSet<string>(columnNames));

		/// <summary>
		/// Returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>The list of results.</returns>
		public List<Dictionary<string, object>> Retrieve(params string[] columnNames)
			=> columnNames.Length == 0
				? IterateReaderInternal().ToList()
				: Retrieve(new HashSet<string>(columnNames));

		/// <summary>
		/// Posts all records to a target block using the transform function.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public void ToTargetBlock<T>(Func<IDataRecord, T> transform, ITargetBlock<T> target)
			=> IterateReaderWhile(r => target.Post(transform(r)));

		/// <summary>
		/// Returns a buffer block that will contain the results.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronusly and all data is acquired before the method returns.
		/// If set to false (default) the data is recieved asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>The buffer block that will contain the results.</returns>
		public ISourceBlock<T> AsSourceBlock<T>(Func<IDataRecord, T> transform, bool synchronousExecution = false)
		{
			var source = new BufferBlock<T>();
			void i()
			{
				ToTargetBlock(transform, source);
				source.Complete();
			};
			if (synchronousExecution) i();
			else Task.Run(() => i());
			return source;
		}

        /// <summary>
        /// Iterates each record and attempts to map the fields to type T.
        /// Data is temporarily stored (buffered in entirety) in a queue of dictionaries before applying the transform for each iteration.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>The enumerable to pull the transformed results from.</returns>
        public IEnumerable<T> Results<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var n = x.ColumnNames;

			// Use a queue so that when each item is subsequently enumerated, the reference is removed and memory is progressively cleaned up.
			var q = new Queue<Dictionary<string, object>>();
			foreach (var e in IterateReaderInternal(n))
				q.Enqueue(e);

			return x.Transform(q);
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
        public ISourceBlock<T> ResultsBlock<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides, bool synchronousExecution = false)
		   where T : new()
		{
			var x = new Transformer<T>();
			var n = x.ColumnNames;
			var q = new TransformBlock<Dictionary<string, object>, T>(e => x.TransformAndClear(e));
            void i()
			{
                IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
                ToTargetBlock(r => columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, n)
                    : r.ToDictionary(columnIndexes), q);
				q.Complete();
			};
			if (synchronousExecution) i();
			else Task.Run(() => i());
			return q;
		}

        /// <summary>
        /// Provides a transform block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="synchronousExecution">By default the command is deferred.
        /// If set to true, the command runs synchronusly and all data is acquired before the method returns.
        /// If set to false (default) the data is recieved asynchronously (data will be subsequently posted) and the source block (transform) can be completed early.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public ISourceBlock<T> ResultsBlock<T>(bool synchronousExecution = false)
           where T : new()
            => ResultsBlock<T>(null, synchronousExecution);

    }
}
