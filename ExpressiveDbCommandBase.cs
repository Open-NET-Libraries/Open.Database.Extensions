﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
    /// <summary>
    /// An abstraction for executing commands on a database using best practices and simplified expressive syntax.
    /// </summary>
    public abstract class ExpressiveDbCommandBase<TConnection, TCommand, TDbType, TThis>
            : ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
            where TConnection : DbConnection
            where TCommand : DbCommand
            where TDbType : struct
            where TThis : ExpressiveDbCommandBase<TConnection, TCommand, TDbType, TThis>
    {
        /// <param name="connFactory">The factory to generate connections from.</param>
        /// <param name="type">The command type>.</param>
        /// <param name="command">The SQL command.</param>
        /// <param name="params">The list of params</param>
        public ExpressiveDbCommandBase(
			IDbConnectionFactory<TConnection> connFactory,
			CommandType type,
			string command,
			List<Param> @params = null)
			: base(connFactory, type, command, @params)
        {
        }

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveDbCommandBase(
			IDbConnectionFactory<TConnection> connFactory,
			CommandType type,
			string command,
			params Param[] @params)
			: this(connFactory, type, command, @params.ToList())
        {
        }

        /// <summary>
        /// Asynchronously executes a reader on a command with a handler function.
        /// </summary>
        /// <param name="handler">The handler function for each IDataRecord.</param>
        public async Task ExecuteAsync(Func<TCommand, Task> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = (TCommand)con.CreateCommand(
                Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                await con.OpenAsync();
                await handler(c);
            }
        }

        /// <summary>
        /// Asynchronously executes a reader on a command with a transform function.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function for each IDataRecord.</param>
        /// <returns>The result of the transform.</returns>
        public async Task<T> ExecuteAsync<T>(Func<TCommand, Task<T>> transform)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(
                Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                await con.OpenAsync();
                return await transform(c);
            }
        }

        /// <summary>
        /// Asynchronously executes a reader on a command with a handler function.
        /// </summary>
        /// <param name="handler">The handler function for the data reader.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        public Task ExecuteReaderAsync(Func<DbDataReader, Task> handler, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await handler(await command.ExecuteReaderAsync(behavior)));

        /// <summary>
        /// Asynchronously executes a reader on a command with a transform function.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function for each IDataRecord.</param>
        /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
        /// <returns>The result of the transform.</returns>
        public Task<T> ExecuteReaderAsync<T>(Func<DbDataReader, Task<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteAsync(async command => await transform(await command.ExecuteReaderAsync(behavior)));

        /// <summary>
        /// Calls ExecuteNonQueryAsync on the underlying command.
        /// </summary>
        /// <returns>The integer responise from the method.</returns>
        public Task<int> ExecuteNonQueryAsync()
            => ExecuteAsync(command => command.ExecuteNonQueryAsync());

        /// <summary>
        /// Calls ExecuteScalarAsync on the underlying command.
        /// </summary>
        /// <returns>The value returned from the method.</returns>
        public Task<object> ExecuteScalarAsync()
            => ExecuteAsync(command => command.ExecuteScalarAsync());

        /// <summary>
        /// Asynchronously executes scalar on the underlying command.
        /// </summary>
        /// <typeparam name="T">The type expected.</typeparam>
        /// <returns>The value returned from the method.</returns>
        public async Task<T> ExecuteScalarAsync<T>(Func<object, T> transform)
            => transform(await ExecuteScalarAsync());

        /// <summary>
        /// Asynchronously executes scalar on the underlying command and casts to the expected type.
        /// </summary>
        /// <typeparam name="T">The type expected.</typeparam>
        /// <returns>The value returned from the method.</returns>
        public async Task<T> ExecuteScalarAsync<T>()
            => (T)(await ExecuteScalarAsync());

        /// <summary>
        /// Asynchronously executes scalar on the underlying command.
        /// </summary>
        /// <typeparam name="T">The type expected.</typeparam>
        /// <returns>The value returned from the method.</returns>
        public async Task<T> ExecuteScalarAsync<T>(Func<object, Task<T>> transform)
            => await transform(await ExecuteScalarAsync());

        /// <summary>
        /// Iterates asynchronously and will stop iterating if canceled.
        /// </summary>
        /// <param name="handler">The active IDataRecord is passed to this handler.</param>
        /// <param name="token">An optional cancellation token.</param>
        /// <returns></returns>
        public Task IterateReaderAsync(Action<IDataRecord> handler, CancellationToken? token = null)
            => ExecuteAsync(command => command.IterateReaderAsync(handler, token));

        /// <summary>
        /// Iterates asynchronously until the handler returns false.  Then cancels.
        /// </summary>
        /// <param name="predicate">If true, the iteration continues.</param>
        /// <returns>The task that completes when the iteration is done or the predicate evaluates false.</returns>
        public Task IterateReaderAsyncWhile(Func<IDataRecord, bool> predicate)
            => ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));

        /// <summary>
        /// Iterates asynchronously until the handler returns false.  Then cancels.
        /// </summary>
        /// <param name="predicate">If true, the iteration continues.</param>
        /// <returns>The task that completes when the iteration is done or the predicate evaluates false.</returns>
        public Task IterateReaderAsyncWhile(Func<IDataRecord, Task<bool>> predicate)
            => ExecuteAsync(command => command.IterateReaderAsyncWhile(predicate));



        /// <summary>
        /// Asynchronously iterates a IDataReader and returns the each result until the count is met.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <param name="count">The maximum number of records before complete.</param>
        /// <returns>The value from the transform.</returns>
        public Task<List<T>> TakeAsync<T>(Func<IDataRecord, T> transform, int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
            List<T> results = new List<T>();
            if (count == 0) return Task.FromResult(results);

            return IterateReaderAsyncWhile(record =>
            {
                results.Add(transform(record));
                return results.Count < count;
            })
            .ContinueWith(t => results);
        }


		/// <summary>
		/// Reads the first column from every record and returns the results as a list.
		/// </summary>
		/// <returns>The list of transformed records.</returns>
		public Task<List<object>> FirstOrdinalResultsAsync()
			=> ToListAsync(r => r.GetValue(0));


		/// <summary>
		/// Reads the first column from every record.
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public Task<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>()
			=> ExecuteAsync(command => command.FirstOrdinalResultsAsync<T0>());

		/// <summary>
		/// Asynchronously iterates all records within the current result set using an IDataReader and returns the desired results.
		/// </summary>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
		public Task<QueryResult<Queue<object[]>>> RetrieveAsync(int n, params int[] others)
            => RetrieveAsync(new int[1] { n }.Concat(others));

        /// <summary>
        /// Iterates all records within the current result set using an IDataReader and returns the desired results.
        /// </summary>
        /// <param name="c">The first column name to include in the request to the reader for each record.</param>
        /// <param name="others">The remaining column names to request from the reader for each record.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public Task<QueryResult<Queue<object[]>>> RetrieveAsync(string c, params string[] others)
            => RetrieveAsync(new string[1] { c }.Concat(others));

        /// <summary>
        /// Posts all transformed records to the provided target block.
        /// If .Complete is called on the target block, then the iteration stops.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <param name="target">The target block to receive the records.</param>
        /// <returns>A task that is complete once there are no more results.</returns>
        public Task ToTargetBlockAsync<T>(ITargetBlock<T> target, Func<IDataRecord, T> transform)
        {
            Task<bool> lastSend = null;
            return IterateReaderAsyncWhile(async r =>
            {
                if (lastSend != null && !await lastSend) return false;
                lastSend = target.SendAsync(transform(r));
                return true;
            });
        }

        /// <summary>
        /// Provides a BufferBlock as the source of records.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <returns>A buffer block that is recieving the results.</returns>
        public ISourceBlock<T> AsSourceBlockAsync<T>(Func<IDataRecord, T> transform)
        {
            var source = new BufferBlock<T>();
            ToTargetBlockAsync(source, transform)
                .ContinueWith(t => source.Complete())
                .ConfigureAwait(false);
            return source;
        }

        /// <summary>
        /// Returns a source block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public ISourceBlock<T> AsSourceBlockAsync<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
           where T : new()
            => AsSourceBlockAsync<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

        /// <summary>
        /// Returns a source block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public ISourceBlock<T> AsSourceBlockAsync<T>(params (string Field, string Column)[] fieldMappingOverrides)
        where T : new()
            => AsSourceBlockAsync<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>);


        /// <summary>
        /// Asynchronously returns all records via a transform function.
        /// </summary>
        /// <param name="transform">The desired column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public async Task<List<T>> ToListAsync<T>(Func<IDataRecord, T> transform)
        {
            var results = new List<T>();
            await IterateReaderAsync(record => results.Add(transform(record)));
            return results;
        }

        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public Task<IEnumerable<T>> ResultsAsync<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides) where T : new()
            => ResultsAsync<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public Task<IEnumerable<T>> ResultsAsync<T>(params (string Field, string Column)[] fieldMappingOverrides) where T : new()
            => ResultsAsync<T>(fieldMappingOverrides as IEnumerable<(string Field, string Column)>);

        /// <summary>
        /// Returns a source block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public ISourceBlock<T> AsSourceBlockAsync<T>(IEnumerable<(string Field, string Column)> fieldMappingOverrides)
            where T : new()
        {
            var x = new Transformer<T>(fieldMappingOverrides);
            var cn = x.ColumnNames;
            var block = x.ResultsBlock(out Action<string[]> initColumnNames);

            ExecuteReaderAsync(async reader =>
            {
                // Ignores fields that don't match.
                var columns = reader.GetMatchingOrdinals(cn, true);

                var ordinalValues = columns.Select(c => c.Ordinal).ToArray();
                initColumnNames(columns.Select(c => c.Name).ToArray());

                Task<bool> lastSend = null;
                while (await reader.ReadAsync())
                {
                    if (lastSend != null && !await lastSend) break;
                    lastSend = block.SendAsync(reader.GetValuesFromOrdinals(ordinalValues));
                }

                block.Complete();
            });

            return block;
        }

        /// <summary>
        /// Asynchronously iterates all records within the first result set using an IDataReader and returns the results.
        /// </summary>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public Task<QueryResult<Queue<object[]>>> RetrieveAsync()
            => ExecuteReaderAsync(reader => reader.RetrieveAsync());

        /// <summary>
        /// Asynchronously iterates all records within the current result set using an IDataReader and returns the desired results.
        /// </summary>
        /// <param name="ordinals">The ordinals to request from the reader for each record.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public Task<QueryResult<Queue<object[]>>> RetrieveAsync(IEnumerable<int> ordinals)
            => ExecuteReaderAsync(reader => reader.RetrieveAsync(ordinals));

        /// <summary>
        /// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
        /// </summary>
        /// <param name="columnNames">The column names to select.</param>
        /// <param name="normalizeColumnOrder">Orders the results arrays by ordinal.</param>
        /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
        public Task<QueryResult<Queue<object[]>>> RetrieveAsync(IEnumerable<string> columnNames, bool normalizeColumnOrder = false)
            => ExecuteReaderAsync(reader => reader.RetrieveAsync(columnNames, normalizeColumnOrder));


        /// <summary>
        /// Asynchronously returns all records and iteratively attempts to map the fields to type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the list of results.</returns>
        public Task<IEnumerable<T>> ResultsAsync<T>(IEnumerable<(string Field, string Column)> fieldMappingOverrides)
            where T : new()
            => ExecuteReaderAsync(reader => reader.ResultsAsync<T>(fieldMappingOverrides));
    }
}
