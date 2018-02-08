using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Base class for asynchronous enabled commands.
	/// </summary>
	public abstract class ExpressiveAsyncCommandBase<TConnection, TCommand, TDbType, TThis>
		: ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
		where TConnection : class, IDbConnection
		where TCommand : class, IDbCommand
		where TDbType : struct
		where TThis : ExpressiveAsyncCommandBase<TConnection, TCommand, TDbType, TThis>
	{

		/// <param name="connFactory">The factory to generate connections from.</param>
		/// <param name="type">The command type>.</param>
		/// <param name="command">The SQL command.</param>
		/// <param name="params">The list of params</param>
		protected ExpressiveAsyncCommandBase(
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
		protected ExpressiveAsyncCommandBase(
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
		public abstract Task ExecuteAsync(Func<TCommand, Task> handler);

		/// <summary>
		/// Asynchronously executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		public abstract Task<T> ExecuteAsync<T>(Func<TCommand, Task<T>> transform);

		/// <summary>
		/// Asynchronously executes a non-query on the underlying command.
		/// </summary>
		/// <returns>The integer responise from the method.</returns>
		public abstract Task<int> ExecuteNonQueryAsync();

		/// <summary>
		/// Asynchronously executes scalar on the underlying command.
		/// </summary>
		/// <returns>The varlue returned from the method.</returns>
		public abstract Task<object> ExecuteScalarAsync();

		/// <summary>
		/// Asynchronously executes scalar on the underlying command.
		/// </summary>
		/// <typeparam name="T">The type expected.</typeparam>
		/// <returns>The varlue returned from the method.</returns>
		public async Task<T> ExecuteScalarAsync<T>()
		{
			return (T)(await ExecuteScalarAsync());
		}

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
		/// Iterates asynchronously and will stop iterating if canceled.
		/// </summary>
		/// <param name="handler">The active IDataRecord is passed to this handler.</param>
		/// <param name="token">An optional cancellation token.</param>
		/// <returns></returns>
		public abstract Task IterateReaderAsync(Action<IDataRecord> handler, CancellationToken? token = null);

		/// <summary>
		/// Iterates asynchronously until the handler returns false.  Then cancels.
		/// </summary>
		/// <param name="predicate"></param>
		/// <returns></returns>
		public abstract Task IterateReaderAsyncWhile(Func<IDataRecord, bool> predicate);

		/// <summary>
		/// Posts all transformed records to the provided target block.
		/// If .Complete is called on the target block, then the iteration stops.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="target">The target block to receive the records.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public async Task ToTargetBlockAsync<T>(Func<IDataRecord, T> transform, ITargetBlock<T> target)
			=> await IterateReaderAsyncWhile(r => target.Post(transform(r)));

		/// <summary>
		/// Posts requested columns to a target block.
		/// </summary>
		/// <param name="target">The target block to receive the records.</param>
		/// <param name="columnNames">The column names to return.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public async Task ToTargetBlockAsync(ITargetBlock<Dictionary<string, object>> target, ISet<string> columnNames)
		{
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
			await IterateReaderAsyncWhile(r =>
                target.Post(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, columnNames)
                    : r.ToDictionary(columnIndexes)));
		}

		/// <summary>
		/// Posts requested columns to a target block.
		/// </summary>
		/// <param name="target">The target block to receive the records.</param>
		/// <param name="columnNames">The column names to return.  If none are specified, all are returned.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public async Task ToTargetBlockAsync(ITargetBlock<Dictionary<string, object>> target, params string[] columnNames)
		{
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            await IterateReaderAsyncWhile(r =>
                target.Post(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, columnNames)
                    : r.ToDictionary(columnIndexes)));
        }

		/// <summary>
		/// Posts requested columns to a target block.
		/// </summary>
		/// <param name="target">The target block to receive the records.</param>
		/// <param name="columnNames">The column names to return.</param>
		/// <returns>A task that is complete once there are no more results.</returns>
		public async Task ToTargetBlockAsync(ITargetBlock<Dictionary<string, object>> target, IEnumerable<string> columnNames)
		{
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            await IterateReaderAsyncWhile(r =>
                target.Post(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, columnNames)
                    : r.ToDictionary(columnIndexes)));
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
			ToTargetBlockAsync(transform, source)
				.ContinueWith(t => source.Complete())
				.ConfigureAwait(false);
			return source;
		}

		/// <summary>
		/// Provides a BufferBlock as the source of records.
		/// </summary>
		/// <param name="columnNames">The column names to return.</param>
		/// <returns>A buffer block that is recieving the results.</returns>
		public ISourceBlock<Dictionary<string, object>> AsSourceBlockAsync<T>(ISet<string> columnNames)
		{
			var source = new BufferBlock<Dictionary<string, object>>();
			ToTargetBlockAsync(source, columnNames)
				.ContinueWith(t => source.Complete())
				.ConfigureAwait(false);
			return source;
		}

		/// <summary>
		/// Provides a BufferBlock as the source of records.
		/// </summary>
		/// <param name="columnNames">The column names to return.  If none are specified, all are returned.</param>
		/// <returns>A buffer block that is recieving the results.</returns>
		public ISourceBlock<Dictionary<string, object>> AsSourceBlockAsync<T>(params string[] columnNames)
		{
			var source = new BufferBlock<Dictionary<string, object>>();
			ToTargetBlockAsync(source, columnNames)
				.ContinueWith(t => source.Complete())
				.ConfigureAwait(false);
			return source;
		}

		/// <summary>
		/// Provides a BufferBlock as the source of records.
		/// </summary>
		/// <param name="columnNames">The column names to return.</param>
		/// <returns>A buffer block that is recieving the results.</returns>
		public ISourceBlock<Dictionary<string, object>> AsSourceBlockAsync<T>(IEnumerable<string> columnNames)
		{
			var source = new BufferBlock<Dictionary<string, object>>();
			ToTargetBlockAsync(source, columnNames)
				.ContinueWith(t => source.Complete())
				.ConfigureAwait(false);
			return source;
		}

		/// <summary>
		/// Asynchronously returns all records via a transform function.
		/// </summary>
		/// <param name="transform">The desired column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public abstract Task<List<T>> ToListAsync<T>(Func<IDataRecord, T> transform);

		/// <summary>
		/// Asynchronously returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public async Task<List<Dictionary<string, object>>> RetrieveAsync(ISet<string> columnNames)
		{
			var list = new List<Dictionary<string, object>>();
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            await IterateReaderAsync(r =>
                list.Add(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, columnNames)
                    : r.ToDictionary(columnIndexes)));
			return list;
		}

		/// <summary>
		/// Asynchronously returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public Task<List<Dictionary<string, object>>> RetrieveAsync(IEnumerable<string> columnNames)
			=> RetrieveAsync(new HashSet<string>(columnNames));

		/// <summary>
		/// Asynchronously returns all records in order as Dictionaries where the keys are the specified column names.
		/// </summary>
		/// <param name="columnNames">The desired column names.</param>
		/// <returns>A task containing the list of results.</returns>
		public async Task<List<Dictionary<string, object>>> RetrieveAsync(params string[] columnNames)
		{
			// Probably an unnecessary check, but need to be sure.
			if (columnNames.Length != 0)
				return await RetrieveAsync(new HashSet<string>(columnNames));

			var list = new List<Dictionary<string, object>>();
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            await IterateReaderAsync(r =>
                list.Add(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, columnNames)
                    : r.ToDictionary(columnIndexes)));
            return list;
		}

        /// <summary>
        /// Retrieves the results before closing the connection and asynchronously returning an enumerable that coerces the data to fit type T.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A task containing the enumerable to pull the transformed results from.</returns>
        public async Task<IEnumerable<T>> ResultsAsync<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var n = x.ColumnNames;
			var q = new Queue<Dictionary<string, object>>();
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            await IterateReaderAsync(r =>
                q.Enqueue(columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, n)
                    : r.ToDictionary(columnIndexes)));

			return x.Transform(q);
		}

        /// <summary>
        /// Provides a transform block as the source of records.
        /// </summary>
        /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
        /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>A transform block that is recieving the results.</returns>
        public ISourceBlock<T> ResultsBlockAsync<T>(IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides = null)
		   where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			var n = x.ColumnNames;
			var q = new TransformBlock<Dictionary<string, object>, T>(e => x.TransformAndClear(e));
            IReadOnlyList<KeyValuePair<int, string>> columnIndexes = null;
            ToTargetBlockAsync(r => columnIndexes == null
                    ? r.ToDictionaryOutIndexes(out columnIndexes, n)
                    : r.ToDictionary(columnIndexes), q)
				.ContinueWith(t => q.Complete())
				.ConfigureAwait(false);

			return q;
		}

	}
}
