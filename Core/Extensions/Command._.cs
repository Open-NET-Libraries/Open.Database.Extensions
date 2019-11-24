using System;
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
	/// Core non-DB-specific extensions for retrieving data from a command using best practices.
	/// </summary>
	public static partial class CommandExtensions
	{
		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Select(transform).ToList();
		}

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>A list of all results.</returns>
		public static List<T> ToList<T>(this IDbCommand command,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> ToList(command, behavior, transform);

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, ValueTask<T>> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			return await reader.ToListAsync(transform, cancellationToken).ConfigureAwait(false);
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			Func<IDataRecord, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken cancellationToken = default)
			=> ToListAsync(command, behavior, transform, cancellationToken);

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, T> transform,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;

			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			if (useReadAsync) return await reader.ToListAsync(transform, cancellationToken).ConfigureAwait(false);

			var r = reader.ToList(transform, cancellationToken);
			return r;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
			=> ToListAsync(command, behavior, transform, true, cancellationToken);

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
			=> ToListAsync(command, behavior, transform, useReadAsync, cancellationToken);

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ToArray(command, behavior, transform);

		/// <summary>
		/// Iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>A list of all results.</returns>
		public static T[] ToArray<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Select(transform).ToArray();
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTable.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.ToDataTable();
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTables.
		/// Calls .NextResult() to check for more results.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The resultant list of DataTables.</returns>
		public static List<DataTable> ToDataTables(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.ToDataTables();
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			handler(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return transform(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ExecuteReaderAsync(this DbCommand command,
			Action<DbDataReader> handler,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			handler(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static async ValueTask ExecuteReaderAsync(this IDbCommand command,
			Func<IDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (command is DbCommand c)
			{
				await c.ExecuteReaderAsync(reader => handler(reader), behavior);
				return;
			}

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			await handler(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ExecuteReaderAsync(this DbCommand command,
			Func<DbDataReader, ValueTask> handler,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			await handler(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
			Func<DbDataReader, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			return transform(reader);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this IDbCommand command,
			Func<IDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (command is DbCommand c)
				return await ExecuteReaderAsync(c, transform, behavior);

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await transform(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The result of the transform.</returns>
		public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
			Func<DbDataReader, ValueTask<T>> transform,
			CommandBehavior behavior = CommandBehavior.Default, CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			return await transform(reader).ConfigureAwait(false);
		}

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector,
			CommandBehavior behavior = CommandBehavior.Default)
			=> IterateReader(command, behavior, transform, selector);

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
		/// <typeparam name="TResult">The type returned by the selector.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
		/// <returns>The result of the transform.</returns>
		public static TResult IterateReader<TEntity, TResult>(
			this IDbCommand command,
			CommandBehavior behavior,
			Func<IDataRecord, TEntity> transform,
			Func<IEnumerable<TEntity>, TResult> selector)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			if (selector == null) throw new ArgumentNullException(nameof(selector));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return selector(reader.Select(transform));
		}

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void IterateReader(this IDbCommand command, 
			CommandBehavior behavior,
			Action<IDataRecord> handler)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			reader.ForEach(handler);
		}

		/// <summary>
		/// Iterates a reader on a command with a handler function.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void IterateReader(this IDbCommand command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
			=> IterateReader(command, behavior, handler);

		internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			if (command == null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (reader.Read())
				yield return transform(reader);
		}

		internal static IEnumerable<object[]> IterateReaderInternal(IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			if (command == null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			if (reader.Read())
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = new object[fieldCount];
					reader.GetValues(row);
					yield return row;
				} while (reader.Read());
			}
		}

		/// <summary>
		/// Iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void IterateReaderWhile(this IDbCommand command,
			Func<IDataRecord, bool> predicate,
			CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			reader.IterateWhile(predicate);
		}


		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ForEachAsync(this DbCommand command,
			Action<IDataRecord> handler,
			CommandBehavior behavior,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			await reader.ForEachAsync(handler, useReadAsync, cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static ValueTask ForEachAsync(this DbCommand command,
			Action<IDataRecord> handler,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
			=> ForEachAsync(command, handler, CommandBehavior.Default, useReadAsync, cancellationToken);

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ForEachAsync(this DbCommand command,
			Func<IDataRecord, ValueTask> handler, CommandBehavior behavior,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			cancellationToken.ThrowIfCancellationRequested();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			await reader.ForEachAsync(handler, useReadAsync, cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates all records from an IDataReader.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static ValueTask ForEachAsync(this DbCommand command,
			Func<IDataRecord, ValueTask> handler,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
			=> ForEachAsync(command, handler, CommandBehavior.Default, useReadAsync, cancellationToken);

		/// <summary>
		/// Asynchronously iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask IterateReaderWhileAsync(this DbCommand command, Func<IDataRecord, ValueTask<bool>> predicate, CommandBehavior behavior = CommandBehavior.Default, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(true);
			await reader.IterateWhileAsync(predicate, useReadAsync, cancellationToken);
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Throws if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T First<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
			return reader.Select(transform).First();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T FirstOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
			return reader.Select(transform).FirstOrDefault();
		}


		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Throws if none or more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720:Identifier contains type name", Justification = "Conforming to LINQ standards.")]
		public static T Single<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Select(transform).Single();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.  Throws if more than one entry.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The value from the transform.</returns>
		public static T SingleOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Select(transform).SingleOrDefault();
		}

		/// <summary>
		/// Iterates an IDataReader and returns the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the transform limited by the take count.</returns>
		public static List<T> Take<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return reader.Select(transform).Take(count).ToList();
		}

		/// <summary>
		/// Iterates an IDataReader and skips the first number of results defined by the count.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="count">The number of records to skip.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the transform after the skip count.</returns>
		public static List<T> Skip<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (0 < count--) reader.Read();
			return reader.Select(transform).ToList();
		}

		/// <summary>
		/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="skip">The number of entries to skip before starting to take results.</param>
		/// <param name="take">The maximum number of records to return.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <returns>The results from the skip, transform and take operation.</returns>
		public static List<T> SkipThenTake<T>(this IDbCommand command, int skip, int take, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (0 < skip--) reader.Read();
			return reader.Select(transform).Take(take).ToList();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are then converted to null.
		/// </summary>
		/// <returns>The enumerable first ordinal values.</returns>
		public static IEnumerable<object?> FirstOrdinalResults(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			var results = new Queue<object>(IterateReaderInternal(command, behavior | CommandBehavior.SingleResult, r => r.GetValue(0)));
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
			=> command is DbCommand dbc
			? dbc.FirstOrdinalResults<T0>()
			: command.FirstOrdinalResults(behavior | CommandBehavior.SingleResult).Cast<T0>();

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			Contract.EndContractBlock();

			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleResult);
			return reader.FirstOrdinalResults<T0>();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The list of values.</returns>
		public static ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync(useReadAsync, cancellationToken), behavior | CommandBehavior.SingleResult, cancellationToken);

		/// <summary>
		/// Reads the first column from every record..
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <param name="command">The IDbCommand to generate a reader from.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The enumerable of casted values.</returns>
		public static ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, bool useReadAsync = true, CancellationToken cancellationToken = default)
			=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync<T0>(useReadAsync, cancellationToken), behavior | CommandBehavior.SingleResult, cancellationToken);

	}
}
