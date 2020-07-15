using Open.ChannelExtensions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Extensions for writing data to a channel.
	/// </summary>
	public static partial class ChannelDbExtensions
	{
		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel(this IDataReader reader,
			ChannelWriter<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return target.WriteAll(
				reader.AsEnumerable(),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel(this IDataReader reader,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return target.WriteAll(
				reader.AsEnumerable(arrayPool),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel<T>(this IDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return target.WriteAll(
				reader.Select(transform, cancellationToken),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel<T>(this IDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			CancellationToken cancellationToken = default)
			where T : new()
			=> Transformer<T>
				.Create()
				.PipeResultsTo(reader, target, complete, cancellationToken);

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel<T>(this IDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
			CancellationToken cancellationToken = default)
			where T : new()
			=> Transformer<T>
				.Create(fieldMappingOverrides)
				.PipeResultsTo(reader, target, complete, cancellationToken);

		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel(this IDbCommand command,
			ChannelWriter<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReader(reader =>
					ToChannel(reader, target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel(this IDbCommand command,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReader(reader =>
					ToChannel(reader, target, false, arrayPool, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}

		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				var state = command.Connection.EnsureOpen();
				var behavior = CommandBehavior.SingleResult;
				if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
				using var reader = command.ExecuteReader(behavior);
				return await reader.ToChannel(target, false, transform, cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
			ChannelWriter<T> target,
			bool complete,
			CancellationToken cancellationToken = default)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReader(reader =>
					ToChannel(reader, target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
			CancellationToken cancellationToken = default)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReader(reader =>
					ToChannel(reader, target, false, fieldMappingOverrides, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel(this IExecuteReader command,
			ChannelWriter<object[]> target,
			bool complete)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync( // Must be ExecuteReaderAsync to await the to channel completion.
					reader => reader.ToChannel(target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel(this IExecuteReader command,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync( // Must be ExecuteReaderAsync to await the to channel completion.
					reader => reader.ToChannel(target, false, arrayPool, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader and through the transform function and posts each record it to the target channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IExecuteReader command,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync( // Must be ExecuteReaderAsync to await the to channel completion.
					reader => reader.ToChannel(target, false, transform, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IExecuteReader command,
			ChannelWriter<T> target,
			bool complete)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync( // Must be ExecuteReaderAsync to await the to channel completion.
					reader => reader.ToChannel(target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Iterates an IDataReader mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IExecuteReader command,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync( // Must be ExecuteReaderAsync to await the to channel completion.
					reader => reader.ToChannel(target, false, fieldMappingOverrides, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

#if NETSTANDARD2_1
		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
			ChannelWriter<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return target.WriteAllAsync(
				reader.AsAsyncEnumerable(cancellationToken),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync(this DbDataReader reader,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			return target.WriteAllAsync(
				reader.AsAsyncEnumerable(arrayPool, cancellationToken),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return target.WriteAllAsync(
				reader.SelectAsync(transform, cancellationToken),
				complete,
				false,
				cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			CancellationToken cancellationToken = default)
			where T : new()
			=> Transformer<T>
				.Create()
				.PipeResultsToAsync(reader, target, complete, cancellationToken);

		/// <summary>
		/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
			CancellationToken cancellationToken = default)
			where T : new()
			=> Transformer<T>
				.Create(fieldMappingOverrides)
				.PipeResultsToAsync(reader, target, complete, cancellationToken);

		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync(this DbCommand command,
			ChannelWriter<object[]> target,
			bool complete,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReaderAsync(reader =>
					ToChannelAsync(reader, target, false, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync(this DbCommand command,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReaderAsync(reader =>
					ToChannelAsync(reader, target, false, arrayPool, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this DbCommand command,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReaderAsync(reader =>
					ToChannelAsync(reader, target, false, transform, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}


		/// <summary>
		/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this DbCommand command,
			ChannelWriter<T> target,
			bool complete,
			CancellationToken cancellationToken = default)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReaderAsync(reader =>
					ToChannelAsync(reader, target, false, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an mapping the results to classes of type <typeparamref name="T"/> and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target channel to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this DbCommand command,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
			CancellationToken cancellationToken = default)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);

			try
			{
				return await command.ExecuteReaderAsync(reader =>
					ToChannelAsync(reader, target, false, fieldMappingOverrides, cancellationToken), cancellationToken: cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
			ChannelWriter<object[]> target,
			bool complete)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync(reader =>
					command.UseAsyncRead && reader is DbDataReader r
					? r.ToChannelAsync(target, false, cancellationToken)
					: reader.ToChannel(target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader and writes each record as an array to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="arrayPool">The array pool to acquire buffers from.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync(this IExecuteReaderAsync command,
			ChannelWriter<object?[]> target,
			bool complete,
			ArrayPool<object?> arrayPool)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync(reader =>
					command.UseAsyncRead && reader is DbDataReader r
					? r.ToChannelAsync(target, false, arrayPool, cancellationToken)
					: reader.ToChannel(target, false, arrayPool, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
			ChannelWriter<T> target,
			bool complete,
			Func<IDataRecord, T> transform)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync(reader =>
					command.UseAsyncRead && reader is DbDataReader r
					? r.ToChannelAsync(target, false, transform, cancellationToken)
					: reader.ToChannel(target, false, transform, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}


		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
			ChannelWriter<T> target,
			bool complete)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync(reader =>
					command.UseAsyncRead && reader is DbDataReader r
					? r.ToChannelAsync(target, false, cancellationToken)
					: reader.ToChannel(target, false, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}

		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
			ChannelWriter<T> target,
			bool complete,
			IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
			where T : new()
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			Contract.EndContractBlock();

			var cancellationToken = command.CancellationToken;
			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken).ConfigureAwait(false);
			try
			{
				return await command.ExecuteReaderAsync(reader =>
					command.UseAsyncRead && reader is DbDataReader r
					? r.ToChannelAsync(target, false, fieldMappingOverrides, cancellationToken)
					: reader.ToChannel(target, false, fieldMappingOverrides, cancellationToken)).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (complete)
				{
					complete = false;
					target.Complete(ex);
				}
				throw;
			}
			finally
			{
				if (complete)
					target.Complete();
			}
		}
#endif


	}
}
