using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Open.ChannelExtensions;

namespace Open.Database.Extensions
{
	public static partial class ChannelExtensions
	{
		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ValueTask<long> ToChannel<T>(this IDataReader reader,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			bool complete = false,
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
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if(!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken);

			var state = command.Connection.EnsureOpen();
			var behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await reader.ToChannel(target, transform, true, cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this IExecuteReader command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken);
			return await command.ExecuteReaderAsync(
				reader => reader.ToChannel(target, transform, true, cancellationToken));
		}

#if NETSTANDARD2_1
		/// <summary>
		/// Asynchronously iterates an DbDataReader through the transform function and writes each record to the channel.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="complete">If true, will call .Complete() if all the results have successfully been written (or the source is emtpy).</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			bool complete = false,
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
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannel<T>(this DbCommand command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (!command.Connection.State.HasFlag(ConnectionState.Open))
				await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken);

			var state = command.Connection.EnsureOpen();
			var behavior = CommandBehavior.SingleResult;
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await reader.ToChannelAsync(target, transform, true, cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The command to generate a reader from.</param>
		/// <param name="target">The target channel writer to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static async ValueTask<long> ToChannelAsync<T>(this IExecuteReaderAsync command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			await target.WaitToWriteAndThrowIfClosedAsync(true, cancellationToken);
			return await command.ExecuteReaderAsync(reader =>
				command.UseAsyncRead && reader is DbDataReader r
				? r.ToChannelAsync(target, transform, true, cancellationToken)
				: reader.ToChannel(target, transform, true, cancellationToken));
		}
#endif


	}
}
