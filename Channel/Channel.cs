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
		/// <param name="deferredExecution">If true, calls await Task.Yield() before writing.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannel<T>(this IDataReader reader,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			bool complete = false,
			bool deferredExecution = false,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return target.WriteAll(
				reader.Select(transform, cancellationToken),
				complete,
				deferredExecution,
				cancellationToken);
		}

		public static ChannelReader<T> AsChannel<T>(this IDataReader reader,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
				{
					AllowSynchronousContinuations = true,
					SingleWriter = true
				});

			reader.ToChannel(channel.Writer, transform, true, true, cancellationToken);

			return channel.Reader;
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken"></param>
		public static async ValueTask<long> ToChannel<T>(this IDbCommand command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			bool deferredExecution = false,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			await target.WaitToWriteAndThrowIfClosedAsync(cancellationToken);
			var state = command.Connection.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await reader.ToChannel(target, transform, true, deferredExecution, cancellationToken);
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
		/// <param name="deferredExecution">If true, calls await Task.Yield() before writing.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		public static ValueTask<long> ToChannelAsync<T>(this DbDataReader reader,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			bool complete = false,
			bool deferredExecution = false,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return target.WriteAllAsync(
				reader.SelectAsync(transform, cancellationToken),
				complete,
				deferredExecution,
				cancellationToken);
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken"></param>
		public static async ValueTask<long> ToChannelAsync<T>(this DbCommand command,
			ChannelWriter<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			bool deferredExecution = false,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			await target.WaitToWriteAndThrowIfClosedAsync(cancellationToken);
			var state = await command.Connection.EnsureOpenAsync(cancellationToken);
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
			return await reader.ToChannelAsync(target, transform, true, deferredExecution, cancellationToken);
		}
#endif


	}
}
