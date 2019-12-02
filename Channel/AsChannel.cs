using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Channels;

namespace Open.Database.Extensions
{
	public static partial class ChannelExtensions
	{
		internal static Channel<T> CreateChannel<T>(int capacity = -1, bool singleReader = false, bool singleWriter = true)
			=> capacity > 0
			? Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
			{
				SingleWriter = singleWriter,
				SingleReader = singleReader,
				AllowSynchronousContinuations = true,
				FullMode = BoundedChannelFullMode.Wait
			})
			: Channel.CreateUnbounded<T>(new UnboundedChannelOptions
			{
				SingleWriter = singleWriter,
				SingleReader = singleReader,
				AllowSynchronousContinuations = true
			});

		// NOTE:
		// Some of these methods are kept internal to avoid the risks involved with leaving a conneciton open while waiting to write to a channel.

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to a channel.
		/// Be sure to await the completion.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
		/// <param name="capacity">The optional bounded capacity of the channel. Default is unbound.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		internal static ChannelReader<T> AsChannel<T>(this IDataReader reader,
			Func<IDataRecord, T> transform, bool singleReader, int capacity,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			if (capacity == 0) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Cannot be zero.");
			if (capacity < -1) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Must greater than zero or equal to negative one (unbounded).");
			Contract.EndContractBlock();

			var channel = CreateChannel<T>(capacity, singleReader);
#if NETSTANDARD2_1
			if (reader is DbDataReader r)
				ToChannelAsync(r, channel.Writer, transform, true, cancellationToken);
			else
#endif
			ToChannel(reader, channel.Writer, transform, true, cancellationToken);
			return channel.Reader;
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to an unbound channel.
		/// Be sure to await the completion.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ChannelReader<T> AsChannel<T>(this IDataReader reader,
			Func<IDataRecord, T> transform, bool singleReader,
			CancellationToken cancellationToken = default)
			=> AsChannel(reader, transform, singleReader, -1, cancellationToken);

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to a channel.
		/// Be sure to await the completion.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
		/// <param name="capacity">The optional bounded capacity of the channel. Default is unbound.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		internal static ChannelReader<T> AsChannel<T>(this IDbCommand command,
			Func<IDataRecord, T> transform, bool singleReader, int capacity,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			if (capacity == 0) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Cannot be zero.");
			if (capacity < -1) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Must greater than zero or equal to negative one (unbounded).");
			Contract.EndContractBlock();

			var channel = CreateChannel<T>(capacity, singleReader);
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#if NETSTANDARD2_1
			if (command is DbCommand c)
				ToChannelAsync(c, channel.Writer, transform, true, cancellationToken);
			else
#endif
			ToChannel(command, channel.Writer, transform, true, cancellationToken);
			return channel.Reader;
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to a channel.
		/// Be sure to await the completion.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ChannelReader<T> AsChannel<T>(this IDbCommand command,
			Func<IDataRecord, T> transform, bool singleReader,
			CancellationToken cancellationToken = default)
			=> AsChannel(command, transform, singleReader, -1, cancellationToken);

		/// <summary>
		/// Iterates an IDataReader through the transform function and writes each record to a channel.
		/// Be sure to await the completion.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="singleReader">True will cause the resultant reader to optimize for the assumption that no concurrent read operations will occur.</param>
		/// <param name="cancellationToken">An optional cancellation token.</param>
		/// <returns>The number of records processed.</returns>
		public static ChannelReader<T> AsChannel<T>(this IExecuteReader command,
			Func<IDataRecord, T> transform, bool singleReader,
			CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var channel = CreateChannel<T>(-1, singleReader);
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
#if NETSTANDARD2_1
			if (command is IExecuteReaderAsync c)
				ToChannelAsync(c, channel.Writer, transform, cancellationToken);
			else
#endif
			ToChannel(command, channel.Writer, transform, cancellationToken);
			return channel.Reader;
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
		}


	}
}
