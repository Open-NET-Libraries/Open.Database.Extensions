using System;
using System.Data;
using System.Diagnostics.Contracts;
using System.Threading.Channels;
using System.Threading.Tasks;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class ChannelExtensions
	{

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// Stops if the channel rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public static void ToChannel<T>(this IExecuteReader command, ChannelWriter<T> target, Func<IDataRecord, T> transform)
			=> IterateReaderWhile(r => target.TryWrite(transform(r)));

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// Stops if the channel rejects.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="target">The target block to receive the results (to be posted to).</param>
		public void ToChannel<T>(Channel<T> target, Func<IDataRecord, T> transform)
			=> ToChannel(target.Writer, transform);

		/// <summary>
		/// Posts all records to a channel using the transform function.
		/// </summary>
		/// <typeparam name="T">The expected type.</typeparam>
		/// <param name="transform">The transform function.</param>
		/// <param name="synchronousExecution">By default the command is deferred.
		/// If set to true, the command runs synchronously and all data is acquired before the method returns.
		/// If set to false (default) the data is received asynchronously (deferred: data will be subsequently posted) and the source block (transform) can be completed early.</param>
		/// <returns>A reader of an unbounded channel containing the results.</returns>
		public ChannelReader<T> AsChannel<T>(
			Func<IDataRecord, T> transform,
			bool synchronousExecution = false)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
			{
				SingleWriter = true,
				AllowSynchronousContinuations = true
			});

			void I()
			{
				ToChannel(channel, transform);
				channel.Writer.Complete();
			}

			if (synchronousExecution) I();
			else Task.Run(I);
			return channel.Reader;
		}

	}
}
