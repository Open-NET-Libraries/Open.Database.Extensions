using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Extensions for pipelining data from a database to Dataflow blocks.
	/// </summary>
	public static partial class DataflowExtensions
	{
		internal static bool IsStillAlive<T>(this ITargetBlock<T> block)
		{
			var completion = block.Completion;
			return !completion.IsCompleted && !completion.IsCanceled && !completion.IsFaulted;
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete).
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receive the results.</param>
		public static void ToTargetBlock<T>(this IDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			while (target.IsStillAlive() && reader.Read() && target.Post(transform(reader))) { }
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// Will stop reading if the target rejects (is complete).
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ToTargetBlockAsync<T>(this IDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var ok = !cancellationToken.IsCancellationRequested;
			while (ok
				&& target.IsStillAlive()
				&& reader.Read())
			{
				var values = transform(reader);
				ok = await target.SendAsync(values, cancellationToken).ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// Will stop reading if the target rejects (is complete).
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static ValueTask ToTargetBlockAsync<T>(this DbDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			bool useReadAsync = true,
            CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			return useReadAsync
				? ToTargetBlockAsyncCore()
				: ToTargetBlockAsync((IDataReader)reader, target, transform, cancellationToken);

			async ValueTask ToTargetBlockAsyncCore()
			{
				var lastSend = Task.FromResult(true);
				while (
					target.IsStillAlive()
					&& await reader.ReadAsync(cancellationToken).ConfigureAwait(false) // Premtively grab next while waiting for previous transform.
					&& await lastSend.ConfigureAwait(false))
				{
					var values = transform(reader);
					lastSend = target.SendAsync(values, cancellationToken);
				}

				// Makes sure we hook up to the last one if the while loop is done to cover any edge cases.
				await lastSend.ConfigureAwait(false);
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		public static ValueTask ToTargetBlockAsync<T>(this DbDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken)
			=> ToTargetBlockAsync(reader, target, transform, true, cancellationToken);

	}
}
