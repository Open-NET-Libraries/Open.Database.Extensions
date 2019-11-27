using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Dataflow
{
	public static partial class DataflowExtensions
	{

		internal static bool IsStillAlive<T>(this ITargetBlock<T> block)
		{
			var completion = block.Completion;
			return !completion.IsCompleted && !completion.IsCanceled && !completion.IsFaulted;
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
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
        /// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="reader">The SqlDataReader to read from.</param>
        /// <param name="target">The target block to receive the results.</param>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        /// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        public static async ValueTask ToTargetBlockAsync<T>(this DbDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			bool useReadAsync = true,
            CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				var lastSend = new ValueTask<bool>(true);
				while (
                    target.IsStillAlive()
                    && await reader.ReadAsync(cancellationToken).ConfigureAwait(false) // Premtively grab next while waiting for previous transform.
					&& await lastSend.ConfigureAwait(false))
				{
					var values = transform(reader);
					lastSend = target.Post(values)
						? new ValueTask<bool>(true)
						: new ValueTask<bool>(target.SendAsync(values, cancellationToken));
				}

				// Makes sure we hook up to the last one if the while loop is done to cover any edge cases.
				if (!lastSend.IsCompleted)
                    await lastSend.ConfigureAwait(false);
			}
			else
			{
				var ok = true;
				cancellationToken.ThrowIfCancellationRequested();
				while (ok
                    && target.IsStillAlive()
                    && reader.Read())
				{
					var values = transform(reader);
					ok = target.Post(values) || await target.SendAsync(values, cancellationToken).ConfigureAwait(false);
					cancellationToken.ThrowIfCancellationRequested();
				}
			}
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
        public static async ValueTask ToTargetBlockAsync<T>(this DbCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			bool useReadAsync = true,
            CancellationToken cancellationToken = default)
		{
			if (command is null) throw new ArgumentNullException(nameof(command));
			if (target is null) throw new ArgumentNullException(nameof(target));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (target.IsStillAlive())
			{
				var state = await command.Connection.EnsureOpenAsync(cancellationToken);
				if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
				using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
				if (target.IsStillAlive())
					await reader.ToTargetBlockAsync(target, transform, useReadAsync, cancellationToken);
			}
		}

		/// <summary>
		/// Iterates an IDataReader through the transform function and posts each record to the target block.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The IDataReader to iterate.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		public static void ToTargetBlock<T>(this IDbCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default)
			=> command.ExecuteReader(reader => reader.ToTargetBlock(target, transform), behavior);
	}
}
