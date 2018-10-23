using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions
{
	public static partial class Extensions
	{
		internal static bool IsStillAlive<T>(this ITargetBlock<T> task)
		{
			return IsStillAlive(task.Completion);
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
			if (target == null) throw new ArgumentNullException(nameof(target));
			if (transform == null) throw new ArgumentNullException(nameof(transform));
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
        public static async Task ToTargetBlockAsync<T>(this DbDataReader reader,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			bool useReadAsync = true,
            CancellationToken cancellationToken = default)
		{
			if (target == null) throw new ArgumentNullException(nameof(target));
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				Task<bool> lastSend = null;
				while (
                    target.IsStillAlive() && !cancellationToken.IsCancellationRequested
                    && await reader.ReadAsync(cancellationToken).ConfigureAwait(false) // Premtively grab next while waiting for previous transform.
					&& (lastSend == null || await lastSend.ConfigureAwait(false)))
				{
					var values = transform(reader);
					lastSend = target.Post(values) ? null : target.SendAsync(values);
				}
                // Makes sure we hook up to the last one if the while loop is done to cover any edge cases.
                if (lastSend != null)
                    await lastSend.ConfigureAwait(false);
			}
			else
			{
				var ok = true;
				while (ok
                    && target.IsStillAlive() && !cancellationToken.IsCancellationRequested
                    && reader.Read())
				{
					var values = transform(reader);
					ok = target.Post(values) || await target.SendAsync(values);
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
        public static async Task ToTargetBlockAsync<T>(this DbCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			bool useReadAsync = true,
            CancellationToken cancellationToken = default)
		{
			if (target == null) throw new ArgumentNullException(nameof(target));
			if (transform == null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (target.IsStillAlive())
			{
				var state = await command.Connection.EnsureOpenAsync(cancellationToken);
				if (state == ConnectionState.Closed) behavior = behavior | CommandBehavior.CloseConnection;
				using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken))
				{
					if (target.IsStillAlive())
						await reader.ToTargetBlockAsync(target, transform, useReadAsync, cancellationToken);
				}
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
