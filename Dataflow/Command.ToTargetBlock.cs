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

		/// <summary>
		/// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
		/// If a connection is desired to remain open after completion, you must open the connection before calling this method.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="command">The DbCommand to generate a reader from.</param>
		/// <param name="target">The target block to receive the results.</param>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The behavior to use with the data reader.</param>
		/// <param name="cancellationToken"></param>
		public static ValueTask ToTargetBlockAsync<T>(this IDbCommand command,
			ITargetBlock<T> target,
			Func<IDataRecord, T> transform,
			CommandBehavior behavior = CommandBehavior.Default,
			CancellationToken cancellationToken = default)
			=> command.ExecuteReaderAsync(reader => reader.ToTargetBlockAsync(target, transform, cancellationToken), behavior, cancellationToken);

	}
}
