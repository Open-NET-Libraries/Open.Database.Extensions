using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

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
        /// <param name="target">The target block to receivethe results.</param>
        public static void ToTargetBlock<T>(this IDataReader reader,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
        {
            while (target.IsStillAlive() && reader.Read() && target.Post(transform(reader))) { }
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="reader">The SqlDataReader to read from.</param>
        /// <param name="target">The target block to receive the results.</param>
        /// <param name="transform">The transform function to process each IDataRecord.</param>
        public static async Task ToTargetBlockAsync<T>(this DbDataReader reader,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
        {
            Task<bool> lastSend = null;
            while (target.IsStillAlive()
                && await reader.ReadAsync()
                && (lastSend == null || await lastSend))
            {
                // Allows for a premtive read before waiting for the next send.
                lastSend = target.SendAsync(transform(reader));
            }
        }

        /// <summary>
        /// Asynchronously iterates an IDataReader and through the transform function and posts each record it to the target block.
        /// </summary>
        /// <typeparam name="T">The return type of the transform function.</typeparam>
        /// <param name="command">The DbCommand to generate a reader from.</param>
        /// <param name="target">The target block to receive the results.</param>
        /// <param name="transform">The transform function for each IDataRecord.</param>
        public static async Task ToTargetBlockAsync<T>(this DbCommand command,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
        {
            if (target.IsStillAlive())
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (target.IsStillAlive())
                        await reader.ToTargetBlockAsync(target, transform);
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
        public static void ToTargetBlock<T>(this IDbCommand command,
            ITargetBlock<T> target,
            Func<IDataRecord, T> transform)
            => command.ExecuteReader(reader => reader.ToTargetBlock(target, transform));

    }
}
