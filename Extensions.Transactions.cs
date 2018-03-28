using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
    public static partial class Extensions
    {

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static bool ExecuteTransaction<TConn>(
            this TConn target, Func<TConn, bool> action)
            where TConn : IDbConnection
        {
            bool result = false;
            IDbTransaction transaction = null;

            if (target.State != ConnectionState.Open)
            {
                if (target.State != ConnectionState.Closed)
                    target.Close();
                target.Open();
            }

            try
            {
                transaction = target.BeginTransaction();
                result = action(target);
            }
            finally
            {
                if (transaction != null)
                {
                    if (result) transaction.Commit();
                    else transaction.Rollback();
                }
            }
            return result;
        }

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static bool ExecuteTransaction<TConn>(
            this TConn target, Func<bool> action)
            where TConn : IDbConnection
                => target.ExecuteTransaction(c => action());

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if no exceptions are thrown.
        /// Rolls-back the transaction if the handler throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        public static void ExecuteTransaction<TConn>(
            this TConn target, Action<TConn> action)
            where TConn : IDbConnection
                => target.ExecuteTransaction(c => { action(c); return true; });

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if no exceptions are thrown.
        /// Rolls-back the transaction if the handler throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        public static void ExecuteTransaction<TConn>(
            this TConn target, Action action)
            where TConn : IDbConnection
                => target.ExecuteTransaction(c => { action(); return true; });

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection factory to generate a connection from.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static void ExecuteTransaction<TConn>(
            this IDbConnectionFactory<TConn> target, Func<TConn, bool> action)
            where TConn : IDbConnection
        {
            using (var conn = target.Create())
            {
                conn.ExecuteTransaction(action);
            }
        }

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection factory to generate a connection from.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static void ExecuteTransaction<TConn>(
            this IDbConnectionFactory<TConn> target, Action<TConn> action)
            where TConn : IDbConnection
            => target.ExecuteTransaction(c => { action(c); return true; });



        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static async Task<bool> ExecuteTransactionAsync<TConn>(
            this TConn target, Func<TConn, Task<bool>> action)
            where TConn : DbConnection
        {
            bool result = false;
            IDbTransaction transaction = null;

            if (target.State != ConnectionState.Open)
            {
                if (target.State != ConnectionState.Closed)
                    target.Close();
                await target.OpenAsync();
            }

            try
            {
                transaction = target.BeginTransaction();
                result = await action(target);
            }
            finally
            {
                if (transaction != null)
                {
                    if (result) transaction.Commit();
                    else transaction.Rollback();
                }
            }
            return result;
        }

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static Task<bool> ExecuteTransactionAsync<TConn>(
            this TConn target, Func<Task<bool>> action)
            where TConn : DbConnection
                => target.ExecuteTransactionAsync(async c => await action());

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if no exceptions are thrown.
        /// Rolls-back the transaction if the handler throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        public static Task ExecuteTransactionAsync<TConn>(
            this TConn target, Func<TConn, Task> action)
            where TConn : DbConnection
                => target.ExecuteTransactionAsync(async c => { await action(c); return true; });

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if no exceptions are thrown.
        /// Rolls-back the transaction if the handler throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection to transact with.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        public static Task ExecuteTransactionAsync<TConn>(
            this TConn target, Func<Task> action)
            where TConn : DbConnection
                => target.ExecuteTransactionAsync(async c => { await action(); return true; });

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection factory to generate a connection from.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static async Task<bool> ExecuteTransactionAsync<TConn>(
            this IDbConnectionFactory<TConn> target, Func<TConn, Task<bool>> action)
            where TConn : DbConnection
        {
            using (var conn = target.Create())
            {
               return await conn.ExecuteTransactionAsync(action);
            }
        }

        /// <summary>
        /// Begins a transaction before executing the handler and then commits if the handler returns true.
        /// Rolls-back the transaction if the handler returns false or throws.
        /// </summary>
        /// <typeparam name="TConn">The connection type.</typeparam>
        /// <param name="target">The connection factory to generate a connection from.</param>
        /// <param name="action">The handler to execute while a transaction is pending.</param>
        /// <returns>True if the handler returns true.</returns>
        public static Task ExecuteTransactionAsync<TConn>(
            this IDbConnectionFactory<TConn> target, Func<TConn, Task> action)
            where TConn : DbConnection
            => target.ExecuteTransactionAsync(async c => { await action(c); return true; });

    }
}
