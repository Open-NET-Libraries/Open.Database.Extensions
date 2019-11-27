using System;
using System.Data;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Common interface used for expressive commands.
	/// </summary>
	public interface IExecuteCommand
	{
		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="action">The handler function for each IDataRecord.</param>
		void Execute(Action<IDbCommand> action);

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		T Execute<T>(Func<IDbCommand, T> transform);

		/// <summary>
		/// Asynchronously executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		ValueTask ExecuteAsync(Func<IDbCommand, ValueTask> handler);

		/// <summary>
		/// Asynchronously executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		ValueTask<T> ExecuteAsync<T>(Func<IDbCommand, ValueTask<T>> transform);
	}

	/// <summary>
	/// Common interface used for expressive commands.
	/// </summary>
	public interface IExecuteCommand<out TCommand> : IExecuteCommand
		where TCommand : IDbCommand
	{
		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="action">The handler function for each IDataRecord.</param>
		void Execute(Action<TCommand> action);

		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		T Execute<T>(Func<TCommand, T> transform);

		/// <summary>
		/// Asynchronously executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		ValueTask ExecuteAsync(Func<TCommand, ValueTask> handler);

		/// <summary>
		/// Asynchronously executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <returns>The result of the transform.</returns>
		ValueTask<T> ExecuteAsync<T>(Func<TCommand, ValueTask<T>> transform);
	}
}
