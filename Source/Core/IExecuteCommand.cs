namespace Open.Database.Extensions;

/// <summary>
/// Executes work against an <see cref="IDbCommand"/> that the implementation creates, runs, and cleans up for the caller.
/// </summary>
/// <remarks>
/// Each call takes a connection, opens it only if needed, creates the command (with its text, type, timeout, parameters, and transaction already applied), passes it to the supplied function, and then releases the connection. Anything provider related that the function throws propagates unchanged and the connection is still released. The command is disposed when the function returns, so do not keep it.
/// </remarks>
/// <seealso cref="IExecuteCommand{TCommand}"/>
/// <seealso cref="IExecuteReader"/>
/// <seealso cref="ExpressiveCommand"/>
public interface IExecuteCommand
{
	/// <summary>
	/// Creates a command and passes it to <paramref name="action"/>.
	/// </summary>
	/// <param name="action">The action that operates on the command.</param>
	void Execute(Action<IDbCommand> action);

	/// <summary>
	/// Creates a command and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="transform">The function that operates on the command.</param>
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	T Execute<T>(Func<IDbCommand, T> transform);

	/// <summary>
	/// Asynchronously creates a command and passes it to <paramref name="handler"/>.
	/// </summary>
	/// <param name="handler">The asynchronous function that operates on the command.</param>
	/// <returns>A task that completes after <paramref name="handler"/> has completed and the connection has been released.</returns>
	/// <remarks>
	/// Implementations that expose a cancellation token throw <see cref="OperationCanceledException"/> if it is already canceled when this method is called.
	/// </remarks>
	ValueTask ExecuteAsync(Func<IDbCommand, ValueTask> handler);

	/// <summary>
	/// Asynchronously creates a command and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="transform">The asynchronous function that operates on the command.</param>
	/// <returns>A task that produces the value returned by <paramref name="transform"/> after the connection has been released.</returns>
	/// <remarks>
	/// Implementations that expose a cancellation token throw <see cref="OperationCanceledException"/> if it is already canceled when this method is called.
	/// </remarks>
	ValueTask<T> ExecuteAsync<T>(Func<IDbCommand, ValueTask<T>> transform);
}

/// <summary>
/// A strongly typed <see cref="IExecuteCommand"/> whose commands are of type <typeparamref name="TCommand"/>.
/// </summary>
/// <typeparam name="TCommand">The type of command passed to the functions.</typeparam>
/// <seealso cref="IExecuteCommand"/>
public interface IExecuteCommand<out TCommand> : IExecuteCommand
	where TCommand : IDbCommand
{
	/// <summary>
	/// Creates a command of type <typeparamref name="TCommand"/> and passes it to <paramref name="action"/>.
	/// </summary>
	/// <inheritdoc cref="IExecuteCommand.Execute(Action{IDbCommand})"/>
	void Execute(Action<TCommand> action);

	/// <summary>
	/// Creates a command of type <typeparamref name="TCommand"/> and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <inheritdoc cref="IExecuteCommand.Execute{T}(Func{IDbCommand, T})"/>
	T Execute<T>(Func<TCommand, T> transform);

	/// <summary>
	/// Asynchronously creates a command of type <typeparamref name="TCommand"/> and passes it to <paramref name="handler"/>.
	/// </summary>
	/// <inheritdoc cref="IExecuteCommand.ExecuteAsync(Func{IDbCommand, ValueTask})"/>
	ValueTask ExecuteAsync(Func<TCommand, ValueTask> handler);

	/// <summary>
	/// Asynchronously creates a command of type <typeparamref name="TCommand"/> and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <inheritdoc cref="IExecuteCommand.ExecuteAsync{T}(Func{IDbCommand, ValueTask{T}})"/>
	ValueTask<T> ExecuteAsync<T>(Func<TCommand, ValueTask<T>> transform);
}
