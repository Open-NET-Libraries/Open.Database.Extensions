namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Reaches the blocking side of a command the library made.
/// A command from outside the library has none: the call says so and points to the async call.
/// </summary>
static class SyncSupport
{
	/// <summary>Gets the blocking side of a command that reads rows.</summary>
	/// <param name="command">The command.</param>
	/// <returns>The command's blocking side.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls.</exception>
	internal static IExecuteReaderSync AsSync(this IExecuteReader command)
		=> command as IExecuteReaderSync ?? throw Missing(command, nameof(command));

	/// <summary>Gets the blocking side of a command that reads rows through a typed reader.</summary>
	/// <typeparam name="TReader">The type of reader.</typeparam>
	/// <param name="command">The command.</param>
	/// <returns>The command's blocking side.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls.</exception>
	internal static IExecuteReaderSync<TReader> AsSync<TReader>(this IExecuteReader<TReader> command)
		where TReader : IDataReader
		=> command as IExecuteReaderSync<TReader> ?? throw Missing(command, nameof(command));

	/// <summary>Gets the blocking side of a command that runs work against a command.</summary>
	/// <param name="command">The command.</param>
	/// <returns>The command's blocking side.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls.</exception>
	internal static IExecuteCommandSync AsSync(this IExecuteCommand command)
		=> command as IExecuteCommandSync ?? throw Missing(command, nameof(command));

	/// <summary>Gets the blocking side of a command that runs work against a typed command.</summary>
	/// <typeparam name="TCommand">The type of command.</typeparam>
	/// <param name="command">The command.</param>
	/// <returns>The command's blocking side.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls.</exception>
	internal static IExecuteCommandSync<TCommand> AsSync<TCommand>(this IExecuteCommand<TCommand> command)
		where TCommand : IDbCommand
		=> command as IExecuteCommandSync<TCommand> ?? throw Missing(command, nameof(command));

	static Exception Missing(object? instance, string paramName)
		=> instance is null
			? new ArgumentNullException(paramName)
			: new NotSupportedException($"{instance.GetType()} does not support synchronous calls. Use the async call.");
}
