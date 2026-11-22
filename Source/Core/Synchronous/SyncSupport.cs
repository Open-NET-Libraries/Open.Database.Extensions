namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Reaches the sync interface of a type the library only knows by its async-capable interface, and names the interface to implement when it is missing.
/// </summary>
static class SyncSupport
{
	/// <summary>Gets the sync interface of a command that reads rows.</summary>
	/// <param name="command">The command.</param>
	/// <returns>The command as <see cref="IExecuteReaderSync"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not implement <see cref="IExecuteReaderSync"/>.</exception>
	internal static IExecuteReaderSync AsSync(this IExecuteReader command)
		=> command as IExecuteReaderSync ?? throw Missing(command, nameof(command), nameof(IExecuteReaderSync));

	/// <summary>Gets the sync interface of a command that runs work against a command.</summary>
	/// <param name="command">The command.</param>
	/// <returns>The command as <see cref="IExecuteCommandSync"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not implement <see cref="IExecuteCommandSync"/>.</exception>
	internal static IExecuteCommandSync AsSync(this IExecuteCommand command)
		=> command as IExecuteCommandSync ?? throw Missing(command, nameof(command), nameof(IExecuteCommandSync));

	static Exception Missing(object? instance, string paramName, string needed)
		=> instance is null
			? new ArgumentNullException(paramName)
			: new NotSupportedException($"{instance.GetType()} does not support synchronous calls. Make it implement Open.Database.Extensions.Synchronous.{needed}, or use the async call.");
}
