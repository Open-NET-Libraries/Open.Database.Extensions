namespace Open.Database.Extensions.Core;

internal static class ValueTaskHelpers
{
	/// <summary>
	/// Completes a <see cref="ValueTask{TResult}"/> as a plain
	/// <see cref="ValueTask"/>, without allocating when it has already finished.
	/// </summary>
	/// <typeparam name="T">The type of the discarded result.</typeparam>
	/// <param name="task">The task whose result is discarded.</param>
	/// <returns>
	/// A task that represents the completion of <paramref name="task"/>.
	/// </returns>
	public static ValueTask Discard<T>(ValueTask<T> task)
	{
		if (task.IsCompletedSuccessfully)
		{
			_ = task.Result;
			return default;
		}

		return AwaitIt(task);

		static async ValueTask AwaitIt(ValueTask<T> pending) => await pending.ConfigureAwait(false);
	}
}
