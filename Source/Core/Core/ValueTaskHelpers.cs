namespace Open.Database.Extensions.Core;

internal static class ValueTaskHelpers
{
	/// <summary>Completes a <see cref="ValueTask{TResult}"/> as a plain <see cref="ValueTask"/>, without allocating when it has already finished.</summary>
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
