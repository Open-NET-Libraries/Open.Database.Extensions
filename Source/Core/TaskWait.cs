namespace Open.Database.Extensions.Core;

internal static class TaskWait
{
	/// <summary>
	/// Waits for a task that never faults, or until the token fires
	/// (the cancel is thrown; the task is not affected).
	/// </summary>
	internal static async Task Async(
		Task task,
		CancellationToken cancellationToken)
	{
		if (task.IsCompleted || !cancellationToken.CanBeCanceled)
		{
			await task.ConfigureAwait(false);
			return;
		}

		TaskCompletionSource<bool> fired = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		using (cancellationToken.Register(
			static s => ((TaskCompletionSource<bool>)s!)
				.TrySetResult(true),
			fired))
		{
			Task first = await Task
				.WhenAny(task, fired.Task)
				.ConfigureAwait(false);

			if (first != task)
				cancellationToken.ThrowIfCancellationRequested();
		}
	}
}
