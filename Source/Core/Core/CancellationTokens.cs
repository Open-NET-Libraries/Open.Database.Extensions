namespace Open.Database.Extensions.Core;

internal static class CancellationTokens
{
	/// <summary>
	/// The one token that governs a call made with two: it is cancelled when either is.
	/// </summary>
	/// <param name="commandToken">The command's own token.</param>
	/// <param name="callToken">The token passed to the call.</param>
	/// <param name="linked">
	/// Set only when both tokens can be canceled and differ: the source that makes the returned token.
	/// The caller owns it and must dispose it when the call ends, however it ends. Otherwise <see langword="null"/>.
	/// </param>
	/// <returns>
	/// <see langword="default"/> when neither token can be canceled, the one that can when only one can (or when both are the same), and the linked source's token otherwise.
	/// Only the last case allocates.
	/// </returns>
	[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last")]
	public static CancellationToken Combine(CancellationToken commandToken, CancellationToken callToken, out CancellationTokenSource? linked)
	{
		linked = null;

		// A token that cannot be canceled is default, so the first two cases return default when neither can be canceled.
		if (!commandToken.CanBeCanceled) return callToken;
		if (!callToken.CanBeCanceled || commandToken == callToken) return commandToken;

		linked = CancellationTokenSource.CreateLinkedTokenSource(commandToken, callToken);
		return linked.Token;
	}

	/// <summary>
	/// Starts a call under the one token that governs it
	/// (see <see cref="Combine"/>) and releases the linked source,
	/// if any, when the call ends however it ends.
	/// </summary>
	/// <remarks>
	/// When no linked source is needed,
	/// <paramref name="start"/> is returned as it is: no state machine,
	/// no allocation, and a synchronous throw from it propagates synchronously.
	/// On the linked path a synchronous throw
	/// from <paramref name="start"/> becomes a faulted task.
	/// Pass a <see langword="static"/> lambda and a state,
	/// so a call with no token or one token allocates nothing.
	/// </remarks>
	/// <param name="commandToken">The command's own token.</param>
	/// <param name="callToken">The token passed to the call.</param>
	/// <param name="state">
	/// The state handed to <paramref name="start"/>.
	/// </param>
	/// <param name="start">Starts the call with the governing token.</param>
	[SuppressMessage(
		"Design", "CA1068:CancellationToken parameters must come last")]
	public static ValueTask<TResult> RunAsync<TState, TResult>(
		CancellationToken commandToken,
		CancellationToken callToken,
		TState state,
		Func<TState, CancellationToken, ValueTask<TResult>> start)
	{
		CancellationToken token = Combine(
			commandToken,
			callToken,
			out CancellationTokenSource? linked);
		return linked is null
			? start(state, token)
			: Linked(linked, token, state, start);

		static async ValueTask<TResult> Linked(
			CancellationTokenSource linked,
			CancellationToken token,
			TState state,
			Func<TState, CancellationToken, ValueTask<TResult>> start)
		{
			using (linked)
				return await start(state, token).ConfigureAwait(false);
		}
	}

	/// <inheritdoc cref="RunAsync{TState, TResult}(CancellationToken, CancellationToken, TState, Func{TState, CancellationToken, ValueTask{TResult}})"/>
	[SuppressMessage(
		"Design", "CA1068:CancellationToken parameters must come last")]
	public static ValueTask RunAsync<TState>(
		CancellationToken commandToken,
		CancellationToken callToken,
		TState state,
		Func<TState, CancellationToken, ValueTask> start)
	{
		CancellationToken token = Combine(
			commandToken,
			callToken,
			out CancellationTokenSource? linked);
		return linked is null
			? start(state, token)
			: Linked(linked, token, state, start);

		static async ValueTask Linked(
			CancellationTokenSource linked,
			CancellationToken token,
			TState state,
			Func<TState, CancellationToken, ValueTask> start)
		{
			using (linked)
				await start(state, token).ConfigureAwait(false);
		}
	}
}
