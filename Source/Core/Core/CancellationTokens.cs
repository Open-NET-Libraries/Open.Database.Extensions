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
}
