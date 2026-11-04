namespace Open.Database.Extensions;

/// <summary>
/// Specifies what an operation does with a connection's open/closed state once it has finished.
/// </summary>
/// <remarks>
/// Operations that are given a connection open it first if it is not already open, so callers never need to
/// open it ahead of time. This setting only controls what happens afterwards.
/// Callers that want a connection to stay open across several operations can simply open it themselves first;
/// a connection that was already open is never closed by the library.
/// </remarks>
public enum ConnectionBehavior
{
	/// <summary>
	/// (Default) Returns the connection to the state it was in before the operation:
	/// if it was not open, it is closed (not disposed) when the operation completes, even if the operation fails;
	/// if it was already open, it is left open.
	/// </summary>
	RestoreState = 0,

	/// <summary>
	/// Leaves the connection open if the operation had to open it.
	/// This was the behavior of the transaction extensions before <see cref="RestoreState"/> became the default.
	/// </summary>
	LeaveOpen = 1,
}
