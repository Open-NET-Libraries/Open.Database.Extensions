namespace Open.Database.Extensions;

/// <summary>
/// What a write into a channel you own does to the channel when it ends.
/// </summary>
public enum ChannelEnd
{
	/// <summary>
	/// Writes only. The channel stays open, so several passes can share it.
	/// A failure or a cancel comes back through the returned task
	/// and the channel is left as it was.
	/// </summary>
	KeepOpen = 0,

	/// <summary>
	/// Completes the channel when the pass ends:
	/// without an error after the last row,
	/// or with the exception that ended the pass, a cancel included.
	/// </summary>
	Complete = 1,
}
