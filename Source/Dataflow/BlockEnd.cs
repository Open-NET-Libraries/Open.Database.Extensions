namespace Open.Database.Extensions;

/// <summary>
/// What a write into a block you own does to the block when it ends.
/// </summary>
public enum BlockEnd
{
	/// <summary>
	/// Writes only. The block stays open, so several passes can share it.
	/// A failure or a cancel comes back through the returned task
	/// and the block is left as it was.
	/// </summary>
	KeepOpen = 0,

	/// <summary>
	/// Completes the block when the pass ends after the last row,
	/// or faults it with the exception that ended the pass, a cancel included.
	/// A faulted block drops the rows it holds that were not received yet.
	/// </summary>
	Complete = 1,
}
