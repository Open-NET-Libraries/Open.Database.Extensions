using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// The debug pattern of the mapping article: the null policy only in Debug builds.
#region null_debug_model
#if DEBUG
[NullPolicy(NullAction.Throw)]
#endif
public class PostStats
{
	public int Id { get; init; }
	public int Likes { get; init; }
}
#endregion

public static class NullDebug
{
	public static async Task<ValueMappingException?> RunAsync(
		SqliteConnection connection,
		CancellationToken cancellationToken)
	{
		#region null_debug_use
		try
		{
			_ = await connection
				.Command("select 1 as id, null as likes")
				.Map<PostStats>()
				.ToListAsync(cancellationToken);
			return null;
		}
		catch (ValueMappingException ex)
		{
			// Debug: A NULL was read from column 'likes' into 'Likes' of
			// PostStats at row 0, whose null policy is Throw.
			// Release: A NULL was read into a non-nullable member of
			// PostStats at row 0. Declare the member nullable.
			return ex;
		}
		#endregion
	}
}
