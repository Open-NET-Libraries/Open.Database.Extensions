using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;
using static Samples.AsyncBuffers;

namespace Samples.AsyncBuffersArticle;

// The controller of "Returning it from a controller" in the "Async buffers
// and streams" article. NoServiceLayerChecks requests it.
#region buffers_aspnet_controller
[ApiController]
[Route("posts")]
public class PostsController(
	IDbConnectionFactory<SqliteConnection> connectionFactory)
	: ControllerBase
{
	[HttpGet]
	public Task<PooledArray<Post>> GetAsync()
	{
		// LatestPosts is the data-layer method shown above (call it as you
		// have named it): the read starts here.
		PooledBuffer<Post> buffer = LatestPosts(
			connectionFactory,
			HttpContext.RequestAborted);

		// The framework disposes it when the request is over.
		Response.RegisterForDispose(buffer);

		// The framework awaits the rows, then writes them.
		// A PooledArray<Post> is an IReadOnlyList<Post>:
		// System.Text.Json writes it as a JSON array.
		return buffer.Items;
	}
}
#endregion
