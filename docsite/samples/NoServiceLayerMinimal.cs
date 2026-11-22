using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Open.Database.Extensions;

namespace Samples;

// The examples of the "Minimal APIs" page: the same command factory as the
// controllers, mapped as endpoints. NoServiceLayerChecks runs them.
public static class PostEndpoints
{
	#region nsl_minimal_endpoints
	public static IEndpointRouteBuilder MapPostEndpoints(
		this IEndpointRouteBuilder endpoints)
	{
		RouteGroupBuilder posts = endpoints.MapGroup("minimal/posts");

		// The pooled buffer: registered, then awaited, then returned.
		posts.MapGet(
			"by-author/{author}",
			static async Task<IReadOnlyList<Post>> (
				int author,
				PostCommands commands,
				HttpContext http) =>
			{
				PooledBuffer<Post> buffer = commands
					.ByAuthor(author)
					.ToAsyncBuffer(http.RequestAborted);
				http.Response.RegisterForDispose(buffer);
				return await buffer.Items;
			});

		// A list: one expression, nothing to register.
		posts.MapGet(
			"by-author/{author}/list",
			static (int author, PostCommands commands, HttpContext http)
				=> commands
					.ByAuthor(author)
					.ToListAsync(http.RequestAborted));

		return endpoints;
	}
	#endregion

	// Compiled, not run: the samples host the endpoints on a test server.
	public static async Task RunAsync(string[] args)
	{
		#region nsl_minimal_program
		WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
		string connectionString = builder.Configuration
			.GetConnectionString("Posts")
			?? throw new InvalidOperationException("No Posts connection string.");
		builder.Services.AddPostCommands(connectionString);

		WebApplication app = builder.Build();
		app.MapPostEndpoints();

		await app.RunAsync();
		#endregion
	}
}
