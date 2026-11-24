using System.Buffers;
using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Open.Database.Extensions;

namespace Samples;

/// <summary>
/// The app of the "No service layer" article, hosted on a test server: the
/// samples project is not a web app, so this factory builds the host itself.
/// </summary>
public sealed class SamplesAppFactory : WebApplicationFactory<Program>
{
	protected override IHostBuilder CreateHostBuilder()
		=> new HostBuilder()
			.ConfigureWebHost(web => web
				.ConfigureServices(static services => services
					.AddPostCommands(Db.ConnectionString)
					.AddSingleton(new Classic.PostRepository(Db.ConnectionString))
					.AddSingleton<Classic.PostService>()
					.AddControllers())
				.Configure(static app => app
					.UseRouting()
					.UseEndpoints(static endpoints => endpoints
						.MapPostEndpoints()
						.MapControllers())));

	// The factory looks for a project folder by the name of the assembly: use
	// the output folder instead.
	protected override void ConfigureWebHost(IWebHostBuilder builder)
		=> builder.UseContentRoot(AppContext.BaseDirectory);
}

/// <summary>Checks what the "No service layer" article says, on a test server.</summary>
public static class NoServiceLayerChecks
{
	public static async Task RunAsync(Action<bool, string> check)
	{
		await using SamplesAppFactory app = new();

		// The article's own tests.
		await new PostCommandsTests(new DatabaseFixture()).ByAuthorReadsThatAuthorsPosts();
		PostsEndpointTests endpointTests = new(app);
		await endpointTests.ReturnsThePostsAsJson();
		await endpointTests.GivesEveryArrayBack();
		check(true, "no service layer: the article's three tests pass (command factory, endpoint, lifetime guard)");

		// Every shape writes the same JSON: classic, buffer, list, ETag.
		CountingPool<Post> pool = new();
		await using WebApplicationFactory<Program> counted = app.WithWebHostBuilder(
			web => web.ConfigureServices(services => services.AddSingleton<ArrayPool<Post>>(pool)));
		using HttpClient client = counted.CreateClient();

		string classic = await client.GetStringAsync("classic/posts/by-author/42");
		string buffered = await client.GetStringAsync("posts/by-author/42");
		string listed = await client.GetStringAsync("list/posts/by-author/42");
		check(classic.StartsWith("[{\"id\":1,", StringComparison.Ordinal) && classic.Contains("\"id\":3,", StringComparison.Ordinal), "no service layer: the classic stack writes the posts of author 42");
		check(buffered == classic && listed == classic, "no service layer: the pooled buffer, the list and the classic stack write the same JSON");
		string minimal = await client.GetStringAsync("minimal/posts/by-author/42");
		string minimalList = await client.GetStringAsync("minimal/posts/by-author/42/list");
		check(minimal == classic && minimalList == classic, "no service layer: the minimal API endpoints, buffer and list, write the same JSON");

		// The controller of "Returning it from a controller" (pooled buffers article).
		string latest = await client.GetStringAsync("posts");
		List<Post> latestList = await Db.Factory
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToListAsync(CancellationToken.None);
		check(latest == JsonSerializer.Serialize(latestList, JsonSerializerOptions.Web), "pooled buffers: the controller that returns Items without awaiting it writes the same JSON as ToListAsync");

		// ETag: a 200 with the tag, then a 304 for the same tag, with no body.
		using HttpResponseMessage first = await client.GetAsync("cached/posts/by-author/42");
		string? etag = first.Headers.ETag?.Tag;
		string firstBody = await first.Content.ReadAsStringAsync();
		check(first.StatusCode == HttpStatusCode.OK && etag is not null && first.Headers.ETag?.IsWeak == true && firstBody == classic, "no service layer: the ETag action answers 200 with a weak ETag and the same JSON");

		using HttpRequestMessage again = new(HttpMethod.Get, "cached/posts/by-author/42");
		again.Headers.TryAddWithoutValidation("If-None-Match", etag);
		using HttpResponseMessage second = await client.SendAsync(again);
		byte[] secondBody = await second.Content.ReadAsByteArrayAsync();
		check(second.StatusCode == HttpStatusCode.NotModified && secondBody.Length == 0 && second.Headers.ETag?.Tag == etag, "no service layer: the same tag in If-None-Match gives a 304 with the ETag and no body");

		using HttpRequestMessage weak = new(HttpMethod.Get, "cached/posts/by-author/42");
		weak.Headers.TryAddWithoutValidation("If-None-Match", "W/" + etag);
		using HttpResponseMessage third = await client.SendAsync(weak);
		check(third.StatusCode == HttpStatusCode.NotModified, "no service layer: If-None-Match compares weakly, so W/ and the tag match");

		using HttpRequestMessage any = new(HttpMethod.Get, "cached/posts/by-author/42");
		any.Headers.TryAddWithoutValidation("If-None-Match", "*");
		using HttpResponseMessage fourth = await client.SendAsync(any);
		check(fourth.StatusCode == HttpStatusCode.NotModified, "no service layer: If-None-Match: * matches any ETag");

		using HttpResponseMessage otherAuthor = await client.GetAsync("cached/posts/by-author/7");
		check(otherAuthor.Headers.ETag?.Tag is { } otherTag && otherTag != etag, "no service layer: other rows give another ETag");

		using CancellationTokenSource limit = new(TimeSpan.FromSeconds(5));
		while (pool.Returned < pool.Rented && !limit.IsCancellationRequested)
			await Task.Delay(10);
		check(pool.Rented >= 5 && pool.Returned == pool.Rented && pool.Dirty == 0, $"no service layer: every array the buffer, the list, the minimal endpoints and the ETag action rented came back cleared ({pool.Rented} rented)");

		// The guard is not vacuous: without RegisterForDispose nothing comes back.
		CountingPool<Post> leaked = new();
		await using WebApplicationFactory<Program> leaky = app.WithWebHostBuilder(
			web => web.ConfigureServices(services => services.AddSingleton<ArrayPool<Post>>(leaked)));
		using HttpClient leakyClient = leaky.CreateClient();
		string leakedBody = await leakyClient.GetStringAsync("forgot-to-register/posts/by-author/42");
		// A fixed wait, not a loop: nothing ever comes back (a buffer has no
		// finalizer); the wait only gives a stray dispose time to show.
		await Task.Delay(500);
		check(leakedBody == classic && leaked.Rented > 0 && leaked.Returned == 0, "no service layer: without RegisterForDispose the response is the same but no array comes back, so the lifetime guard fails");
	}
}

// Not in the article: the mistake the lifetime guard is there to catch.
[Microsoft.AspNetCore.Mvc.ApiController]
[Microsoft.AspNetCore.Mvc.Route("forgot-to-register/posts")]
public sealed class ForgotToRegisterController(PostCommands commands)
	: Microsoft.AspNetCore.Mvc.ControllerBase
{
	[Microsoft.AspNetCore.Mvc.HttpGet("by-author/{author}")]
	public Task<PooledArray<Post>> ByAuthorAsync(int author)
		=> commands
			.ByAuthor(author)
			.ToPooledBuffer(HttpContext.RequestAborted)
			.Items;
}
