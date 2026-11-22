using System.Buffers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Open.Database.Extensions;
using Xunit;

namespace Samples;

// The tests of the "No service layer" article, written for xUnit v3.
// Program.cs runs them too, through SamplesAppFactory.

#region nsl_test_commands
// Every class that touches the test database joins this collection: the
// database is filled once, and the classes do not run at the same time.
[CollectionDefinition(nameof(DatabaseCollection))]
public sealed class DatabaseCollection : ICollectionFixture<DatabaseFixture>;

// The command factory, against a real database: the SQL and the mapping.
[Collection(nameof(DatabaseCollection))]
public sealed class PostCommandsTests(DatabaseFixture database)
{
	[Fact]
	public async Task ByAuthorReadsThatAuthorsPosts()
	{
		PostCommands commands = new(
			database.ConnectionFactory,
			ArrayPool<Post>.Shared);

		List<Post> posts = await commands
			.ByAuthor(42)
			.ToListAsync(TestContext.Current.CancellationToken);

		Assert.Equal([1, 2, 3], posts.Select(static p => p.Id));
		Assert.All(posts, static p => Assert.Equal(42, p.Author));
	}
}
#endregion

#region nsl_test_endpoint
// The controller, end to end: routing, the query, the JSON.
[Collection(nameof(DatabaseCollection))]
public sealed class PostsEndpointTests(WebApplicationFactory<Program> app)
	: IClassFixture<WebApplicationFactory<Program>>
{
	[Fact]
	public async Task ReturnsThePostsAsJson()
	{
		using HttpClient client = app.CreateClient();

		List<Post>? posts = await client.GetFromJsonAsync<List<Post>>(
			"posts/by-author/42",
			TestContext.Current.CancellationToken);

		Assert.NotNull(posts);
		Assert.Equal([1, 2, 3], posts.Select(static p => p.Id));
	}

	// The lifetime guard: every array the request rented came back.
	[Fact]
	public async Task GivesEveryArrayBack()
	{
		CountingPool<Post> pool = new();
		await using WebApplicationFactory<Program> counted = app
			.WithWebHostBuilder(web => web.ConfigureServices(
				services => services.AddSingleton<ArrayPool<Post>>(pool)));
		using HttpClient client = counted.CreateClient();

		using HttpResponseMessage response = await client.GetAsync(
			"posts/by-author/42",
			TestContext.Current.CancellationToken);
		response.EnsureSuccessStatusCode();

		// The framework disposes the buffer once the response is sent,
		// so the client can get here first: give it up to five seconds.
		using CancellationTokenSource limit = new(TimeSpan.FromSeconds(5));
		while (pool.Returned < pool.Rented && !limit.IsCancellationRequested)
			await Task.Delay(10, TestContext.Current.CancellationToken);

		Assert.True(pool.Rented > 0);
		Assert.Equal(pool.Rented, pool.Returned);
	}
}
#endregion

/// <summary>The test database of the samples: the shared in-memory SQLite database.</summary>
public sealed class DatabaseFixture
{
	// Created once per collection: (re)creates and fills the tables.
	public DatabaseFixture()
	{
		Db.Seed();
		ConnectionFactory = Db.Factory;
	}

	public IDbConnectionFactory<SqliteConnection> ConnectionFactory { get; }
}
