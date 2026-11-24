using System.Buffers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Open.Database.Extensions;
using static Samples.Sql;

namespace Samples;

// Check-only (not shown in the documentation): the lifetime of the RegisterForDispose form of the
// "No service layer" article when the request does not end well, on a test server and on Kestrel.
// The pool balance is counted by a CountingPool, as in the article's lifetime guard.

/// <summary>What the controllers below signal to the check, and the gate that holds a read.</summary>
public sealed class LifetimeProbe
{
	public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
	public TaskCompletionSource Aborted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>Held shut until the check lets the read go on.</summary>
	public ManualResetEventSlim Gate { get; } = new(false);

	/// <summary>The exceptions that left the action, seen by a middleware.</summary>
	public List<Exception> Failures { get; } = [];
}

/// <summary>The command factory of the article, with a query that can be held in its first read.</summary>
public sealed class HeldPostCommands(ArrayPool<Post> pool, LifetimeProbe probe)
{
	static readonly MappedDbQuery<Post> HeldQuery
		= DbQuery.Text("select * from posts where author = @author and wait_for_gate() = 1").Map<Post>();

	readonly DbConnectionFactory<SqliteConnection> _factory = new(() =>
	{
		var connection = new SqliteConnection(Db.ConnectionString);
		int calls = 0;

		// The first row goes through; the read is held in the second, after the first chunk was rented.
		connection.CreateFunction("wait_for_gate", () =>
		{
			if (++calls > 1) probe.Gate.Wait(TimeSpan.FromSeconds(10));
			return 1;
		});
		return connection;
	});

	public MappedQuery<Post> Held(int author)
		=> HeldQuery.Using(_factory).AddParam("author", author).UsePool(pool, clearOnReturn: true);

	public MappedQuery<Post> ByAuthor(int author)
		=> DbQuery.Text(ListSql).Map<Post>().Using(_factory).AddParam("author", author).UsePool(pool, clearOnReturn: true);
}

// The article's controller, but the read is held until the check lets it go: the client aborts while the read runs.
[ApiController]
[Route("lifetime/aborted")]
public sealed class AbortedPostsController(HeldPostCommands commands, LifetimeProbe probe)
	: ControllerBase
{
	[HttpGet("by-author/{author}")]
	public Task<PooledArray<Post>> ByAuthorAsync(int author)
	{
		HttpContext.RequestAborted.Register(static state => ((LifetimeProbe)state!).Aborted.TrySetResult(), probe);
		probe.Started.TrySetResult();
		return commands
			.Held(author)
			.ToAsyncBuffer(HttpContext.RequestAborted)
			.RegisterForDispose(Response);
	}
}

// The article's controller, but the action throws after the buffer was registered and its rows were read.
[ApiController]
[Route("lifetime/throws")]
public sealed class ThrowingPostsController(HeldPostCommands commands)
	: ControllerBase
{
	[HttpGet("by-author/{author}")]
	public async Task<PooledArray<Post>> ByAuthorAsync(int author)
	{
		Task<PooledArray<Post>> rows = commands
			.ByAuthor(author)
			.ToAsyncBuffer(HttpContext.RequestAborted)
			.RegisterForDispose(Response);

		await rows;
		throw new InvalidOperationException("The action failed after the buffer was registered.");
	}
}

/// <summary>Runs the abort and the throw on a test server and on Kestrel and counts the arrays.</summary>
public static class NoServiceLayerLifetimeChecks
{
	public sealed record Outcome(int Rented, int Returned, bool Aborted, string? ClientFailure, string[] Failures, bool Canceled)
	{
		public override string ToString()
			=> $"rented {Rented}, returned {Returned}, RequestAborted {Aborted}, client {ClientFailure ?? "none"}, action failed with [{string.Join(", ", Failures)}]";
	}

	public static async Task RunAsync(Action<bool, string> check)
	{
		Outcome testServerAbort = await AbortAsync(kestrel: false);
		Outcome kestrelAbort = await AbortAsync(kestrel: true);
		Outcome testServerThrow = await ThrowAsync(kestrel: false);
		Outcome kestrelThrow = await ThrowAsync(kestrel: true);

		Console.WriteLine($"  [lifetime] test server, abort: {testServerAbort}");
		Console.WriteLine($"  [lifetime] Kestrel, abort: {kestrelAbort}");
		Console.WriteLine($"  [lifetime] test server, throw: {testServerThrow}");
		Console.WriteLine($"  [lifetime] Kestrel, throw: {kestrelThrow}");

		check(kestrelAbort.Rented > 0 && kestrelAbort.Aborted, "no service layer, Kestrel: a client abort during the read reaches RequestAborted (the read had rented arrays)");
		check(kestrelAbort.Returned == kestrelAbort.Rented, "no service layer, Kestrel: after a client abort during the read, every array went back to the pool");
		check(kestrelThrow.Rented > 0 && kestrelThrow.Returned == kestrelThrow.Rented, "no service layer, Kestrel: when the action throws after RegisterForDispose, every array went back to the pool");
		check(kestrelAbort.Canceled && testServerAbort.Canceled, "no service layer: on a client abort the action's task faults with an OperationCanceledException (a TaskCanceledException)");
		check(kestrelThrow.ClientFailure == "status 500", "no service layer, Kestrel: an action that throws answers 500");

		// What the test server does not do, so a test of your own controller on it cannot see these two cases.
		check(testServerAbort.Rented > 0 && testServerAbort.Returned == 0, "no service layer, test server: after a client abort the registered buffer is NOT disposed (use Kestrel to check it)");
		check(testServerThrow.Rented > 0 && testServerThrow.Returned == 0 && testServerThrow.ClientFailure == nameof(InvalidOperationException), "no service layer, test server: when the action throws the exception reaches the caller and the registered buffer is NOT disposed");
	}

	static async Task<(IHost Host, HttpClient Client, CountingPool<Post> Pool, LifetimeProbe Probe)> StartAsync(bool kestrel)
	{
		CountingPool<Post> pool = new();
		LifetimeProbe probe = new();
		IHost host = new HostBuilder()
			.ConfigureWebHost(web =>
			{
				if (kestrel) web.UseKestrel().UseUrls("http://127.0.0.1:0");
				else web.UseTestServer();

				web.UseContentRoot(AppContext.BaseDirectory)
					.ConfigureServices(services => services
						.AddSingleton<ArrayPool<Post>>(pool)
						.AddSingleton(probe)
						.AddSingleton<HeldPostCommands>()
						.AddControllers())
					.Configure(app => app
						.Use(async (context, next) =>
						{
							try
							{
								await next(context);
							}
							catch (Exception ex)
							{
								lock (probe.Failures) probe.Failures.Add(ex);
								throw;
							}
						})
						.UseRouting()
						.UseEndpoints(static endpoints => endpoints.MapControllers()));
			})
			.Build();
		await host.StartAsync();

		HttpClient client;
		if (kestrel)
		{
			string address = host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
			client = new HttpClient { BaseAddress = new Uri(address) };
		}
		else
		{
			client = host.GetTestClient();
		}

		return (host, client, pool, probe);
	}

	static async Task<Outcome> AbortAsync(bool kestrel)
	{
		(IHost host, HttpClient client, CountingPool<Post> pool, LifetimeProbe probe) = await StartAsync(kestrel);
		using (host)
		using (client)
		{
			using CancellationTokenSource abort = new();
			Task<HttpResponseMessage> call = client.GetAsync("lifetime/aborted/by-author/42", abort.Token);
			await WithinAsync(probe.Started.Task);

			// Wait until the read has rented its first chunk: the abort must find arrays in use.
			using CancellationTokenSource rentLimit = new(TimeSpan.FromSeconds(5));
			while (pool.Rented == 0 && !rentLimit.IsCancellationRequested)
			{
				await Task.Delay(10);
			}

			// The read is held in its first row: the client leaves now.
			await abort.CancelAsync();
			string? clientFailure = null;
			try
			{
				using HttpResponseMessage _ = await call;
			}
			catch (Exception ex)
			{
				clientFailure = ex.GetType().Name;
			}

			bool aborted = await Reached(probe.Aborted.Task);

			// Let the read go on, to the end of the one row it was held in.
			probe.Gate.Set();
			await Settle(pool);
			await Task.Delay(300);
			return Summary(pool, aborted, clientFailure, probe);
		}
	}

	static async Task<Outcome> ThrowAsync(bool kestrel)
	{
		(IHost host, HttpClient client, CountingPool<Post> pool, LifetimeProbe probe) = await StartAsync(kestrel);
		using (host)
		using (client)
		{
			probe.Gate.Set();
			string? clientFailure = null;
			try
			{
				using HttpResponseMessage response = await client.GetAsync("lifetime/throws/by-author/42");
				clientFailure = "status " + (int)response.StatusCode;
			}
			catch (Exception ex)
			{
				clientFailure = ex.GetType().Name;
			}

			await Settle(pool);
			await Task.Delay(300);
			return Summary(pool, false, clientFailure, probe);
		}
	}

	static Outcome Summary(CountingPool<Post> pool, bool aborted, string? clientFailure, LifetimeProbe probe)
	{
		lock (probe.Failures)
			return new(pool.Rented, pool.Returned, aborted, clientFailure, [.. probe.Failures.Select(static ex => ex.GetType().Name)], probe.Failures.Exists(static ex => ex is OperationCanceledException));
	}

	static async Task WithinAsync(Task task)
		=> await task.WaitAsync(TimeSpan.FromSeconds(10));

	static async Task<bool> Reached(Task task)
	{
		try
		{
			await task.WaitAsync(TimeSpan.FromSeconds(2));
			return true;
		}
		catch (TimeoutException)
		{
			return false;
		}
	}

	// The framework disposes what the request registered once the response is over, which can come after the client has its answer: wait for the arrays, with a limit.
	static async Task Settle(CountingPool<Post> pool)
	{
		using CancellationTokenSource limit = new(TimeSpan.FromSeconds(5));
		while (pool.Returned < pool.Rented && !limit.IsCancellationRequested)
		{
			await Task.Delay(10);
		}
	}
}
