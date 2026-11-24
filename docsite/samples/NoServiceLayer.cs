using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Microsoft.Net.Http.Headers;
using Open.Database.Extensions;
using static Samples.Sql;

namespace Samples;

// The examples of the "No service layer" article: a command factory that a
// controller calls directly. NoServiceLayerChecks runs every controller here
// on a test server and checks what the article says about it.

#region nsl_odx
// Command factory: each query defined once.
public sealed class PostCommands(
	IDbConnectionFactory<SqliteConnection> connectionFactory,
	ArrayPool<Post> pool)
{
	static readonly MappedDbQuery<Post> ByAuthorQuery
		= DbQuery.Text(ListSql).Map<Post>();

	// Bound, not run: the caller picks the terminal.
	public MappedQuery<Post> ByAuthor(int author)
		=> ByAuthorQuery
			.Using(connectionFactory)
			.AddParam(author)
			.UsePool(pool, clearOnReturn: true);
}




// No service: nothing to copy.




















// Controller, recommended: the pooled rows, for the serializer.
[ApiController]
[Route("posts")]
public sealed class PostsController(PostCommands commands)
	: ControllerBase
{
	[HttpGet("by-author/{author}")]
	public Task<PooledArray<Post>> ByAuthorAsync(
		int author)
		=> commands
			.ByAuthor(author)
			.ToPooledBuffer(HttpContext.RequestAborted)
			// The framework disposes it after the response,
			// and awaits the rows, then writes them.
			.RegisterForDispose(Response);
}
#endregion

#region nsl_register_for_dispose
// In your app: the framework disposes the buffer after the
// response, so the arrays go back to the pool.
public static class PooledBufferHttpExtensions
{
	public static Task<PooledArray<T>> RegisterForDispose<T>(
		this PooledBuffer<T> buffer,
		HttpResponse response)
	{
		ArgumentNullException.ThrowIfNull(buffer);
		ArgumentNullException.ThrowIfNull(response);

		response.RegisterForDispose(buffer);
		return buffer.Items;
	}
}
#endregion

public static class NoServiceLayerApp
{
	#region nsl_registration
	// Program.cs: the entry point provides the connection factory and the
	// pool; the controllers ask for PostCommands.
	public static IServiceCollection AddPostCommands(
		this IServiceCollection services,
		string connectionString)
		=> services
			.AddSingleton<IDbConnectionFactory<SqliteConnection>>(
				new DbConnectionFactory<SqliteConnection>(
					()
						=> new SqliteConnection(connectionString)))
			.AddSingleton(ArrayPool<Post>.Shared)
			.AddSingleton<PostCommands>();
	#endregion
}

[ApiController]
[Route("list/posts")]
public sealed class ListPostsController(PostCommands commands)
	: ControllerBase
{
	#region nsl_list
	// The plain alternative: a List<Post> that the garbage collector
	// takes after the response. Nothing to register.
	[HttpGet("by-author/{author}")]
	public ValueTask<List<Post>> ByAuthorAsync(int author)
		=> commands
			.ByAuthor(author)
			.ToListAsync(HttpContext.RequestAborted);
	#endregion
}

[ApiController]
[Route("cached/posts")]
public sealed class CachedPostsController(PostCommands commands)
	: ControllerBase
{
	#region nsl_etag
	[HttpGet("by-author/{author}")]
	public async Task<ActionResult<IReadOnlyList<Post>>> ByAuthorAsync(
		int author)
	{
		PooledArray<Post> posts = await commands
			.ByAuthor(author)
			.ToPooledBuffer(HttpContext.RequestAborted)
			.RegisterForDispose(Response);

		// First pass: a weak ETag of the values. A 304 carries it too.
		EntityTagHeaderValue etag = new(ETagOf(posts), isWeak: true);
		Response.GetTypedHeaders().ETag = etag;

		IList<EntityTagHeaderValue> ifNoneMatch
			= Request.GetTypedHeaders().IfNoneMatch;
		if (ifNoneMatch.Any(tag
			=> tag.Equals(EntityTagHeaderValue.Any)
			|| tag.Compare(etag, useStrongComparison: false)))
		{
			return StatusCode(StatusCodes.Status304NotModified);
		}

		// Second pass: the serializer walks the same rows.
		return Ok(posts);
	}
	#endregion

	#region nsl_etag_hash
	// A hash of every field the response carries.
	static string ETagOf(PooledArray<Post> posts)
	{
		using IncrementalHash hash
			= IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

		foreach (Post post in posts)
		{
			Append(hash, post.Id);
			Append(hash, post.Author);
			Append(hash, post.Title);
			Append(hash, post.Body);
		}

		return $"\"{Convert.ToHexString(hash.GetCurrentHash())}\"";
	}

	static void Append(IncrementalHash hash, int value)
	{
		Span<byte> bytes = stackalloc byte[sizeof(int)];
		BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
		hash.AppendData(bytes);
	}

	// The byte count first, so that "ab" + "c" and "a" + "bc" differ.
	// UTF-8, so the bytes are the same on every machine.
	static void Append(IncrementalHash hash, string? value)
	{
		if (value is null)
		{
			Append(hash, -1);
			return;
		}

		byte[] utf8 = ArrayPool<byte>.Shared
			.Rent(Encoding.UTF8.GetMaxByteCount(value.Length));
		int written = Encoding.UTF8.GetBytes(value, utf8);
		Append(hash, written);
		hash.AppendData(utf8, 0, written);
		ArrayPool<byte>.Shared.Return(utf8);
	}
	#endregion
}
