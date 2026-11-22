using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using static Samples.Sql;

namespace Samples.Classic;

// The classic stack of the "No service layer" article, for the side-by-side.
// It reads the same rows and writes the same JSON as the ODX version.
#region nsl_classic
// Repository: rows into entities.
public sealed class PostRepository(string connectionString)
{
	public async Task<List<PostEntity>> ByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
	{
		await using SqliteConnection connection
			= new(connectionString);
		IEnumerable<PostEntity> rows
			= await connection.QueryAsync<PostEntity>(
				new CommandDefinition(
					ListSql,
					new { author },
					cancellationToken: cancellationToken));
		return rows.AsList();
	}
}

// Service: entities copied into DTOs.
public sealed class PostService(PostRepository repository)
{
	public async Task<List<PostDto>> ByAuthorAsync(
		int author,
		CancellationToken cancellationToken)
	{
		List<PostEntity> entities = await repository
			.ByAuthorAsync(author, cancellationToken);
		return entities
			.Select(static e => new PostDto
			{
				Id = e.Id,
				Author = e.Author,
				Title = e.Title,
				Body = e.Body,
			})
			.ToList();
	}
}

// Controller: the DTO list, for the serializer.
[ApiController]
[Route("classic/posts")]
public sealed class PostsController(PostService service)
	: ControllerBase
{
	[HttpGet("by-author/{author}")]
	public Task<List<PostDto>> ByAuthorAsync(int author)
		=> service.ByAuthorAsync(
			author,
			HttpContext.RequestAborted);
}
#endregion

// The entity and the DTO of the classic stack: the same four properties.
public sealed class PostEntity
{
	public required int Id { get; init; }
	public int Author { get; init; }
	public string Title { get; init; } = "";
	public string? Body { get; init; }
}

public sealed class PostDto
{
	public required int Id { get; init; }
	public int Author { get; init; }
	public string Title { get; init; } = "";
	public string? Body { get; init; }
}

