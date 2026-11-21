using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;

namespace Open.Database.Extensions.Generator.Models;

/// <summary>An enum read from an integer column.</summary>
public enum PostState
{
	Draft,
	Active,
	Archived,
}

// ---- created then set: a public parameterless constructor and settable properties ----

/// <summary>The undecorated twin: every member read by name (the three tiers).</summary>
public sealed class PlainPost
{
	public int Id { get; set; }

	public DateTime Created { get; set; }

	// Non-nullable: a NULL keeps the initializer.
	public string Title { get; set; } = "untitled";

	public decimal Price { get; set; }

	public int? Qty { get; set; }

	public bool IsActive { get; set; }

	public Guid Key { get; set; }

	public double Score { get; set; }

	public string? Note { get; set; }

	public byte[]? Blob { get; set; }

	public long Big { get; set; }

	public PostState State { get; set; } = PostState.Active;

	// [AllowNull] makes a non-nullable reference nullable to the map: a NULL is assigned.
	[AllowNull]
	public string Label { get; set; } = "label";
}

/// <summary>The decorated twin: every member declares its column and its ordinal; one member is not mapped.</summary>
public sealed class DecoratedPost
{
	[Column("post_id", Order = 0)]
	public int Id { get; set; }

	[Column("created_on", Order = 1)]
	public DateTime Created { get; set; }

	[Column("title", Order = 2)]
	public string Title { get; set; } = "untitled";

	[Column("price", Order = 3)]
	public decimal Price { get; set; }

	[Column("qty", Order = 4)]
	public int? Qty { get; set; }

	[Column("is_active", Order = 5)]
	public bool IsActive { get; set; }

	[Column("post_key", Order = 6)]
	public Guid Key { get; set; }

	[Column("score", Order = 7)]
	public double Score { get; set; }

	[Column("note", Order = 8)]
	public string? Note { get; set; }

	[Column("blob", Order = 9)]
	public byte[]? Blob { get; set; }

	[Column("big", Order = 10)]
	public long Big { get; set; }

	[Column("state", Order = 11)]
	public PostState State { get; set; } = PostState.Active;

	[Column("label", Order = 12)]
	[AllowNull]
	public string Label { get; set; } = "label";

	// Not a column, whatever the result holds: keeps its initializer.
	[NotMapped]
	public string Display { get; set; } = "kept";
}

/// <summary>A decorated twin that names its columns but declares no ordinals: matched by the declared name (exactly or ignoring case).</summary>
public sealed class NamedPost
{
	[Column("post_id")]
	public int Id { get; set; }

	[Column("title")]
	public string Title { get; set; } = "untitled";

	// Undecorated: still read by name, all three tiers.
	public string? Note { get; set; }
}

// ---- built through the one public constructor ----

/// <summary>The undecorated positional record: each parameter is fed like a property; one more property is set after.</summary>
public sealed record PlainRecord(int Id, string Title, int? Qty, PostState State = PostState.Archived, string Tag = "none")
{
	public string? Note { get; set; }
}

/// <summary>The decorated positional record.</summary>
public sealed record DecoratedRecord(
	[property: Column("post_id", Order = 0)] int Id,
	[property: Column("title", Order = 1)] string Title,
	[property: Column("qty", Order = 2)] int? Qty,
	[property: Column("state", Order = 3)] PostState State = PostState.Archived)
{
	[NotMapped]
	public string? Note { get; set; } = "kept";
}

// ---- init-only and required members: an object initializer ----

/// <summary>The undecorated init model: required members need a column; a NULL into a non-nullable one keeps what the type gives it.</summary>
public sealed class PlainInit
{
	public required int Id { get; init; }

	public required string Title { get; init; }

	public string? Note { get; init; }

	public decimal Price { get; set; }
}

/// <summary>The decorated init model.</summary>
public sealed class DecoratedInit
{
	[Column("post_id", Order = 0)]
	public required int Id { get; init; }

	[Column("title", Order = 1)]
	public required string Title { get; init; }

	[Column("note", Order = 2)]
	public string? Note { get; init; }

	[Column("price", Order = 3)]
	public decimal Price { get; set; }
}

// ---- other shapes ----

/// <summary>A struct created as its default value, then set.</summary>
public struct PlainStruct
{
	public int Id { get; set; }

	public string? Title { get; set; }
}

/// <summary>A base type whose public properties a derived model inherits.</summary>
public class PostBase
{
	public int Id { get; set; }

	public string? Title { get; set; }
}

/// <summary>A derived model: its own properties come first, then the base type's (reflection's order).</summary>
public sealed class DerivedPost : PostBase
{
	public string? Note { get; set; }
}
