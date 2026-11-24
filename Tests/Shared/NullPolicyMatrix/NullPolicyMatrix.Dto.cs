using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;

#nullable enable

namespace Open.Database.Extensions.Tests.NullPolicyMatrix;

// One DTO that is both the row model and the API response: ODX's declarations ([Column], [NotMapped], [NullPolicy], [NullValue], required)
// decide the mapping alone; validation attributes change nothing in it, and JSON attributes shape only the JSON.
// A reader maps Dto.Rows (a NULL first, then values that break the validation, then a valid row), serializes each with System.Text.Json
// and validates it, and maps Dto.FailingRows, which must throw Dto.FailingMessage.
[NullPolicy(NullAction.Throw)]
public sealed class PostDto
{
	[Key]
	[Column(Order = 0)]
	public int Id { get; set; }

	[Column("post_title")]
	[Required]
	[MaxLength(20)]
	[NullValue("")]
	[JsonPropertyName("title")]
	public string Title { get; set; } = "";

	[Range(0, 100)]
	[NullValue(-1)]
	[JsonPropertyName("score")]
	public int Score { get; set; }

	// Hidden from the JSON, still mapped, and under the type's Throw policy.
	[JsonIgnore]
	public string? Secret { get; set; }

	public required string Author { get; init; }

	// Not mapped although a column of its name comes back; still in the JSON.
	[NotMapped]
	[JsonPropertyName("note")]
	public string Note { get; set; } = "not mapped";
}

/// <summary>What one row of <see cref="Dto.Rows"/> must give.</summary>
public sealed class DtoExpected(int id, string title, int score, string? secret, string author, string json, string[] invalid)
{
	public int Id { get; } = id;
	public string Title { get; } = title;
	public int Score { get; } = score;
	public string? Secret { get; } = secret;
	public string Author { get; } = author;

	/// <summary>The JSON System.Text.Json writes with its default options.</summary>
	public string Json { get; } = json;

	/// <summary>The members <c>Validator.TryValidateObject</c> reports (all properties validated), in order.</summary>
	public string[] Invalid { get; } = invalid;
}

public static class Dto
{
	/// <summary>The column names, in order: Id must be at ordinal 0.</summary>
	public static readonly string[] ColumnNames = ["Id", "post_title", "Score", "Secret", "Author", "Note"];

	/// <summary>The column types, in the order of <see cref="ColumnNames"/>.</summary>
	public static readonly Type[] ColumnTypes = [typeof(int), typeof(string), typeof(int), typeof(string), typeof(string), typeof(string)];

	public static readonly object?[][] Rows =
	[
		[1, null, null, "s1", "ann", "from the database"],
		[2, "a title far too long for it", 500, "s2", "bob", "from the database"],
		[3, "Hi", 50, "s3", "cy", "from the database"],
	];

	public static readonly DtoExpected[] Expected =
	[
		new(1, "", -1, "s1", "ann", """{"Id":1,"title":"","score":-1,"Author":"ann","note":"not mapped"}""", ["Title", "Score"]),
		new(2, "a title far too long for it", 500, "s2", "bob", """{"Id":2,"title":"a title far too long for it","score":500,"Author":"bob","note":"not mapped"}""", ["Title", "Score"]),
		new(3, "Hi", 50, "s3", "cy", """{"Id":3,"title":"Hi","score":50,"Author":"cy","note":"not mapped"}""", []),
	];

	/// <summary>A valid row, then a NULL into the [JsonIgnore] member, which the type's Throw policy still covers.</summary>
	public static readonly object?[][] FailingRows =
	[
		[3, "Hi", 50, "s3", "cy", null],
		[4, "Hi", 50, null, "di", null],
	];

	public const string FailingMessage = "A NULL was read from column 'Secret' into 'Secret' of PostDto at row 1, whose null policy is Throw.";
}
