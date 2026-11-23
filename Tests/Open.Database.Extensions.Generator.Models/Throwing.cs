namespace Open.Database.Extensions.Generator.Models;

// Models whose own code throws while a row is built: the error must name the same member (or none, for a constructor) as the runtime map's.

/// <summary>A created-then-set model with a user-written setter that refuses a negative value.</summary>
public sealed class CheckedPost
{
	int _qty;

	public int Id { get; set; }

	public int Qty
	{
		get => _qty;
		set => _qty = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), value, "Qty cannot be negative.");
	}

	public string? Title { get; set; }
}

/// <summary>A created-then-set model whose parameterless constructor throws.</summary>
public sealed class FailingPost
{
	public FailingPost() => throw new NotSupportedException("This model cannot be created.");

	public int Id { get; set; }
}

/// <summary>A positional record whose constructor refuses a negative Id.</summary>
public sealed record CheckedRecord(int Id, string? Title)
{
	public int Id { get; } = Id >= 0 ? Id : throw new ArgumentOutOfRangeException(nameof(Id), Id, "Id cannot be negative.");

	public string? Note { get; set; }
}

/// <summary>An init model: a required init-only member whose init accessor refuses a negative value, and a settable member that refuses an empty text.</summary>
public sealed class CheckedInit
{
	readonly int _id;
	string? _note;

	public required int Id
	{
		get => _id;
		init => _id = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value), value, "Id cannot be negative.");
	}

	public string? Title { get; init; }

	public string? Note
	{
		get => _note;
		set => _note = value is "" ? throw new ArgumentException("Note cannot be empty.", nameof(value)) : value;
	}
}
