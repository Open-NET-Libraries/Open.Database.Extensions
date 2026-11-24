using System.ComponentModel.DataAnnotations.Schema;

namespace Open.Database.Extensions.Generator.Models;

// The benchmark twins (SG.md 5f): the first 1, 3, 5, 10 and 20 members of the Benchmarks project's SynRow, with its types and nullability,
// so Gen (undecorated, read by name) and GenD (decorated, constant ordinals) read exactly the columns V2's Hand(width) reads, at every catalog width.
// Every member is fed by the synthetic catalog of that width, so a passing result set is a full layout.
// The twins of widths 1, 3, 5 and 10 have only those members, while V2's Hand(width) at those widths builds a full SynRow (its 20 members): so at widths
// below 20 a twin allocates less than SynRow, and Gen (and GenD) there is NOT comparable with V2 in time or in allocation: only width 20, where the twin
// has all 20 members, is. At width 20 GenRow20 and GenRowD20 differ from SynRow in being separate classes with the same members.

/// <summary>Width 1, undecorated.</summary>
public sealed class GenRow1
{
	public int Id { get; set; }
}

/// <summary>Width 1, decorated.</summary>
public sealed class GenRowD1
{
	[Column("Id", Order = 0)] public int Id { get; set; }
}

/// <summary>Width 3, undecorated.</summary>
public sealed class GenRow3
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
}

/// <summary>Width 3, decorated.</summary>
public sealed class GenRowD3
{
	[Column("Id", Order = 0)] public int Id { get; set; }
	[Column("Created", Order = 1)] public DateTime Created { get; set; }
	[Column("Title", Order = 2)] public string? Title { get; set; }
}

/// <summary>Width 5, undecorated.</summary>
public sealed class GenRow5
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
}

/// <summary>Width 5, decorated: [Column(Name, Order)] on every member.</summary>
public sealed class GenRowD5
{
	[Column("Id", Order = 0)] public int Id { get; set; }
	[Column("Created", Order = 1)] public DateTime Created { get; set; }
	[Column("Title", Order = 2)] public string? Title { get; set; }
	[Column("Price", Order = 3)] public decimal Price { get; set; }
	[Column("Qty", Order = 4)] public int? Qty { get; set; }
}

/// <summary>Width 10, undecorated.</summary>
public sealed class GenRow10
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
	public bool IsActive { get; set; }
	public Guid Key { get; set; }
	public double Score { get; set; }
	public string? Note { get; set; }
	public byte[]? Blob { get; set; }
}

/// <summary>Width 10, decorated.</summary>
public sealed class GenRowD10
{
	[Column("Id", Order = 0)] public int Id { get; set; }
	[Column("Created", Order = 1)] public DateTime Created { get; set; }
	[Column("Title", Order = 2)] public string? Title { get; set; }
	[Column("Price", Order = 3)] public decimal Price { get; set; }
	[Column("Qty", Order = 4)] public int? Qty { get; set; }
	[Column("IsActive", Order = 5)] public bool IsActive { get; set; }
	[Column("Key", Order = 6)] public Guid Key { get; set; }
	[Column("Score", Order = 7)] public double Score { get; set; }
	[Column("Note", Order = 8)] public string? Note { get; set; }
	[Column("Blob", Order = 9)] public byte[]? Blob { get; set; }
}

/// <summary>Width 20, undecorated.</summary>
public sealed class GenRow20
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string? Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
	public bool IsActive { get; set; }
	public Guid Key { get; set; }
	public double Score { get; set; }
	public string? Note { get; set; }
	public byte[]? Blob { get; set; }
	public long Big { get; set; }
	public string? Body { get; set; }
	public decimal? Amount { get; set; }
	public DateTime? Stamp { get; set; }
	public string? Tag { get; set; }
	public double Ratio { get; set; }
	public bool? Flag { get; set; }
	public Guid? Link { get; set; }
	public long Count { get; set; }
	public string? Summary { get; set; }
}

/// <summary>Width 20, decorated: [Column(Name, Order)] on every member.</summary>
public sealed class GenRowD20
{
	[Column("Id", Order = 0)] public int Id { get; set; }
	[Column("Created", Order = 1)] public DateTime Created { get; set; }
	[Column("Title", Order = 2)] public string? Title { get; set; }
	[Column("Price", Order = 3)] public decimal Price { get; set; }
	[Column("Qty", Order = 4)] public int? Qty { get; set; }
	[Column("IsActive", Order = 5)] public bool IsActive { get; set; }
	[Column("Key", Order = 6)] public Guid Key { get; set; }
	[Column("Score", Order = 7)] public double Score { get; set; }
	[Column("Note", Order = 8)] public string? Note { get; set; }
	[Column("Blob", Order = 9)] public byte[]? Blob { get; set; }
	[Column("Big", Order = 10)] public long Big { get; set; }
	[Column("Body", Order = 11)] public string? Body { get; set; }
	[Column("Amount", Order = 12)] public decimal? Amount { get; set; }
	[Column("Stamp", Order = 13)] public DateTime? Stamp { get; set; }
	[Column("Tag", Order = 14)] public string? Tag { get; set; }
	[Column("Ratio", Order = 15)] public double Ratio { get; set; }
	[Column("Flag", Order = 16)] public bool? Flag { get; set; }
	[Column("Link", Order = 17)] public Guid? Link { get; set; }
	[Column("Count", Order = 18)] public long Count { get; set; }
	[Column("Summary", Order = 19)] public string? Summary { get; set; }
}

#pragma warning disable CS8618 // The non-nullable strings are always set by the reader (a NULL there throws); no initializer, so new() stores nothing, as SynRow's.

/// <summary>
/// Width 20, undecorated, with the member types V2's hand-written selector assumes: Title, Body and Tag are non-nullable <c>string</c> (V2 reads them
/// with GetString alone), the rest as <see cref="GenRow20"/>. The generator reads a non-nullable member without an IsDBNull test, so this twin's
/// generated reader reads V2's cells the way V2 does for every member but one: IsDBNull on Qty, Note, Amount, Stamp, Flag, Link and Summary only, the
/// strings alone. The exception is Blob: it is <c>byte[]?</c> here, so the generator tests the value of GetValue for NULL (a compare per row), where V2
/// casts it alone. It builds a GenRowT20 (its own class), not V2's SynRow. <see cref="GenRow20"/> stays as it was (string? members, tested), so earlier
/// numbers stay comparable.
/// </summary>
public sealed class GenRowT20
{
	public int Id { get; set; }
	public DateTime Created { get; set; }
	public string Title { get; set; }
	public decimal Price { get; set; }
	public int? Qty { get; set; }
	public bool IsActive { get; set; }
	public Guid Key { get; set; }
	public double Score { get; set; }
	public string? Note { get; set; }
	public byte[]? Blob { get; set; }
	public long Big { get; set; }
	public string Body { get; set; }
	public decimal? Amount { get; set; }
	public DateTime? Stamp { get; set; }
	public string Tag { get; set; }
	public double Ratio { get; set; }
	public bool? Flag { get; set; }
	public Guid? Link { get; set; }
	public long Count { get; set; }
	public string? Summary { get; set; }
}

#pragma warning restore CS8618
