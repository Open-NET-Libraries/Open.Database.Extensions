namespace Open.Database.Extensions.Benchmarks;

// Twins of SynRow with a NULL policy, to measure what the opt-in check costs next to V1_Sync. Same members and types as SynRow.
// The synthetic table holds NULLs in its nullable columns, and a policy always applies (nullable members included), so a Throw on the class would throw
// on the data: SynRowThrow puts Throw on each NON-nullable member (every one of them is tested for NULL), and SynRowDefault puts Default on the class
// (every member is tested, a NULL keeps the default).
public sealed class SynRowThrow
{
	[NullPolicy(NullAction.Throw)] public int Id { get; set; }
	[NullPolicy(NullAction.Throw)] public DateTime Created { get; set; }
	[NullPolicy(NullAction.Throw)] public string Title { get; set; } = "";
	[NullPolicy(NullAction.Throw)] public decimal Price { get; set; }
	public int? Qty { get; set; }
	[NullPolicy(NullAction.Throw)] public bool IsActive { get; set; }
	[NullPolicy(NullAction.Throw)] public Guid Key { get; set; }
	[NullPolicy(NullAction.Throw)] public double Score { get; set; }
	public string? Note { get; set; }
	[NullPolicy(NullAction.Throw)] public byte[] Blob { get; set; } = [];
	[NullPolicy(NullAction.Throw)] public long Big { get; set; }
	[NullPolicy(NullAction.Throw)] public string Body { get; set; } = "";
	public decimal? Amount { get; set; }
	public DateTime? Stamp { get; set; }
	[NullPolicy(NullAction.Throw)] public string Tag { get; set; } = "";
	[NullPolicy(NullAction.Throw)] public double Ratio { get; set; }
	public bool? Flag { get; set; }
	public Guid? Link { get; set; }
	[NullPolicy(NullAction.Throw)] public long Count { get; set; }
	public string? Summary { get; set; }
}

[NullPolicy(NullAction.Default)]
public sealed class SynRowDefault
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
