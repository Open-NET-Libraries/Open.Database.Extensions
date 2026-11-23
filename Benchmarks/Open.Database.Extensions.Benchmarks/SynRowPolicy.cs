namespace Open.Database.Extensions.Benchmarks;

// SynRow's twin with a NULL policy on the class (the benchmark of the cost of the opt-in check: every member is tested for NULL).
// Same members and types as SynRow, so V1_Throw_Sync differs from V1_Sync only by the policy.
[NullPolicy(NullAction.Throw)]
public sealed class SynRowThrow
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
