namespace Open.Database.Extensions.Benchmarks;

/// <summary>A representative row: 12 columns, mixed types, several nullable.</summary>
public class Post
{
	public int Id { get; set; }
	public string Title { get; set; } = "";
	public string? Body { get; set; }
	public DateTime Created { get; set; }
	public decimal Price { get; set; }
	public bool IsPublished { get; set; }
	public int? ViewCount { get; set; }
	public double Score { get; set; }
	public Guid Key { get; set; }
	public short Rank { get; set; }
	public long Big { get; set; }
	public string? Tag { get; set; }
}
