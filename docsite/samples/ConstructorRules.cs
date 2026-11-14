namespace Samples;

// Check-only models (not shown in the documentation): the constructor rule at its edges.

// A record with a second public constructor: two public constructors and none without parameters.
public sealed record PairRecord(int Id, string Name)
{
	public PairRecord(int id) : this(id, "") { }
}

// A constructor whose parameters all have defaults still takes parameters, so it does not count as parameterless.
public sealed class AllOptional
{
	public AllOptional(int id = 7, string name = "none") => (Id, Name) = (id, name);
	public AllOptional(int id, string name, int stars) => (Id, Name, Stars) = (id, name, stars);
	public int Id { get; }
	public string Name { get; }
	public int Stars { get; }
}

// A record with a second public constructor that is parameterless: the parameterless one is used.
public sealed record ParameterlessRecord(int Id, string Name)
{
	public ParameterlessRecord() : this(0, "built by new()") { }
}
