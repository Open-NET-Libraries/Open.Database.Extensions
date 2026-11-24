using System.Data;
using Open.Database.Extensions.Generator.Models;
using Open.Database.Extensions.Synchronous;

namespace Open.Database.Extensions.Benchmarks.Generated;

/// <summary>
/// The controls of Gen and Gen_T: the selector the generator would write for each twin, written by hand and compiled in this assembly, through the same
/// public <c>Map(Func)</c> entry the generated reader uses (width 20 only). Each reads with the NULL tests the generator makes for that twin's member
/// types, so each IS the control of its generated reader, not an approximation of V2:
/// <list type="bullet">
/// <item><see cref="Control20"/> (Ctl) on <see cref="GenRow20"/>, whose Title, Body, Tag and Blob are nullable: tested for NULL (a NULL gives null).
/// If Gen is slower than this, the generated code is the cause, not the assembly, the type or the entry.</item>
/// <item><see cref="ControlT20"/> (Ctl_T) on <see cref="GenRowT20"/>, whose Title, Body and Tag are non-nullable (read alone, as V2 reads them) and whose
/// Blob is nullable (tested). It differs from V2 in that one NULL test on Blob (V2 casts <c>GetValue</c> alone), which is a compare per row.</item>
/// </list>
/// V2 itself builds a <c>SynRow</c> at every width and reads Blob without the test: Gen and Ctl at width 20 are not V2 (Ctl_T is closest, by one compare).
/// </summary>
public static class GeneratedControl
{
	public static object Control20(IExecuteReader command)
		=> command.Map(Read20).ToList();

	/// <summary>The delegate of <see cref="Control20S"/>: the SAME method as <see cref="Control20"/> reads (Read20), converted ONCE into a static readonly field.</summary>
	static readonly Func<IDataRecord, GenRow20> Read20Delegate = Read20;

	/// <summary>
	/// CtlS: Control20 with ONE thing changed, the delegate. Control20 passes the method group (a delegate converted at each call, which C# 11+ caches in a hidden static
	/// field, but through a different path than V2's own static readonly <c>Hand20</c>); this passes a static readonly <c>Func</c> field, as V2 does. Same selector, same
	/// row type, same assembly: a difference from Ctl_Sync isolates the DELEGATE CREATION per call.
	/// </summary>
	public static object Control20S(IExecuteReader command)
		=> command.Map(Read20Delegate).ToList();
	/// <summary>The control of Gen_T: the same selector on <see cref="GenRowT20"/> (its non-nullable strings read alone).</summary>
	public static object ControlT20(IExecuteReader command)
		=> command.Map(ReadT20).ToList();

	// V2's selector on the generated twin: one object initializer, constant ordinals; a nullable member tested with IsDBNull,
	// a byte[]? through GetValue tested on the cell.
	static GenRow20 Read20(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.IsDBNull(2) ? null : r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
		IsActive = r.GetBoolean(5),
		Key = r.GetGuid(6),
		Score = r.GetDouble(7),
		Note = r.IsDBNull(8) ? null : r.GetString(8),
		Blob = r.IsDBNull(9) ? null : (byte[])r.GetValue(9),
		Big = r.GetInt64(10),
		Body = r.IsDBNull(11) ? null : r.GetString(11),
		Amount = r.IsDBNull(12) ? null : r.GetDecimal(12),
		Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13),
		Tag = r.IsDBNull(14) ? null : r.GetString(14),
		Ratio = r.GetDouble(15),
		Flag = r.IsDBNull(16) ? null : r.GetBoolean(16),
		Link = r.IsDBNull(17) ? null : r.GetGuid(17),
		Count = r.GetInt64(18),
		Summary = r.IsDBNull(19) ? null : r.GetString(19),
	};

	static GenRowT20 ReadT20(IDataRecord r) => new()
	{
		Id = r.GetInt32(0),
		Created = r.GetDateTime(1),
		Title = r.GetString(2),
		Price = r.GetDecimal(3),
		Qty = r.IsDBNull(4) ? null : r.GetInt32(4),
		IsActive = r.GetBoolean(5),
		Key = r.GetGuid(6),
		Score = r.GetDouble(7),
		Note = r.IsDBNull(8) ? null : r.GetString(8),
		Blob = r.IsDBNull(9) ? null : (byte[])r.GetValue(9),
		Big = r.GetInt64(10),
		Body = r.GetString(11),
		Amount = r.IsDBNull(12) ? null : r.GetDecimal(12),
		Stamp = r.IsDBNull(13) ? null : r.GetDateTime(13),
		Tag = r.GetString(14),
		Ratio = r.GetDouble(15),
		Flag = r.IsDBNull(16) ? null : r.GetBoolean(16),
		Link = r.IsDBNull(17) ? null : r.GetGuid(17),
		Count = r.GetInt64(18),
		Summary = r.IsDBNull(19) ? null : r.GetString(19),
	};
}
