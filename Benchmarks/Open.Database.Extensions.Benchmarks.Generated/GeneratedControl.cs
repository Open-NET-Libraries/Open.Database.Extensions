using System.Data;
using Open.Database.Extensions.Generator.Models;
using Open.Database.Extensions.Synchronous;

namespace Open.Database.Extensions.Benchmarks.Generated;

/// <summary>
/// The control of Gen: V2's hand-written selector for width 20, copied as it is (constant ordinals, <c>IsDBNull</c> on the nullable members only),
/// but compiled in this assembly, on the generator's twin of the row, through the same public <c>Map(Func)</c> entry the generated reader uses.
/// If Gen is slower than this, the generated code is the cause, not the assembly, the type or the entry.
/// </summary>
public static class GeneratedControl
{
	public static object Control20(IExecuteReader command)
		=> command.Map(Read20).ToList();

	static GenRow20 Read20(IDataRecord r) => new()
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
		Blob = (byte[])r.GetValue(9),
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
