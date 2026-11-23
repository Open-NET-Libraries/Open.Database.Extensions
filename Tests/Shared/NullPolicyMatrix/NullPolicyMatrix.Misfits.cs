#nullable enable

namespace Open.Database.Extensions.Tests.NullPolicyMatrix;

// The models of Matrix.Misfits: a [NullValue] that does not convert to its member's type.
// The map is never created, whatever the rows: the error is the model's, not a row's.
public static partial class Matrix
{
	public sealed class BadIntFromText
	{
		[NullValue("x")] public int A { get; set; }
	}

	public sealed class BadIntFromNull
	{
		[NullValue(null)] public int A { get; set; }
	}

	public sealed class BadGuidFromText
	{
		[NullValue("nope")] public Guid A { get; set; }
	}

	public sealed class BadDateFromText
	{
		[NullValue("not a date")] public DateTime A { get; set; }
	}

	public sealed class BadByteOverflow
	{
		[NullValue(300)] public byte A { get; set; }
	}

	public sealed record BadRecordParameter([NullValue("x")] int A);
}
