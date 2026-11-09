using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>The provider's failure when a later statement of a batch fails.</summary>
[ExcludeFromCodeCoverage]
internal sealed class LaterSetFailure(string message) : DbException(message);

/// <summary>
/// A reader with one set of rows and <paramref name="laterSets"/> more result sets after it. It counts every <c>NextResult</c> and <c>Read</c> made after the first set was left,
/// and throws <see cref="LaterSetFailure"/> at the <paramref name="failAtCall"/>-th call of <c>NextResult</c> (sync or async, counted together) when one is given.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class MultiSetReader(object?[][] rows, int laterSets, int? failAtCall = null) : TypedReader(ReaderFixtures.Columns, rows)
{
	int _sets;

	/// <summary>How many times <c>NextResult</c> or <c>NextResultAsync</c> was called.</summary>
	public int NextResultCalls { get; private set; }

	/// <summary>How many of those were the async one.</summary>
	public int AsyncCalls { get; private set; }

	/// <summary>The token the last <c>NextResultAsync</c> was given.</summary>
	public CancellationToken LastToken { get; private set; }

	/// <summary>How many reads were made after the first <c>NextResult</c>: later rows are never read.</summary>
	public int ReadsAfterNextResult { get; private set; }

	public override bool Read()
	{
		if (NextResultCalls > 0) ReadsAfterNextResult++;
		return base.Read();
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		if (NextResultCalls > 0) ReadsAfterNextResult++;
		return base.ReadAsync(cancellationToken);
	}

	public override bool NextResult() => Advance();

	public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
	{
		AsyncCalls++;
		LastToken = cancellationToken;
		return Task.FromResult(Advance());
	}

	bool Advance()
	{
		NextResultCalls++;
		if (NextResultCalls == failAtCall) throw new LaterSetFailure("a later statement failed");
		return _sets++ < laterSets;
	}
}
