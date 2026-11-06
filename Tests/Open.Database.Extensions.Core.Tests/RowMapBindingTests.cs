using System.Data.Common;
using Open.Database.Extensions.Core;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Compile-time guard: a bare `null` for the map must bind to the RowMap<T>? overload, never be ambiguous with the tuple convenience overloads.
// Nothing runs; if the overload priorities regress, this file stops compiling.
[ExcludeFromCodeCoverage]
static class RowMapBindingTests
{
	[SuppressMessage("Usage", "CA1801", Justification = "Compile-only.")]
	static void NullMapBindsToTheMapOverload(DbCommand command, DbDataReader reader, CancellationToken cancellationToken)
	{
		_ = command.ResultsAsync<RowMapTests.Item>(null, cancellationToken);
		_ = command.ResultsAsync<RowMapTests.Item>(null, true, cancellationToken);
		_ = reader.ResultsAsync<RowMapTests.Item>(null, cancellationToken);
		_ = reader.ResultsBufferedAsync<RowMapTests.Item>(null, cancellationToken);
		_ = reader.ResultsBufferedAsync<RowMapTests.Item>(null, true, cancellationToken);
	}
}
