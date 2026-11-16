namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A <see cref="PlainReaderCommand"/> that is also an
/// <see cref="IExecuteReaderAsync"/>, so the command-level async helpers
/// (<c>IterateReaderAsync</c>) can be run over a plain reader.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class PlainAsyncReaderCommand(Func<IDataReader> factory)
	: PlainReaderCommand(factory), IExecuteReaderAsync
{
	public bool UseAsyncRead => false;
}
