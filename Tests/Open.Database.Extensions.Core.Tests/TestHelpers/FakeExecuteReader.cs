using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A minimal <see cref="IExecuteReader"/> backed by an in-memory column/row set. Each execution
/// is handed a fresh <see cref="DbDataReader"/> (via <see cref="FakeReader.CreateDb"/>), so the
/// async paths exercise the real <c>is DbDataReader</c> streaming branch rather than the
/// synchronous-wrapped fallback. <see cref="ExecuteCount"/> records how many times the query
/// actually ran, which is how the deferred-execution behavior is asserted.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class FakeExecuteReader(IReadOnlyList<string> columns, params object?[][] rows) : IExecuteReader
{
	/// <summary>Number of readers created — i.e. how many times a terminal executed the query.</summary>
	public int ExecuteCount { get; private set; }

	/// <summary>The command-scoped token, chosen when a terminal doesn't supply a cancelable one.</summary>
	public CancellationToken CancellationToken { get; set; }

	DbDataReader NewReader()
	{
		ExecuteCount++;
		return FakeReader.CreateDb(columns, rows);
	}

	public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		handler(reader);
	}

	public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		return transform(reader);
	}

	public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = NewReader();
		await handler(reader).ConfigureAwait(false);
	}

	public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = NewReader();
		return await transform(reader).ConfigureAwait(false);
	}
}
