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

	/// <summary>The command-scoped token. A terminal combines it with the token it was given and passes the result to <see cref="ExecuteReaderAsync{TState, TResult}(TState, Func{IDataReader, TState, ValueTask{TResult}}, CommandBehavior, CancellationToken)"/>.</summary>
	public CancellationToken CancellationToken { get; set; }

	/// <summary>The token each call of the token overload of <c>ExecuteReaderAsync</c> was given, in order.</summary>
	public List<CancellationToken> ReaderTokens { get; } = [];

	/// <summary>The <see cref="CommandBehavior"/> each execution was asked for, in order.</summary>
	public List<CommandBehavior> Behaviors { get; } = [];

	readonly Func<DbDataReader>? _factory;

	/// <summary>Creates a command whose every execution is handed the reader the factory builds (e.g. a typed one).</summary>
	public FakeExecuteReader(Func<DbDataReader> factory) : this([]) => _factory = factory;

	DbDataReader NewReader(CommandBehavior behavior)
	{
		ExecuteCount++;
		Behaviors.Add(behavior);
		return _factory?.Invoke() ?? FakeReader.CreateDb(columns, rows);
	}

	public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader(behavior);
		handler(reader);
	}

	public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader(behavior);
		return transform(reader);
	}

	public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = NewReader(behavior);
		await handler(reader).ConfigureAwait(false);
	}

	public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = NewReader(behavior);
		return await transform(reader).ConfigureAwait(false);
	}

	public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader(behavior);
		return transform(reader, state);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		await using DbDataReader reader = NewReader(behavior);
		return await transform(reader, state).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
	{
		ReaderTokens.Add(cancellationToken);
		cancellationToken.ThrowIfCancellationRequested(); // as a real command does
		await using DbDataReader reader = NewReader(behavior);
		return await transform(reader, state).ConfigureAwait(false);
	}
}
