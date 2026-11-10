using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// An <see cref="IExecuteReaderAsync"/> that records the token its four-parameter <c>ExecuteReaderAsync</c> receives (the one that governs the call)
/// and completes synchronously unless <see cref="Stall"/> is set, so the allocation tests can use it.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class RecordingAsyncExecuteReader(IReadOnlyList<string> columns, params object?[][] rows) : IExecuteReaderAsync
{
	public CancellationToken CancellationToken { get; set; }

	public bool UseAsyncRead { get; set; }

	/// <summary>The token each call of the four-parameter member was given.</summary>
	public List<CancellationToken> ReaderTokens { get; } = [];

	/// <summary>When set, the execute waits, without end, for its token to be canceled (what a slow server does) after signaling <see cref="Entered"/>.</summary>
	public bool Stall { get; set; }

	public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>Turn off to record nothing (the allocation tests must not measure the recording).</summary>
	public bool Recording { get; set; } = true;

	DbDataReader NewReader() => FakeReader.CreateDb(columns, rows);

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

	public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		return transform(reader, state);
	}

	public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		await handler(reader).ConfigureAwait(false);
	}

	public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		return await transform(reader).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using DbDataReader reader = NewReader();
		return await transform(reader, state).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
	{
		if (Recording) ReaderTokens.Add(cancellationToken);
		cancellationToken.ThrowIfCancellationRequested(); // as a real command does
		if (Stall)
		{
			Entered.TrySetResult();
			await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
		}

		using DbDataReader reader = NewReader();
		return await transform(reader, state).ConfigureAwait(false);
	}
}
