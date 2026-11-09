namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// An <see cref="IExecuteReader"/> whose readers are a plain <see cref="IDataReader"/>, not a <c>DbDataReader</c>: the library has a separate,
/// synchronous path for them. Every execution is handed a fresh reader from the factory and disposes it, as a command does.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class PlainReaderCommand(Func<IDataReader> factory) : IExecuteReader
{
	public CancellationToken CancellationToken => default;

	public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		handler(reader);
	}

	public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		return transform(reader);
	}

	public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		return transform(reader, state);
	}

	public async ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		await handler(reader).ConfigureAwait(false);
	}

	public async ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		return await transform(reader).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		using IDataReader reader = factory();
		return await transform(reader, state).ConfigureAwait(false);
	}

	public async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		using IDataReader reader = factory();
		return await transform(reader, state).ConfigureAwait(false);
	}
}
