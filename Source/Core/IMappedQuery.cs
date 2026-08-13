namespace Open.Database.Extensions;

/// <summary>
/// Defines a query that is mapped to a model type <typeparamref name="T"/>. The query is executed when a terminal method is called, and the results are returned as an enumerable of <typeparamref name="T"/>.
/// </summary>
public interface IMappedQuery<T>
{
	/// <summary>
	/// Executes the query and returns the results as an enumerable of <typeparamref name="T"/>.
	/// </summary>
	void Read(Action<IEnumerable<T>> handler);

	/// <summary>
	/// Executes the query and returns the results as an enumerable of <typeparamref name="T"/>.
	/// </summary>
	TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler);

	/// <summary>
	/// Executes the query and returns the results as an enumerable of <typeparamref name="T"/>.
	/// </summary>
	ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken);

	/// <summary>
	/// Executes the query and returns the results as an enumerable of <typeparamref name="T"/>.
	/// </summary>
	ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken);
}
