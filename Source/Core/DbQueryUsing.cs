namespace Open.Database.Extensions;

/// <summary>
/// A <see cref="DbQuery"/> bound to where its connections come from.
/// A <see langword="struct"/>: binding allocates nothing.
/// </summary>
/// <remarks>
/// Add the values with <c>AddParam</c> or <c>AddParams</c> and then
/// <c>Map&lt;T&gt;()</c>; or call <c>Map&lt;T&gt;()</c> for a query that takes
/// no values.
/// It is a reader the same way a command is, so the <c>Map</c> and
/// <c>IterateReader</c> extensions on <see cref="IExecuteReader"/>
/// apply to it.
/// </remarks>
public readonly struct DbQueryUsing
	: IExecuteReaderAsync, IEquatable<DbQueryUsing>
{
	readonly DbQuery? _definition;
	readonly object? _source;

	internal DbQueryUsing(DbQuery definition, object source)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		_definition = definition;
		_source = source;
	}

	DbQuery Definition
		=> _definition ?? throw DbQueryRunner.Unbound();

	object SourceOrThrow
		=> _source ?? throw DbQueryRunner.Unbound();

	/// <summary>
	/// The connection, transaction, pool or factory the query is bound to.
	/// </summary>
	public object Source => SourceOrThrow;

	/// <summary>
	/// Adds the public properties and fields of <paramref name="values"/>
	/// as parameters, each under its own name.
	/// </summary>
	/// <typeparam name="T">The type of the values.</typeparam>
	/// <param name="values">The object to read the values from.</param>
	/// <returns>
	/// A new query holding the values; this one is not changed.
	/// </returns>
	/// <remarks>
	/// Declare the type with <see cref="DbQuery.Params{TParams}"/>
	/// to have it checked at compile time.
	/// </remarks>
	public DbQueryCall AddParams<T>(T values)
		where T : notnull
		=> DbQueryCall.NewRoot(Definition, SourceOrThrow, null, values);

	/// <summary>Adds an input parameter named as you write it.</summary>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">
	/// The value of the parameter. A <see langword="null"/> is sent as a
	/// database null, typed as <typeparamref name="T"/> is.
	/// </param>
	/// <returns>
	/// A new query holding the value; this one is not changed.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="name"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryCall AddParam<T>(string name, T value)
		=> DbQueryCall.NewRoot(
			Definition,
			SourceOrThrow,
			DbQueryCall.CheckedName(name),
			value);

	/// <summary>Adds an input parameter with a string value.</summary>
	/// <remarks>
	/// Compilers older than C# 13 ignore <c>OverloadResolutionPriority</c>,
	/// so without this overload they would read the name as the value of
	/// <see cref="AddParam{T}(T, string?)"/>.
	/// </remarks>
	/// <inheritdoc cref="AddParam{T}(string, T)"/>
	public DbQueryCall AddParam(string name, string? value)
		=> DbQueryCall.NewRoot(
			Definition,
			SourceOrThrow,
			DbQueryCall.CheckedName(name),
			value);

	/// <summary>
	/// Adds an input parameter named for the argument:
	/// <c>AddParam(authorId)</c> adds <c>authorId</c>,
	/// <c>AddParam(filter.AuthorId)</c> adds <c>AuthorId</c>.
	/// </summary>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <param name="value">The value of the parameter.</param>
	/// <param name="expression">
	/// The text of the argument, supplied by the compiler. Do not pass it.
	/// </param>
	/// <returns>
	/// A new query holding the value; this one is not changed.
	/// </returns>
	/// <exception cref="ArgumentException">
	/// A name cannot be derived from the argument (it is not a variable or a
	/// chain of members).
	/// </exception>
	[OverloadResolutionPriority(-1)]
	public DbQueryCall AddParam<T>(
		T value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> DbQueryCall.NewRoot(
			Definition,
			SourceOrThrow,
			DbQueryCall.Derive(expression),
			value);

	/// <inheritdoc />
	public bool Equals(DbQueryUsing other)
		=> Equals(_definition, other._definition)
		&& Equals(_source, other._source);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> obj is DbQueryUsing other && Equals(other);

	/// <summary>Whether two bound queries are the same definition on the same source.</summary>
	/// <param name="left">The first.</param>
	/// <param name="right">The second.</param>
	/// <returns><see langword="true"/> when they are equal.</returns>
	public static bool operator ==(DbQueryUsing left, DbQueryUsing right)
		=> left.Equals(right);

	/// <summary>Whether two bound queries differ.</summary>
	/// <param name="left">The first.</param>
	/// <param name="right">The second.</param>
	/// <returns><see langword="true"/> when they are not equal.</returns>
	public static bool operator !=(DbQueryUsing left, DbQueryUsing right)
		=> !left.Equals(right);

	/// <inheritdoc />
	public override int GetHashCode()
	{
		unchecked
		{
			return ((_definition?.GetHashCode() ?? 0) * 31)
				+ (_source?.GetHashCode() ?? 0);
		}
	}

	CancellationToken IExecuteReader.CancellationToken => default;

	bool IExecuteReaderAsync.UseAsyncRead => false;

	TResult IExecuteReader.ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceOrThrow,
			null,
			state,
			transform,
			behavior);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			null,
			state,
			transform,
			behavior,
			cancellationToken);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			null,
			state,
			transform,
			behavior,
			default);

	void IExecuteReader.ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceOrThrow,
			null,
			handler,
			behavior);

	T IExecuteReader.ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceOrThrow,
			null,
			transform,
			behavior);

	ValueTask IExecuteReader.ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			null,
			handler,
			behavior);

	ValueTask<T> IExecuteReader.ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			null,
			transform,
			behavior);
}

/// <summary>
/// A <see cref="DbQuery{TParams}"/> bound to where its connections come
/// from. The values are given in one call, checked at compile time.
/// </summary>
/// <typeparam name="TParams">The type of the values.</typeparam>
public readonly struct DbQueryUsing<TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQuery? _definition;
	readonly object? _source;

	internal DbQueryUsing(DbQuery definition, object source)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

		_definition = definition;
		_source = source;
	}

	/// <summary>
	/// Adds the public properties and fields of <paramref name="values"/>
	/// as parameters, each under its own name.
	/// </summary>
	/// <param name="values">The values.</param>
	/// <returns>
	/// A new query holding the values; this one is not changed.
	/// </returns>
	public DbQueryCall AddParams(TParams values)
		=> DbQueryCall.NewRoot(
			_definition ?? throw DbQueryRunner.Unbound(),
			_source ?? throw DbQueryRunner.Unbound(),
			null,
			values);
}
