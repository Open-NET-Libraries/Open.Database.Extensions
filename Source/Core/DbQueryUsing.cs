using Open.Database.Extensions.Synchronous;

using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// Represents a <see cref="DbQuery"/> bound to where its connections come
/// from.
/// </summary>
/// <remarks>
/// Add the values with <c>AddParam</c> or <c>AddParams</c> and then
/// <c>Map&lt;T&gt;()</c>; or call <c>Map&lt;T&gt;()</c> for a query that takes
/// no values.
/// It is a reader the same way a command is, so the <c>Map</c>
/// extensions on <see cref="IExecuteReader"/> apply to it.
/// </remarks>
// A struct: binding allocates nothing.
public readonly struct DbQueryUsing
	: IExecuteReaderAsync, IExecuteReaderSync, IEquatable<DbQueryUsing>
{
	readonly DbQuery? _definition;
	readonly object? _source;

	internal DbQueryUsing(DbQuery definition, object source)
	{
		// The Using(...) that makes this has checked the source.
		Debug.Assert(source is not null);

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
	/// <value>The connection, transaction, pool or factory the query was bound to with <c>Using</c>.</value>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public object Source => SourceOrThrow;

	/// <summary>
	/// Maps each row to a <typeparamref name="T"/> by column name (a single value such as
	/// <see cref="int"/> is read from the first column), and returns the mapped query.
	/// </summary>
	/// <remarks>
	/// The same as <see cref="CoreExtensions.Map{T}(IExecuteReader)"/>.
	/// </remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	// The query holds this value as it is; the value is not boxed.
	public MappedQuery<T> Map<T>()
		=> FirstColumn<T>.Instance is { } firstColumn
		? new(Views.QueryCore(ToCall(), firstColumn))
		: new(Views.QueryCore(ToCall(), RowMap<T>.Default));

	/// <summary>Maps each row to a <typeparamref name="T"/> with a map the caller declared, and returns the mapped query.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="map">The map. A <see langword="null"/> map reads as <see cref="Map{T}()"/> does.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	// A bare null does not compile here (it also fits the selector overload):
	// write (RowMap<T>?)null.
	public MappedQuery<T> Map<T>(RowMap<T>? map)
		=> map is null
		? Map<T>()
		: new(Views.QueryCore(ToCall(), map));

	/// <summary>Maps each row with a selector, and returns the mapped query.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="selector">Called for each row, from any thread and by overlapping calls, so it must be safe for concurrent use.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<T> Map<T>(Func<IDataRecord, T> selector)
	{
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new(Views.QueryCore(ToCall(), new SelectorSource<T>(selector)));
	}

	/// <summary>Reads each row without a model, as an array of its values.</summary>
	/// <remarks>The same as <see cref="CoreExtensions.AsObjectArrays(IExecuteReader)"/>.</remarks>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<object?[]> AsObjectArrays()
		=> new(Views.QueryCore(ToCall(), ObjectArraySource.Instance));

	/// <summary>Reads each row without a model, as a <see cref="DictionaryRow"/>.</summary>
	/// <remarks>The same as <see cref="CoreExtensions.AsDictionaryRows(IExecuteReader)"/>.</remarks>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<DictionaryRow> AsDictionaryRows()
		=> new(Views.QueryCore(ToCall(), DictionaryRowSource.Instance));

	/// <summary>Gets the query as a call with no values.</summary>
	internal DbQueryCall ToCall()
		=> new(Definition, SourceOrThrow, default);

	/// <summary>Gets the query as a call holding the values.</summary>
	internal DbQueryCall WithParams(ParamList values)
		=> new(Definition, SourceOrThrow, values);

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
	/// For the lightest call, use a typed <see cref="DbQuery.Params{TParams}"/> definition with a <see langword="readonly"/> <see langword="record"/> <see langword="struct"/>.
	/// </remarks>
	public DbQueryCall AddParams<T>(T values)
		where T : notnull
		=> ToCall().AddParams(values);

	/// <summary>Adds an input parameter named as you write it.</summary>
	/// <remarks>
	/// For the lightest call, declare the values with <see cref="DbQuery.Params{TParams}"/> and a <see langword="readonly"/> <see langword="record"/> <see langword="struct"/>.
	/// </remarks>
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
	/// <exception cref="ArgumentException">
	/// <paramref name="name"/> is empty or white space.
	/// </exception>
	public DbQueryCall AddParam<T>(string name, T value)
		=> ToCall()
			.AddParam(name, value);

	/// <summary>Adds an input parameter with a string value.</summary>
	/// <inheritdoc cref="AddParam{T}(string, T)"/>
	// Compilers older than C# 13 ignore OverloadResolutionPriority, so without
	// this overload they would read the name as the value of AddParam<T>(T,
	// string?).
	public DbQueryCall AddParam(string name, string? value)
		=> ToCall()
			.AddParam(name, value);

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
		=> ToCall()
			.AddParam(value, expression);

	/// <inheritdoc />
	public bool Equals(DbQueryUsing other)
		=> DbQueryCall.SameTarget(
			_definition,
			_source,
			other._definition,
			other._source);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> obj is DbQueryUsing other && Equals(other);

	/// <summary>Determines whether two bound queries are the same definition on the same source.</summary>
	/// <param name="left">The first bound query.</param>
	/// <param name="right">The second bound query.</param>
	/// <returns><see langword="true"/> if they are equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator ==(DbQueryUsing left, DbQueryUsing right)
		=> left.Equals(right);

	/// <summary>Determines whether two bound queries differ.</summary>
	/// <param name="left">The first bound query.</param>
	/// <param name="right">The second bound query.</param>
	/// <returns><see langword="true"/> if they are not equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator !=(DbQueryUsing left, DbQueryUsing right)
		=> !left.Equals(right);

	/// <inheritdoc />
	public override int GetHashCode()
		=> DbQueryCall.TargetHash(_definition, _source);

	CancellationToken IExecuteReader.CancellationToken => default;

	bool IExecuteReaderAsync.UseAsyncRead => false;

	TResult IExecuteReaderSync.ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
		=> ToCall().Run(state, transform, behavior);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> ToCall().RunAsync(state, transform, behavior, cancellationToken);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior)
		=> ToCall().RunAsync(state, transform, behavior, default);

	void IExecuteReaderSync.ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceOrThrow,
			default,
			handler,
			behavior);

	T IExecuteReaderSync.ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceOrThrow,
			default,
			transform,
			behavior);

	ValueTask IExecuteReader.ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			default,
			handler,
			behavior);

	ValueTask<T> IExecuteReader.ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceOrThrow,
			default,
			transform,
			behavior);
}

/// <summary>
/// Represents a <see cref="DbQuery{TParams}"/> bound to where its
/// connections come from.
/// The values are given in one call, checked at compile time.
/// </summary>
/// <typeparam name="TParams">The type of the values.</typeparam>
public readonly struct DbQueryUsing<TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQuery? _definition;
	readonly object? _source;

	internal DbQueryUsing(DbQuery definition, object source)
	{
		// The Using(...) that makes this has checked the source.
		Debug.Assert(source is not null);

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
		=> new DbQueryCall(
			_definition ?? throw DbQueryRunner.Unbound(),
			_source ?? throw DbQueryRunner.Unbound(),
			default).AddParams(values);
}
