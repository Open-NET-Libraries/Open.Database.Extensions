using Open.Database.Extensions.Synchronous;

namespace Open.Database.Extensions;

/// <summary>
/// Represents a <see cref="DbQuery"/> bound to a source with some values
/// added.
/// A <see langword="readonly"/> <see langword="struct"/>: each
/// <c>AddParam</c> returns a new one and leaves this one unchanged, so a call
/// can be kept and branched.
/// </summary>
/// <remarks>
/// <para>
/// The values of a call and of every call made from it by adding values are
/// kept in one array sized from the definition, so a chain of <c>AddParam</c>
/// allocates one array, not one object per value.
/// A call that is kept and added to twice does not interfere with itself: the
/// second addition works on a copy.
/// A value that is an object, and the object of <c>AddParams</c>, is read when the query runs.
/// </para>
/// <para>
/// Map it with <c>Map&lt;T&gt;()</c>, then run a terminal. Nothing touches the database before that.
/// </para>
/// </remarks>
public readonly struct DbQueryCall
	: IExecuteReaderAsync, IExecuteReaderSync, IEquatable<DbQueryCall>
{
	readonly DbQuery? _definition;
	readonly object? _source;

	internal DbQueryCall(DbQuery definition, object source, ParamList parameters)
	{
		_definition = definition;
		_source = source;
		Params = parameters;
	}

	internal bool IsBound => _definition is not null;

	internal DbQuery Definition
		=> _definition ?? throw DbQueryRunner.Unbound();

	internal object SourceCore
		=> _source ?? throw DbQueryRunner.Unbound();

	internal ParamList Params { get; }

	/// <summary>
	/// The connection, transaction, pool or factory the query is bound to.
	/// </summary>
	/// <value>The connection, transaction, pool or factory the query was bound to with <c>Using</c>.</value>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public object Source => SourceCore;

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public DbQueryCall AddParams<T>(T values)
		where T : notnull
		=> new(
			Definition,
			SourceCore,
			Params.AddMembers(Definition, values));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public DbQueryCall AddParam<T>(string name, T value)
		=> new(
			Definition,
			SourceCore,
			Params.AddNamed(Definition, CheckedName(name), value));

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public DbQueryCall AddParam(string name, string? value)
		=> new(
			Definition,
			SourceCore,
			Params.AddNamed(Definition, CheckedName(name), value));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public DbQueryCall AddParam<T>(
		T value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new(
			Definition,
			SourceCore,
			Params.AddDerived(Definition, CheckedExpression(expression), value));


	/// <summary>
	/// Maps each row to a <typeparamref name="T"/> by column name (a single value such as
	/// <see cref="int"/> is read from the first column), and returns the mapped query.
	/// </summary>
	/// <remarks>
	/// The same as <see cref="CoreExtensions.Map{T}(IExecuteReader)"/>, written the same way, without boxing this value:
	/// the query holds the call as it is.
	/// </remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<T> Map<T>()
		=> FirstColumn<T>.Instance is { } firstColumn
		? new(Views.QueryCore(Bound(), firstColumn))
		: new(Views.QueryCore(Bound(), RowMap<T>.Default));

	/// <summary>Maps each row to a <typeparamref name="T"/> with a map the caller declared, and returns the mapped query.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="map">The map. A <see langword="null"/> map reads exactly as <see cref="Map{T}()"/> does (write it typed, <c>(RowMap&lt;T&gt;?)null</c>: a bare <see langword="null"/> also fits the selector overload).</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<T> Map<T>(RowMap<T>? map)
		=> map is null
		? Map<T>()
		: new(Views.QueryCore(Bound(), map));

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

		return new(Views.QueryCore(Bound(), new SelectorSource<T>(selector)));
	}

	/// <summary>Reads each row without a model, as an array of its values.</summary>
	/// <remarks>The same as <see cref="CoreExtensions.AsObjectArrays(IExecuteReader)"/>.</remarks>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<object?[]> AsObjectArrays()
		=> new(Views.QueryCore(Bound(), ObjectArraySource.Instance));

	/// <summary>Reads each row without a model, as a <see cref="DictionaryRow"/>.</summary>
	/// <remarks>The same as <see cref="CoreExtensions.AsDictionaryRows(IExecuteReader)"/>.</remarks>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is a <see langword="default"/> value, which is bound to nothing.</exception>
	public MappedQuery<DictionaryRow> AsDictionaryRows()
		=> new(Views.QueryCore(Bound(), DictionaryRowSource.Instance));

	DbQueryCall Bound()
	{
		_ = Definition;
		return this;
	}

	internal static string CheckedName(string name)
	{
		if (name is null) throw new ArgumentNullException(nameof(name));
		if (string.IsNullOrWhiteSpace(name))
		{
			throw new ArgumentException(
				"Parameter names cannot be empty or white space.",
				nameof(name));
		}

		return name;
	}

	/// <summary>
	/// Checks the text of the argument of <c>AddParam(value)</c>, which the
	/// compiler supplies.
	/// </summary>
	/// <remarks>
	/// The facts of an expression are worked out once per text.
	/// The name it stands for is settled when the query runs.
	/// </remarks>
	/// <param name="expression">The text of the argument.</param>
	/// <returns>The facts of the expression.</returns>
	/// <exception cref="ArgumentException">A name cannot be derived from the expression.</exception>
	[SuppressMessage(
		"Usage",
		"CA2208:Instantiate argument exceptions correctly")]
	internal static ParameterNames.DerivedExpression CheckedExpression(
		string? expression)
		=> ParameterNames.Describe(expression)
			?? throw (expression is null
				? new ArgumentException(
					"The compiler did not supply the argument's text (deriving a name needs C# 10 or newer). Pass the name explicitly: AddParam(\"name\", value).",
					"value")
				: new ArgumentException(
					$"Cannot derive a parameter name from the argument '{expression}'. Pass a variable or a chain of members such as filter.AuthorId, or name the parameter: AddParam(\"name\", value).",
					"value"));

	/// <inheritdoc />
	public bool Equals(DbQueryCall other)
		=> Equals(_definition, other._definition)
		&& Equals(_source, other._source)
		&& Params.SameEntries(other.Params);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> obj is DbQueryCall other && Equals(other);

	/// <summary>Determines whether two calls are the same definition on the same source with the same values added.</summary>
	/// <param name="left">The first call.</param>
	/// <param name="right">The second call.</param>
	/// <returns><see langword="true"/> if they are equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator ==(DbQueryCall left, DbQueryCall right)
		=> left.Equals(right);

	/// <summary>Determines whether two calls differ.</summary>
	/// <param name="left">The first call.</param>
	/// <param name="right">The second call.</param>
	/// <returns><see langword="true"/> if they are not equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator !=(DbQueryCall left, DbQueryCall right)
		=> !left.Equals(right);

	/// <inheritdoc />
	public override int GetHashCode()
	{
		unchecked
		{
			int h = _definition?.GetHashCode() ?? 0;
			h = (h * 31) + (_source?.GetHashCode() ?? 0);
			return (h * 31) + Params.EntriesHash();
		}
	}

	CancellationToken IExecuteReader.CancellationToken => default;

	bool IExecuteReaderAsync.UseAsyncRead => false;

	TResult IExecuteReaderSync.ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
		=> Run(state, transform, behavior);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> RunAsync(state, transform, behavior, cancellationToken);

	ValueTask<TResult> IExecuteReader.ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior)
		=> RunAsync(state, transform, behavior, default);

	void IExecuteReaderSync.ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			Params,
			handler,
			behavior);

	T IExecuteReaderSync.ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			Params,
			transform,
			behavior);

	ValueTask IExecuteReader.ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			Params,
			handler,
			behavior);

	ValueTask<T> IExecuteReader.ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			Params,
			transform,
			behavior);

	/// <summary>
	/// Runs the query without the interface, so a query that holds this value
	/// does not box it.
	/// </summary>
	internal TResult Run<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
		=> DbQueryRunner.Run(
			Definition,
			SourceCore,
			Params,
			state,
			transform,
			behavior);

	/// <inheritdoc cref="Run{TState, TResult}(TState, Func{IDataReader, TState, TResult}, CommandBehavior)"/>
	internal ValueTask<TResult> RunAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> DbQueryRunner.RunAsync(
			Definition,
			SourceCore,
			Params,
			state,
			transform,
			behavior,
			cancellationToken);
}
