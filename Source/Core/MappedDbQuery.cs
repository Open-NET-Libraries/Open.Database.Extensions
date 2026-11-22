using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// A <see cref="DbQuery"/> that is already mapped: the command text, the kind, the timeout, the
/// map that turns a row into a <typeparamref name="T"/>, and the options that never change from one
/// call to the next. It has no connection yet and cannot be read until it has one.
/// </summary>
/// <remarks>
/// <para>
/// It is immutable, so a static field holds it:
/// <c>static readonly MappedDbQuery&lt;Post&gt; ByAuthor = DbQuery.Procedure("ByAuthor").Map&lt;Post&gt;();</c>.
/// The map (and the compiled reader it caches) lives with the definition, so there is no separate
/// <see cref="RowMap{T}"/> field to declare.
/// </para>
/// <para>
/// From here the order is the caller's.
/// <c>ByAuthor.Using(source).AddParam("authorId", id).ToListAsync(ct)</c> is the first way to write it;
/// <c>ByAuthor.AddParam("authorId", id).Using(source).ToListAsync(ct)</c> gives the same command.
/// <see cref="Using(DbConnection)"/> returns the ordinary <see cref="MappedQuery{T}"/> view, with the
/// options of this definition applied, so every terminal, <c>Skip</c> and <c>Take</c> are the ones the view has.
/// </para>
/// <para>
/// Fixed by the definition: the map, <see cref="SingleResult"/>, <see cref="AsyncReads"/>,
/// <see cref="UsePool(ArrayPool{T}, bool)"/> and (on the <see cref="DbQuery"/> before <c>Map</c>)
/// <see cref="DbQuery.SetTimeout(ushort)"/>. Chosen per call: the parameters, <c>Skip</c> and <c>Take</c>.
/// </para>
/// </remarks>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public sealed class MappedDbQuery<T>
{
	readonly DbQuery _definition;
	readonly IRowSource<T> _source;
	readonly bool _singleResult;
	readonly bool _asyncReads;
	readonly ArrayPool<T>? _pool;
	readonly bool _clearOnReturn;

	internal MappedDbQuery(DbQuery definition, IRowSource<T> source)
		: this(definition, source, false, false, null, false)
	{
	}

	MappedDbQuery(
		DbQuery definition,
		IRowSource<T> source,
		bool singleResult,
		bool asyncReads,
		ArrayPool<T>? pool,
		bool clearOnReturn)
	{
		Debug.Assert(definition is not null);
		Debug.Assert(source is not null);

		_definition = definition;
		_source = source;
		_singleResult = singleResult;
		_asyncReads = asyncReads;
		_pool = pool;
		_clearOnReturn = clearOnReturn;
	}

	/// <summary>Gets the command definition under this mapped one.</summary>
	internal DbQuery Query => _definition;

	/// <summary>
	/// Reads only the first result set, in every query made from this definition
	/// (see <see cref="MappedQuery{T}.SingleResult"/>).
	/// </summary>
	/// <returns>A definition that reads only the first result set.</returns>

	public MappedDbQuery<T> SingleResult()
		=> new(_definition, _source, true, _asyncReads, _pool, _clearOnReturn);

	/// <summary>
	/// Reads the rows with the asynchronous read of the provider, in every query made from this definition
	/// (see <see cref="MappedQuery{T}.AsyncReads"/>).
	/// </summary>
	/// <returns>A definition that reads asynchronously.</returns>
	public MappedDbQuery<T> AsyncReads()
		=> new(_definition, _source, _singleResult, true, _pool, _clearOnReturn);

	/// <summary>
	/// Takes the arrays that hold the rows from <paramref name="pool"/>, in every query made from this definition
	/// (see <see cref="MappedQuery{T}.UsePool(ArrayPool{T}, bool)"/>).
	/// </summary>
	/// <param name="pool">The pool the arrays come from and go back to.</param>
	/// <param name="clearOnReturn">Whether the rows are cleared when the arrays go back.</param>
	/// <returns>A definition that uses the pool.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="pool"/> is <see langword="null"/>.</exception>
	public MappedDbQuery<T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
	{
		if (pool is null) throw new ArgumentNullException(nameof(pool));
		Contract.EndContractBlock();

		return new(_definition, _source, _singleResult, _asyncReads, pool, clearOnReturn);
	}

	/// <summary>Reads the query on a connection the caller owns.</summary>
	/// <param name="connection">The connection. It is opened if it is not open, and closed again if the query opened it.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	public MappedQuery<T> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> View(_definition.Using(connection));

	/// <summary>Reads the query inside a transaction the caller owns.</summary>
	/// <param name="transaction">The transaction. Its connection is used, and left open.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> is <see langword="null"/>.</exception>
	public MappedQuery<T> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> View(_definition.Using(transaction));

	/// <summary>Reads the query on a connection taken from a pool.</summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="pool">The pool. A connection is taken for the call and given back when it ends.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="pool"/> is <see langword="null"/>.</exception>
	public MappedQuery<T> Using<TConnection>(IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> View(_definition.Using(pool));

	/// <summary>Reads the query on a connection made by a factory.</summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="factory">The factory. A connection is made for the call and disposed when it ends.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	public MappedQuery<T> Using<TConnection>(IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> View(_definition.Using(factory));

	/// <summary>Adds a named parameter before the source is known.</summary>
	/// <typeparam name="TValue">The type of the value.</typeparam>
	/// <param name="name">The name of the parameter, with or without its prefix.</param>
	/// <param name="value">The value.</param>
	/// <returns>The parameters so far, to be given a source with <c>Using</c>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
	public UnboundCall<T> AddParam<TValue>(string name, TValue value)
		=> new(this, new ParamStep<TValue>(null, DbQueryCall.CheckedName(name), value, ParamKind.Named));

	/// <inheritdoc cref="AddParam{TValue}(string, TValue)"/>
	public UnboundCall<T> AddParam(string name, string? value)
		=> new(this, new ParamStep<string?>(null, DbQueryCall.CheckedName(name), value, ParamKind.Named));

	/// <summary>Adds a parameter whose name is derived from the argument's expression, before the source is known.</summary>
	/// <typeparam name="TValue">The type of the value.</typeparam>
	/// <param name="value">The value.</param>
	/// <param name="expression">The text of the argument, filled in by the compiler.</param>
	/// <returns>The parameters so far, to be given a source with <c>Using</c>.</returns>
	[OverloadResolutionPriority(-1)]
	public UnboundCall<T> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new(this, new ParamStep<TValue>(null, DbQueryCall.CheckedExpression(expression), value, ParamKind.Derived));

	/// <summary>Adds the members of an object as parameters, before the source is known.</summary>
	/// <typeparam name="TValues">The type of the object.</typeparam>
	/// <param name="values">The object whose members are the parameters.</param>
	/// <returns>The parameters so far, to be given a source with <c>Using</c>.</returns>
	public UnboundCall<T> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> new(this, new ParamStep<TValues>(null, null, values, ParamKind.Members));

	/// <summary>Makes the view over a command made from this definition, with the options of this definition.</summary>
	internal MappedQuery<T> View(IExecuteReader command)
	{
		MappedQuery<T> view = new(Views.QueryCore(command, _source));
		if (_singleResult) view = view.SingleResult();
		if (_asyncReads) view = view.AsyncReads();
		if (_pool is not null) view = view.UsePool(_pool, _clearOnReturn);
		return view;
	}
}

/// <summary>
/// A <see cref="MappedDbQuery{T}"/> whose parameters are one typed struct, so the compiler checks them.
/// </summary>
/// <typeparam name="TParams">The struct that holds the parameters.</typeparam>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public sealed class MappedDbQuery<TParams, T>
	where TParams : struct, IEquatable<TParams>
{
	readonly MappedDbQuery<T> _definition;

	internal MappedDbQuery(MappedDbQuery<T> definition)
	{
		Debug.Assert(definition is not null);
		_definition = definition;
	}

	/// <inheritdoc cref="MappedDbQuery{T}.SingleResult"/>
	public MappedDbQuery<TParams, T> SingleResult()
		=> new(_definition.SingleResult());

	/// <inheritdoc cref="MappedDbQuery{T}.AsyncReads"/>
	public MappedDbQuery<TParams, T> AsyncReads()
		=> new(_definition.AsyncReads());

	/// <inheritdoc cref="MappedDbQuery{T}.UsePool(ArrayPool{T}, bool)"/>
	public MappedDbQuery<TParams, T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
		// The definition throws ArgumentNullException for a null pool.
		=> new(_definition.UsePool(pool, clearOnReturn));

	/// <summary>Adds the parameters before the source is known.</summary>
	/// <param name="values">The parameters.</param>
	/// <returns>The parameters, to be given a source with <c>Using</c>.</returns>
	public UnboundCall<TParams, T> AddParams(TParams values)
		=> new(_definition, values);

	/// <inheritdoc cref="MappedDbQuery{T}.Using(DbConnection)"/>
	public MappedDbQueryUsing<TParams, T> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition, _definition.Query.Using(connection));

	/// <inheritdoc cref="MappedDbQuery{T}.Using(DbTransaction)"/>
	public MappedDbQueryUsing<TParams, T> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition, _definition.Query.Using(transaction));

	/// <inheritdoc cref="MappedDbQuery{T}.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public MappedDbQueryUsing<TParams, T> Using<TConnection>(IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition, _definition.Query.Using(pool));

	/// <inheritdoc cref="MappedDbQuery{T}.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public MappedDbQueryUsing<TParams, T> Using<TConnection>(IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition, _definition.Query.Using(factory));
}

/// <summary>
/// A typed mapped query that has a source and waits for its parameters.
/// </summary>
/// <typeparam name="TParams">The struct that holds the parameters.</typeparam>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public readonly struct MappedDbQueryUsing<TParams, T>
	where TParams : struct, IEquatable<TParams>
{
	readonly MappedDbQuery<T>? _definition;
	readonly DbQueryUsing _using;

	internal MappedDbQueryUsing(MappedDbQuery<T> definition, DbQueryUsing source)
	{
		Debug.Assert(definition is not null);
		_definition = definition;
		_using = source;
	}

	/// <summary>Adds the parameters.</summary>
	/// <param name="values">The parameters.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="InvalidOperationException">This is <see langword="default"/>.</exception>
	public MappedQuery<T> AddParams(TParams values)
		=> (_definition ?? throw DbQueryRunner.Unbound()).View(_using.AddParams(values));
}

/// <summary>
/// The parameters of a <see cref="MappedDbQuery{T}"/>, added before it has a source.
/// It has no terminals: it cannot be read until <c>Using</c> gives it a source.
/// </summary>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public readonly struct UnboundCall<T>
{
	readonly MappedDbQuery<T>? _definition;
	readonly ParamStep? _last;

	internal UnboundCall(MappedDbQuery<T> definition, ParamStep last)
	{
		Debug.Assert(definition is not null);
		Debug.Assert(last is not null);

		_definition = definition;
		_last = last;
	}

	MappedDbQuery<T> Definition
		=> _definition ?? throw DbQueryRunner.Unbound();

	/// <inheritdoc cref="MappedDbQuery{T}.AddParam{TValue}(string, TValue)"/>
	public UnboundCall<T> AddParam<TValue>(string name, TValue value)
		=> new(Definition, new ParamStep<TValue>(_last, DbQueryCall.CheckedName(name), value, ParamKind.Named));

	/// <inheritdoc cref="MappedDbQuery{T}.AddParam{TValue}(string, TValue)"/>
	public UnboundCall<T> AddParam(string name, string? value)
		=> new(Definition, new ParamStep<string?>(_last, DbQueryCall.CheckedName(name), value, ParamKind.Named));

	/// <inheritdoc cref="MappedDbQuery{T}.AddParam{TValue}(TValue, string?)"/>
	[OverloadResolutionPriority(-1)]
	public UnboundCall<T> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new(Definition, new ParamStep<TValue>(_last, DbQueryCall.CheckedExpression(expression), value, ParamKind.Derived));

	/// <inheritdoc cref="MappedDbQuery{T}.AddParams{TValues}(TValues)"/>
	public UnboundCall<T> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> new(Definition, new ParamStep<TValues>(_last, null, values, ParamKind.Members));

	/// <summary>Gives the parameters a connection the caller owns.</summary>
	/// <param name="connection">The connection.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">This is <see langword="default"/>.</exception>
	public MappedQuery<T> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Query.Using(connection));

	/// <summary>Gives the parameters a transaction the caller owns.</summary>
	/// <param name="transaction">The transaction.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transaction"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">This is <see langword="default"/>.</exception>
	public MappedQuery<T> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Query.Using(transaction));

	/// <summary>Gives the parameters a connection taken from a pool.</summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="pool">The pool.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="pool"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">This is <see langword="default"/>.</exception>
	public MappedQuery<T> Using<TConnection>(IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Query.Using(pool));

	/// <summary>Gives the parameters a connection made by a factory.</summary>
	/// <typeparam name="TConnection">The connection type.</typeparam>
	/// <param name="factory">The factory.</param>
	/// <returns>The mapped query, to be read with a terminal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">This is <see langword="default"/>.</exception>
	public MappedQuery<T> Using<TConnection>(IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Query.Using(factory));

	MappedQuery<T> Bind(DbQueryUsing root)
	{
		// Definition throws for a default value before this is reached.
		Debug.Assert(_last is not null);

		return Definition.View(ParamStep.Replay(root, _last));
	}
}

/// <summary>
/// The typed parameters of a <see cref="MappedDbQuery{TParams, T}"/>, added before it has a source.
/// It has no terminals: it cannot be read until <c>Using</c> gives it a source.
/// </summary>
/// <typeparam name="TParams">The struct that holds the parameters.</typeparam>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
public readonly struct UnboundCall<TParams, T>
	where TParams : struct, IEquatable<TParams>
{
	readonly MappedDbQuery<T>? _definition;
	readonly TParams _values;

	internal UnboundCall(MappedDbQuery<T> definition, TParams values)
	{
		Debug.Assert(definition is not null);

		_definition = definition;
		_values = values;
	}

	MappedDbQuery<T> Definition
		=> _definition ?? throw DbQueryRunner.Unbound();

	/// <inheritdoc cref="UnboundCall{T}.Using(DbConnection)"/>
	public MappedQuery<T> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Definition.View(Definition.Query.Using(connection).AddParams(_values));

	/// <inheritdoc cref="UnboundCall{T}.Using(DbTransaction)"/>
	public MappedQuery<T> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Definition.View(Definition.Query.Using(transaction).AddParams(_values));

	/// <inheritdoc cref="UnboundCall{T}.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public MappedQuery<T> Using<TConnection>(IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Definition.View(Definition.Query.Using(pool).AddParams(_values));

	/// <inheritdoc cref="UnboundCall{T}.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public MappedQuery<T> Using<TConnection>(IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Definition.View(Definition.Query.Using(factory).AddParams(_values));
}

/// <summary>What a parameter step adds.</summary>
enum ParamKind
{
	/// <summary>A value under a given name.</summary>
	Named,

	/// <summary>A value whose name is derived from the expression that was passed.</summary>
	Derived,

	/// <summary>The members of an object.</summary>
	Members,
}

/// <summary>
/// One parameter added before the source is known: kept in order as a list (newest first),
/// and replayed on the source's <see cref="DbQueryUsing"/> when <c>Using</c> gives it one.
/// </summary>
/// <remarks>
/// Internal, so a later unit of work can hold the same unbound call and bind it to a
/// <see cref="DbTransaction"/>, one of the sources <c>Using</c> takes, without a new public member.
/// </remarks>
internal abstract class ParamStep(ParamStep? previous)
{
	/// <summary>Gets the step added before this one.</summary>
	public ParamStep? Previous { get; } = previous;

	/// <summary>Adds this step's parameter to the first call of a source.</summary>
	public abstract DbQueryCall Apply(DbQueryUsing root);

	/// <summary>Adds this step's parameter to a call.</summary>
	public abstract DbQueryCall Apply(DbQueryCall call);

	/// <summary>Replays every step, oldest first, on the source's root.</summary>
	/// <param name="root">The source with the definition's command.</param>
	/// <param name="last">The newest step.</param>
	/// <returns>The call that holds every parameter.</returns>
	public static DbQueryCall Replay(DbQueryUsing root, ParamStep last)
	{
		var steps = new Stack<ParamStep>();
		for (ParamStep? step = last; step is not null; step = step.Previous)
			steps.Push(step);

		DbQueryCall? call = null;
		foreach (ParamStep step in steps)
			call = call is null ? step.Apply(root) : step.Apply(call);

		Debug.Assert(call is not null);
		return call;
	}
}

/// <summary>A parameter step for one value type.</summary>
internal sealed class ParamStep<TValue>(ParamStep? previous, string? name, TValue value, ParamKind kind)
	: ParamStep(previous)
{
	// A Members step is only made by AddParams<TValues>, which requires notnull.
#pragma warning disable CS8714
	public override DbQueryCall Apply(DbQueryUsing root)
		=> kind switch
		{
			ParamKind.Named when typeof(TValue) == typeof(string)
				=> root.AddParam(name!, (string?)(object?)value),
			ParamKind.Named => root.AddParam(name!, value),
			ParamKind.Derived => root.AddParam(value, name),
			_ => root.AddParams(value),
		};

	public override DbQueryCall Apply(DbQueryCall call)
		=> kind switch
		{
			ParamKind.Named when typeof(TValue) == typeof(string)
				=> call.AddParam(name!, (string?)(object?)value),
			ParamKind.Named => call.AddParam(name!, value),
			ParamKind.Derived => call.AddParam(value, name),
			_ => call.AddParams(value),
		};
#pragma warning restore CS8714
}

/// <summary>
/// Adds parameters to a <see cref="MappedQuery{T}"/> that <c>Using</c> made from a <see cref="MappedDbQuery{T}"/>,
/// so <c>ByAuthor.Using(source).AddParam("authorId", id)</c> reads the same as the other order.
/// </summary>
/// <remarks>
/// Each call returns a new query over the command with the parameter added and keeps every option of the
/// query (the map, <c>SingleResult</c>, <c>AsyncReads</c>, the pool). It applies only to a query that came from
/// a <see cref="DbQuery"/> definition; a query over an expressive command takes its parameters on the command,
/// before <c>Map</c>, and these throw <see cref="InvalidOperationException"/> for it.
/// </remarks>
public static class MappedDbQueryExtensions
{
	const string NotFromADefinition
		= "AddParam applies to a query made from a DbQuery definition; add the parameter to the command before Map.";

	/// <summary>Adds a named parameter to the query.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TValue">The type of the value.</typeparam>
	/// <param name="query">The query, made by <c>Using</c> on a <see cref="MappedDbQuery{T}"/>.</param>
	/// <param name="name">The name of the parameter, with or without its prefix.</param>
	/// <param name="value">The value.</param>
	/// <returns>A new query with the parameter; <paramref name="query"/> is not changed.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException"><paramref name="query"/> was not made from a <see cref="DbQuery"/> definition.</exception>
	public static MappedQuery<T> AddParam<T, TValue>(this MappedQuery<T> query, string name, TValue value)
		=> query.WithCommand(query.Command switch
		{
			DbQueryUsing source => source.AddParam(name, value),
			DbQueryCall call => call.AddParam(name, value),
			_ => throw new InvalidOperationException(NotFromADefinition),
		});

	/// <inheritdoc cref="AddParam{T, TValue}(MappedQuery{T}, string, TValue)"/>
	public static MappedQuery<T> AddParam<T>(this MappedQuery<T> query, string name, string? value)
		=> query.WithCommand(query.Command switch
		{
			DbQueryUsing source => source.AddParam(name, value),
			DbQueryCall call => call.AddParam(name, value),
			_ => throw new InvalidOperationException(NotFromADefinition),
		});

	/// <summary>Adds a parameter whose name is derived from the argument's expression.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TValue">The type of the value.</typeparam>
	/// <param name="query">The query, made by <c>Using</c> on a <see cref="MappedDbQuery{T}"/>.</param>
	/// <param name="value">The value.</param>
	/// <param name="expression">The text of the argument, supplied by the compiler. Do not pass it.</param>
	/// <returns>A new query with the parameter; <paramref name="query"/> is not changed.</returns>
	/// <exception cref="ArgumentException">A name cannot be derived from the argument.</exception>
	/// <exception cref="InvalidOperationException"><paramref name="query"/> was not made from a <see cref="DbQuery"/> definition.</exception>
	[OverloadResolutionPriority(-1)]
	public static MappedQuery<T> AddParam<T, TValue>(
		this MappedQuery<T> query,
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> query.WithCommand(query.Command switch
		{
			DbQueryUsing source => source.AddParam(value, expression),
			DbQueryCall call => call.AddParam(value, expression),
			_ => throw new InvalidOperationException(NotFromADefinition),
		});

	/// <summary>Adds the members of an object as parameters.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <typeparam name="TValues">The type of the object.</typeparam>
	/// <param name="query">The query, made by <c>Using</c> on a <see cref="MappedDbQuery{T}"/>.</param>
	/// <param name="values">The object whose members are the parameters.</param>
	/// <returns>A new query with the parameters; <paramref name="query"/> is not changed.</returns>
	/// <exception cref="InvalidOperationException"><paramref name="query"/> was not made from a <see cref="DbQuery"/> definition.</exception>
	public static MappedQuery<T> AddParams<T, TValues>(this MappedQuery<T> query, TValues values)
		where TValues : notnull
		=> query.WithCommand(query.Command switch
		{
			DbQueryUsing source => source.AddParams(values),
			DbQueryCall call => call.AddParams(values),
			_ => throw new InvalidOperationException(NotFromADefinition),
		});
}
