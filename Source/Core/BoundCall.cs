namespace Open.Database.Extensions;

/// <summary>
/// Marks a chain that reads one value: it ends in <c>ExecuteScalarAsync</c>.
/// </summary>
/// <typeparam name="T">The type of the value.</typeparam>
public readonly struct ScalarKind<T>;

/// <summary>
/// Marks a chain that writes: it ends in <c>ExecuteNonQueryAsync</c>.
/// </summary>
public readonly struct NonQueryKind;

/// <summary>
/// A scalar query or a write bound to where its connections come from, with
/// the values added so far.
/// </summary>
/// <remarks>
/// Made by <c>Using</c> on a <see cref="ScalarDbQuery{T}"/> or a
/// <see cref="DbNonQuery"/>. Each <c>AddParam</c> returns a new one and leaves
/// this one unchanged, so a call can be kept and branched.
/// Nothing touches the database until the terminal runs.
/// </remarks>
/// <typeparam name="TKind">
/// What the call does: <see cref="ScalarKind{T}"/> or
/// <see cref="NonQueryKind"/>.
/// </typeparam>
public readonly struct BoundCall<TKind> : IEquatable<BoundCall<TKind>>
{
	internal BoundCall(DbQueryCall call) => Call = call;

	// The same value a rows query holds: the definition, the source and the
	// values.
	internal DbQueryCall Call { get; }

	/// <inheritdoc cref="DbQueryCall.Source"/>
	public object Source => Call.SourceCore;

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public BoundCall<TKind> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> new(Call.AddParams(values));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public BoundCall<TKind> AddParam<TValue>(string name, TValue value)
		=> new(Call.AddParam(name, value));

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public BoundCall<TKind> AddParam(string name, string? value)
		=> new(Call.AddParam(name, value));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public BoundCall<TKind> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new(Call.AddParam(value, expression));

	/// <inheritdoc />
	public bool Equals(BoundCall<TKind> other)
		=> Call.Equals(other.Call);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> obj is BoundCall<TKind> other && Equals(other);

	/// <summary>Determines whether two calls are the same definition on the same source with the same values added.</summary>
	/// <param name="left">The first call.</param>
	/// <param name="right">The second call.</param>
	/// <returns><see langword="true"/> if they are equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator ==(BoundCall<TKind> left, BoundCall<TKind> right)
		=> left.Equals(right);

	/// <summary>Determines whether two calls differ.</summary>
	/// <param name="left">The first call.</param>
	/// <param name="right">The second call.</param>
	/// <returns><see langword="true"/> if they are not equal; otherwise, <see langword="false"/>.</returns>
	public static bool operator !=(BoundCall<TKind> left, BoundCall<TKind> right)
		=> !left.Equals(right);

	/// <inheritdoc />
	public override int GetHashCode()
	{
		unchecked
		{
			return Call.GetHashCode() * 31 + typeof(TKind).GetHashCode();
		}
	}
}

/// <summary>
/// The values of a scalar query or a write, added before it has a source.
/// It has no terminal: <c>Using</c> gives it a source.
/// </summary>
/// <typeparam name="TKind">
/// What the call does: <see cref="ScalarKind{T}"/> or
/// <see cref="NonQueryKind"/>.
/// </typeparam>
public readonly struct PendingCall<TKind>
{
	readonly ParamList _list;

	internal PendingCall(DbQuery definition, ParamList list = default)
	{
		Definition = definition;
		_list = list;
	}

	// Null only in a default value.
	DbQuery Definition
		=> field ?? throw DbQueryRunner.Unbound();

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public PendingCall<TKind> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> new(Definition, _list.AddMembers(Definition, values));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public PendingCall<TKind> AddParam<TValue>(string name, TValue value)
		=> new(
			Definition,
			_list.AddNamed(Definition, DbQueryCall.CheckedName(name), value));

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public PendingCall<TKind> AddParam(string name, string? value)
		=> new(
			Definition,
			_list.AddNamed(Definition, DbQueryCall.CheckedName(name), value));

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public PendingCall<TKind> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> new(
			Definition,
			_list.AddDerived(
				Definition,
				DbQueryCall.CheckedExpression(expression),
				value));

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundCall<TKind> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Using(connection));

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundCall<TKind> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Using(transaction));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundCall<TKind> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Using(pool));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundCall<TKind> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> Bind(Definition.Using(factory));

	BoundCall<TKind> Bind(DbQueryUsing root)
		=> new(root.WithParams(_list));
}

/// <summary>
/// A scalar query or a write with typed values that has a source and waits for
/// the values.
/// </summary>
/// <typeparam name="TKind">
/// What the call does: <see cref="ScalarKind{T}"/> or
/// <see cref="NonQueryKind"/>.
/// </typeparam>
/// <typeparam name="TParams">The type of the values.</typeparam>
public readonly struct BoundParams<TKind, TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQueryUsing<TParams> _using;

	internal BoundParams(DbQueryUsing<TParams> source) => _using = source;

	/// <inheritdoc cref="DbQueryUsing{TParams}.AddParams(TParams)"/>
	public BoundCall<TKind> AddParams(TParams values)
		=> new(_using.AddParams(values));
}

/// <summary>
/// The typed values of a scalar query or a write, added before it has a
/// source.
/// It has no terminal: <c>Using</c> gives it a source.
/// </summary>
/// <typeparam name="TKind">
/// What the call does: <see cref="ScalarKind{T}"/> or
/// <see cref="NonQueryKind"/>.
/// </typeparam>
/// <typeparam name="TParams">The type of the values.</typeparam>
public readonly struct PendingParams<TKind, TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly TParams _values;

	internal PendingParams(DbQuery<TParams> definition, TParams values)
	{
		Definition = definition;
		_values = values;
	}

	// Null only in a default value.
	DbQuery<TParams> Definition
		=> field ?? throw DbQueryRunner.Unbound();

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundCall<TKind> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(Definition.Using(connection).AddParams(_values));

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundCall<TKind> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(Definition.Using(transaction).AddParams(_values));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundCall<TKind> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(Definition.Using(pool).AddParams(_values));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundCall<TKind> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(Definition.Using(factory).AddParams(_values));
}

/// <summary>
/// The terminals of a <see cref="BoundCall{TKind}"/>: a scalar call has
/// <c>ExecuteScalarAsync</c>, a write has <c>ExecuteNonQueryAsync</c>.
/// </summary>
public static class BoundCallExtensions
{
	/// <summary>
	/// Runs the query and returns the first column of the first row, converted
	/// as <c>Map&lt;T&gt;()</c> converts a column.
	/// </summary>
	/// <remarks>
	/// A NULL is <see langword="null"/> for a <typeparamref name="T"/> that is
	/// nullable or a reference type, and so is no row.
	/// </remarks>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <param name="call">The bound call.</param>
	/// <param name="cancellationToken">The token that governs the call.</param>
	/// <returns>A task that represents the asynchronous operation; its result is the value.</returns>
	/// <exception cref="ValueMappingException">There was no row or the value is NULL and <typeparamref name="T"/> is a non-nullable value type (ask for the nullable type and use <c>??</c> for a default), the value does not convert to <typeparamref name="T"/>, or it is out of its range (the conversion's own error is the inner exception).</exception>
	/// <exception cref="InvalidOperationException"><paramref name="call"/> is a <see langword="default"/> value, which is bound to nothing.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
	public static ValueTask<T> ExecuteScalarAsync<T>(
		this BoundCall<ScalarKind<T>> call,
		CancellationToken cancellationToken = default)
		=> DbQueryRunner.ScalarAsync<T>(
			call.Call.Definition,
			call.Call.SourceCore,
			call.Call.Params,
			cancellationToken);

	/// <summary>Runs the write and returns the number of rows affected.</summary>
	/// <remarks>
	/// The count is what the provider reports, which can be -1 (for example for
	/// a procedure that sets <c>NOCOUNT ON</c>).
	/// It is not a procedure's return value.
	/// </remarks>
	/// <param name="call">The bound call.</param>
	/// <param name="cancellationToken">The token that governs the call.</param>
	/// <returns>A task that represents the asynchronous operation; its result is the number of rows affected.</returns>
	/// <exception cref="InvalidOperationException"><paramref name="call"/> is a <see langword="default"/> value, which is bound to nothing.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
	public static ValueTask<int> ExecuteNonQueryAsync(
		this BoundCall<NonQueryKind> call,
		CancellationToken cancellationToken = default)
		=> DbQueryRunner.NonQueryAsync(
			call.Call.Definition,
			call.Call.SourceCore,
			call.Call.Params,
			cancellationToken);
}
