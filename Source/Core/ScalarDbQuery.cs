using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// A query defined once that reads one value: the first column of the first
/// row, as a <typeparamref name="T"/>. It has no connection yet.
/// </summary>
/// <remarks>
/// <para>
/// Made by <see cref="DbQuery.Scalar{T}"/>.
/// It is immutable, so a <see langword="static"/> field holds it:
/// </para>
/// <code language="csharp">
/// static readonly ScalarDbQuery&lt;int&gt; PostCount
///     = DbQuery.Text("select count(*) from Posts where AuthorId = @authorId")
///         .Scalar&lt;int&gt;();
///
/// int count = await PostCount.Using(db)
///     .AddParam(authorId)
///     .ExecuteScalarAsync(cancellationToken);
/// </code>
/// <para>
/// The values can be added before or after <c>Using</c>. The timeout is the one
/// of the <see cref="DbQuery"/> it was made from.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of the value.</typeparam>
public sealed class ScalarDbQuery<T>
{
	readonly DbQuery _definition;

	internal ScalarDbQuery(DbQuery definition) => _definition = definition;

	PendingCall<ScalarKind<T>> Pending => new(_definition);

	/// <inheritdoc cref="DbQuery.Params{TParams}"/>
	public ScalarDbQuery<TParams, T> Params<TParams>()
		where TParams : struct, IEquatable<TParams>
		=> new(_definition.Params<TParams>());

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundCall<ScalarKind<T>> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(connection).ToCall());

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundCall<ScalarKind<T>> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(transaction).ToCall());

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundCall<ScalarKind<T>> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(pool).ToCall());

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundCall<ScalarKind<T>> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(factory).ToCall());

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public PendingCall<ScalarKind<T>> AddParam<TValue>(string name, TValue value)
		=> Pending.AddParam(name, value);

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public PendingCall<ScalarKind<T>> AddParam(string name, string? value)
		=> Pending.AddParam(name, value);

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public PendingCall<ScalarKind<T>> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> Pending.AddParam(value, expression);

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public PendingCall<ScalarKind<T>> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> Pending.AddParams(values);
}

/// <summary>
/// A <see cref="ScalarDbQuery{T}"/> whose values are one typed struct, so the
/// compiler checks them.
/// </summary>
/// <typeparam name="TParams">The type of the values.</typeparam>
/// <typeparam name="T">The type of the value the query reads.</typeparam>
public sealed class ScalarDbQuery<TParams, T>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQuery<TParams> _definition;

	internal ScalarDbQuery(DbQuery<TParams> definition)
	{
		Debug.Assert(definition is not null);
		_definition = definition;
	}

	/// <summary>Adds the values before the source is known.</summary>
	/// <param name="values">The values.</param>
	/// <returns>The values, to be given a source with <c>Using</c>.</returns>
	public PendingParams<ScalarKind<T>, TParams> AddParams(TParams values)
		=> new(_definition, values);

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundParams<ScalarKind<T>, TParams> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(connection));

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundParams<ScalarKind<T>, TParams> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(transaction));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundParams<ScalarKind<T>, TParams> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(pool));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundParams<ScalarKind<T>, TParams> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(factory));
}
