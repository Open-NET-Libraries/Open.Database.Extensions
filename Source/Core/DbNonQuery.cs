using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// A write defined once: the command text (or stored procedure name) and its
/// timeout, with no connection.
/// Immutable and safe to keep in a <see langword="static"/> field and share.
/// </summary>
/// <remarks>
/// <para>
/// Bind it with <c>Using</c>, add the values with <c>AddParam</c> (or
/// <c>AddParams</c>), then run it with <c>ExecuteNonQueryAsync</c>, which
/// returns the number of rows affected:
/// </para>
/// <code language="csharp">
/// static readonly DbNonQuery SetTitle
///     = DbNonQuery.Text("update Posts set Title = @title where Id = @id");
///
/// int rows = await SetTitle.Using(db)
///     .AddParam(id)
///     .AddParam(title)
///     .ExecuteNonQueryAsync(cancellationToken);
/// </code>
/// <para>
/// For rows use <see cref="DbQuery.Map{T}()"/>, and for one value
/// <see cref="DbQuery.Scalar{T}"/>.
/// The connection is opened late and released as <see cref="DbQuery"/> releases
/// it. Output and return parameters are not supported.
/// </para>
/// </remarks>
public sealed class DbNonQuery : IEquatable<DbNonQuery>
{
	// The text, the kind, the timeout and the names are held, and run, as a
	// query's are.
	readonly DbQuery _definition;

	DbNonQuery(DbQuery definition) => _definition = definition;

	/// <summary>Defines a write from SQL text.</summary>
	/// <inheritdoc cref="DbQuery.Text(string)"/>
	public static DbNonQuery Text(string sql)
		=> new(DbQuery.Text(sql));

	/// <summary>Defines a write that calls a stored procedure.</summary>
	/// <inheritdoc cref="DbQuery.Procedure(string)"/>
	public static DbNonQuery Procedure(string name)
		=> new(DbQuery.Procedure(name));

	/// <inheritdoc cref="DbQuery.SetTimeout(ushort)"/>
	public DbNonQuery SetTimeout(ushort seconds)
		=> new(_definition.SetTimeout(seconds));

	PendingCall<NonQueryKind> Pending => new(_definition);

	/// <inheritdoc cref="DbQuery.Params{TParams}"/>
	public DbNonQuery<TParams> Params<TParams>()
		where TParams : struct, IEquatable<TParams>
		=> new(_definition.Params<TParams>());

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundCall<NonQueryKind> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(connection).ToCall());

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundCall<NonQueryKind> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(transaction).ToCall());

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundCall<NonQueryKind> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(pool).ToCall());

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundCall<NonQueryKind> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(factory).ToCall());

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(string, T)"/>
	public PendingCall<NonQueryKind> AddParam<TValue>(string name, TValue value)
		=> Pending.AddParam(name, value);

	/// <inheritdoc cref="DbQueryUsing.AddParam(string, string?)"/>
	public PendingCall<NonQueryKind> AddParam(string name, string? value)
		=> Pending.AddParam(name, value);

	/// <inheritdoc cref="DbQueryUsing.AddParam{T}(T, string?)"/>
	[OverloadResolutionPriority(-1)]
	public PendingCall<NonQueryKind> AddParam<TValue>(
		TValue value,
		[CallerArgumentExpression(nameof(value))] string? expression = null)
		=> Pending.AddParam(value, expression);

	/// <inheritdoc cref="DbQueryUsing.AddParams{T}(T)"/>
	public PendingCall<NonQueryKind> AddParams<TValues>(TValues values)
		where TValues : notnull
		=> Pending.AddParams(values);

	/// <inheritdoc />
	public bool Equals(DbNonQuery? other)
		=> other is not null && _definition.Equals(other._definition);

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> Equals(obj as DbNonQuery);

	/// <inheritdoc />
	public override int GetHashCode()
		=> _definition.GetHashCode();
}

/// <summary>
/// A <see cref="DbNonQuery"/> that declared the type of its values.
/// </summary>
/// <typeparam name="TParams">
/// The type of the values: a <see langword="struct"/> whose public
/// properties and fields are the parameters.
/// </typeparam>
public sealed class DbNonQuery<TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQuery<TParams> _definition;

	internal DbNonQuery(DbQuery<TParams> definition)
	{
		Debug.Assert(definition is not null);
		_definition = definition;
	}

	/// <summary>Adds the values before the source is known.</summary>
	/// <param name="values">The values.</param>
	/// <returns>The values, to be given a source with <c>Using</c>.</returns>
	public PendingParams<NonQueryKind, TParams> AddParams(TParams values)
		=> new(_definition, values);

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public BoundParams<NonQueryKind, TParams> Using(DbConnection connection)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(connection));

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public BoundParams<NonQueryKind, TParams> Using(DbTransaction transaction)
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(transaction));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public BoundParams<NonQueryKind, TParams> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(pool));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public BoundParams<NonQueryKind, TParams> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		// DbQuery.Using throws ArgumentNullException for a null source.
		=> new(_definition.Using(factory));
}
