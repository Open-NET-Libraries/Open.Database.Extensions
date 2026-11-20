namespace Open.Database.Extensions;

/// <summary>
/// A query defined once: the command text (or stored procedure name) and
/// its timeout, with no connection.
/// Immutable and safe to keep in a <see langword="static"/> field and share.
/// </summary>
/// <remarks>
/// <para>
/// Bind it to where connections come from with <c>Using</c>, add the values
/// with <c>AddParam</c> (or <c>AddParams</c>), then map and read as usual:
/// </para>
/// <code>
/// static readonly DbQuery PostsByAuthor
///     = DbQuery.Procedure("dbo.PostsByAuthor");
///
/// List&lt;Post&gt; posts = await PostsByAuthor
///     .Using(db)
///     .AddParam(authorId)
///     .AddParam(day)
///     .Map&lt;Post&gt;()
///     .ToListAsync(cancellationToken);
/// </code>
/// <para>
/// Nothing touches the database until a terminal runs. The connection is
/// opened late, closed again only if the call opened it, and a connection
/// from a factory is disposed or one from a pool is given back, exactly as
/// the command builders do.
/// </para>
/// <para>
/// Declare the type of the values with <see cref="Params{TParams}"/> to have
/// them checked at compile time.
/// </para>
/// </remarks>
public sealed class DbQuery : IEquatable<DbQuery>
{
	DbQuery(string text, CommandType kind, int timeout, NameSet? textNames)
	{
		CommandText = text;
		Kind = kind;
		Timeout = timeout;
		TextNames = textNames;
	}

	// The @name markers of a text definition, found once when it is made (a definition
	// is a static field), so a call that derives names scans nothing. null for a
	// procedure, whose names are remembered per source (ProcedureNameCache).
	// A set with no names means "not known": each argument is sent as written.
	internal NameSet? TextNames { get; }

	// What the members of a values type matched, once per definition, values type
	// and set of names: the real name of each member in member order (null: the
	// command does not take it). Up to four are kept, newest first (a definition used with
	// more values types, or on more sets of names, drops the oldest), so values
	// types used in turn cost nothing once matched.
	internal const int MaxMatches = 4;

	internal sealed class MemberMatch(Type type, NameSet names, string?[] resolved)
	{
		internal Type Type { get; } = type;

		internal NameSet Names { get; } = names;

		internal string?[] Resolved { get; } = resolved;
	}

	internal MemberMatch[]? Matches
	{
		get => Volatile.Read(ref field);
		set => Volatile.Write(ref field, value);
	}

	internal string CommandText { get; }

	internal CommandType Kind { get; }

	// -1 when no timeout was set: the provider's default applies.
	internal int Timeout { get; }

	/// <summary>Defines a query from SQL text.</summary>
	/// <param name="sql">The SQL text.</param>
	/// <returns>The definition.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="sql"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="ArgumentException">
	/// <paramref name="sql"/> is empty or white space.
	/// </exception>
	public static DbQuery Text(string sql)
		=> new(
			Checked(sql, nameof(sql)),
			CommandType.Text,
			-1,
			new NameSet(SqlTextParameters.Parse(sql)));

	/// <summary>Defines a query that calls a stored procedure.</summary>
	/// <param name="name">The name of the stored procedure.</param>
	/// <returns>The definition.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="name"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="ArgumentException">
	/// <paramref name="name"/> is empty or white space.
	/// </exception>
	public static DbQuery Procedure(string name)
		=> new(
			Checked(name, nameof(name)),
			CommandType.StoredProcedure,
			-1,
			null);

	static string Checked(string text, string paramName)
	{
		if (text is null) throw new ArgumentNullException(paramName);
		if (string.IsNullOrWhiteSpace(text))
		{
			throw new ArgumentException(
				"The command cannot be empty or white space.",
				paramName);
		}

		return text;
	}

	/// <summary>
	/// Returns a new definition with the command timeout set;
	/// this one is not changed.
	/// </summary>
	/// <param name="seconds">
	/// The number of seconds to wait for the command.
	/// </param>
	/// <returns>The new definition.</returns>
	public DbQuery SetTimeout(ushort seconds)
		=> new(CommandText, Kind, seconds, TextNames);

	/// <summary>
	/// Declares the type of the values: a <see langword="struct"/>
	/// (a <see langword="readonly"/> <see langword="record"/>
	/// <see langword="struct"/> is the intended shape)
	/// whose public properties and fields are the parameters.
	/// </summary>
	/// <remarks>
	/// The values are then given in one call, which the compiler checks:
	/// <c>.Using(db).AddParams(new(AuthorId: authorId, Day: day))</c>.
	/// </remarks>
	/// <typeparam name="TParams">The type of the values.</typeparam>
	/// <returns>The typed definition.</returns>
	public DbQuery<TParams> Params<TParams>()
		where TParams : struct, IEquatable<TParams>
		=> new(this);

	/// <summary>
	/// Binds the query to a connection.
	/// The connection is opened late and closed again only if this call
	/// opened it; it is never disposed.
	/// </summary>
	/// <param name="connection">The connection to run on.</param>
	/// <returns>The query, bound.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="connection"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryUsing Using(DbConnection connection)
		=> new(this, connection);

	/// <summary>
	/// Binds the query to a transaction, which runs it on the
	/// transaction's connection.
	/// </summary>
	/// <param name="transaction">The transaction to run in.</param>
	/// <returns>The query, bound.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="transaction"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryUsing Using(DbTransaction transaction)
		=> new(this, transaction);

	/// <summary>
	/// Binds the query to a pool: each run takes a connection, opens it
	/// if needed, and gives it back.
	/// </summary>
	/// <remarks>
	/// The pool must hand out <typeparamref name="TConnection"/> connections,
	/// which are <see cref="DbConnection"/>s. A pool made by
	/// <c>AsGeneric()</c> is an <c>IDbConnectionPool&lt;IDbConnection&gt;</c> and does
	/// not fit; use a pool of <see cref="DbConnection"/> (or of a provider's
	/// connection type), or a factory.
	/// </remarks>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="pool">The pool to take connections from.</param>
	/// <returns>The query, bound.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="pool"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryUsing Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		=> new(this, pool);

	/// <summary>
	/// Binds the query to a factory: each run creates a connection,
	/// opens it, and disposes it when finished.
	/// </summary>
	/// <typeparam name="TConnection">The type of the connection.</typeparam>
	/// <param name="factory">The factory that creates connections.</param>
	/// <returns>The query, bound.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="factory"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryUsing Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		=> new(this, factory);

	/// <inheritdoc />
	public bool Equals(DbQuery? other)
		=> other is not null
		&& (ReferenceEquals(this, other)
			|| (Timeout == other.Timeout
				&& Kind == other.Kind
				&& string.Equals(
					CommandText,
					other.CommandText,
					StringComparison.Ordinal)));

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> Equals(obj as DbQuery);

	/// <inheritdoc />
	public override int GetHashCode()
	{
		unchecked
		{
			int h = StringComparer.Ordinal.GetHashCode(CommandText);
			h = (h * 31) + (int)Kind;
			return (h * 31) + Timeout;
		}
	}
}

/// <summary>
/// A <see cref="DbQuery"/> that declared the type of its values.
/// </summary>
/// <typeparam name="TParams">
/// The type of the values: a <see langword="struct"/> whose public
/// properties and fields are the parameters.
/// </typeparam>
public sealed class DbQuery<TParams>
	where TParams : struct, IEquatable<TParams>
{
	readonly DbQuery _definition;

	internal DbQuery(DbQuery definition) => _definition = definition;

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public DbQueryUsing<TParams> Using(DbConnection connection)
		=> new(_definition, connection);

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public DbQueryUsing<TParams> Using(DbTransaction transaction)
		=> new(_definition, transaction);

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public DbQueryUsing<TParams> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		=> new(_definition, pool);

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public DbQueryUsing<TParams> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		=> new(_definition, factory);
}
