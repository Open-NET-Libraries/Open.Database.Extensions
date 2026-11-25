namespace Open.Database.Extensions;

/// <summary>
/// Represents a query defined once: the command text (or stored procedure
/// name) and its timeout, with no connection.
/// Immutable and safe to keep in a <see langword="static"/> field and share.
/// </summary>
/// <remarks>
/// <para>
/// Bind it to where connections come from with <c>Using</c>, add the values
/// with <c>AddParam</c> (or <c>AddParams</c>), then map and read as usual:
/// </para>
/// <code language="csharp">
/// static readonly DbQuery PostsByAuthor
///     = DbQuery.Procedure("dbo.PostsByAuthor");
///
/// List&lt;Post&gt; posts = await PostsByAuthor.Using(db)
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
/// <para>
/// A query reads: rows with <c>Map</c>, or one value with
/// <see cref="Scalar{T}"/>. A write is a <see cref="DbNonQuery"/>.
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

	/// <summary>
	/// Gets the <c>@name</c> markers of a text definition, found once when it
	/// is made; <see langword="null"/> for a procedure.
	/// </summary>
	// A definition is a static field, so a call that derives names scans
	// nothing. A procedure's names are remembered per source
	// (ProcedureNameCache). A set with no names means "not known": each
	// argument is sent as written.
	internal NameSet? TextNames { get; }

	/// <summary>
	/// What the members of a values type matched, once per definition, values
	/// type and set of names.
	/// </summary>
	/// <remarks>
	/// It holds the real name of each member in member order
	/// (<see langword="null"/>: the command does not take it).
	/// </remarks>
	// Up to four are kept, newest first: values types used in turn cost nothing
	// once matched, and a definition used with more values types, or on more
	// sets of names, matches the oldest again when it returns.
	internal sealed class MemberMatch(Type type, NameSet names, string?[] resolved, NameSet.Entry?[] entries)
	{
		public Type Type { get; } = type;

		public NameSet Names { get; } = names;

		public string?[] Resolved { get; } = resolved;

		/// <summary>Gets the place of each real name in <see cref="Names"/>, in member order.</summary>
		public NameSet.Entry?[] Entries { get; } = entries;
	}

	internal MemberMatch[]? Matches
	{
		get => Volatile.Read(ref field);
		set => Volatile.Write(ref field, value);
	}

	/// <summary>
	/// The most parameters a call of this definition has bound, for a
	/// definition whose text declares no names (a procedure, or a text whose
	/// parameters are positional).
	/// </summary>
	int _seenParams;

	/// <summary>
	/// Gets how many parameters a call of this definition holds, which sizes the
	/// array a call keeps them in.
	/// </summary>
	/// <remarks>
	/// The number of <c>@name</c> markers of a text; for a definition that declares
	/// none, the most a call of it has bound, or four.
	/// </remarks>
	internal int ParamCapacity
	{
		get
		{
			int declared = TextNames?.Names.Length ?? 0;
			if (declared > 0) return declared;

			int seen = Volatile.Read(ref _seenParams);
			return seen > 0 ? seen : 4;
		}
	}

	/// <summary>
	/// Remembers how many parameters a call bound, so the next call sizes its
	/// array to fit.
	/// </summary>
	/// <param name="count">The number of parameters the call bound.</param>
	internal void NoteParams(int count)
	{
		if (count > Volatile.Read(ref _seenParams))
			Volatile.Write(ref _seenParams, count);
	}

	internal string CommandText { get; }

	internal CommandType Kind { get; }

	/// <summary>
	/// Gets the command timeout in seconds, or -1 when no timeout was set:
	/// the provider's default applies.
	/// </summary>
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
	// The @name markers of the text are found once, here.
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
		=> DbQueryCall.CheckedText(
			text,
			paramName,
			"The command cannot be empty or white space.");

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
	/// Maps each row to a <typeparamref name="T"/> by column name (a single value such as
	/// <see cref="int"/> is read from the first column), and returns the mapped definition.
	/// </summary>
	/// <remarks>
	/// The result is immutable and has no connection yet, so it can be held in a static field;
	/// give it a source with <c>Using</c> and read it with a terminal.
	/// </remarks>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <returns>The mapped definition.</returns>
	// Any other type is read through RowMap<T>.Default, shared, so its compiled
	// reader is built once.
	public MappedDbQuery<T> Map<T>()
		=> FirstColumn<T>.Instance is { } firstColumn
		? new(this, firstColumn)
		: new(this, RowMap<T>.Default);

	/// <summary>Maps each row to a <typeparamref name="T"/> with a map the caller declared, and returns the mapped definition.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="map">The map. <see langword="null"/> reads as <see cref="Map{T}()"/> does.</param>
	/// <returns>The mapped definition.</returns>
	// A bare null does not compile here (it also fits the selector overload):
	// write (RowMap<T>?)null. The definition keeps map, so its compiled reader
	// is shared by every call.
	public MappedDbQuery<T> Map<T>(RowMap<T>? map)
		=> map is null ? Map<T>() : new(this, map);

	/// <summary>Maps each row with a selector, and returns the mapped definition.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="selector">Called for each row, from any thread and by overlapping calls of the same definition, so it must be safe for concurrent use.</param>
	/// <returns>The mapped definition.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
	public MappedDbQuery<T> Map<T>(Func<IDataRecord, T> selector)
	{
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new(this, new SelectorSource<T>(selector));
	}

	/// <summary>
	/// Reads each row without a model, as an array of its values, and returns the mapped definition.
	/// </summary>
	/// <remarks>See <see cref="CoreExtensions.AsObjectArrays(IExecuteReader)"/>: the same rows, with this definition's options.</remarks>
	/// <returns>The mapped definition.</returns>
	public MappedDbQuery<object?[]> AsObjectArrays()
		=> new(this, ObjectArraySource.Instance);

	/// <summary>
	/// Reads each row without a model, as a <see cref="DictionaryRow"/>, and returns the mapped definition.
	/// </summary>
	/// <remarks>See <see cref="CoreExtensions.AsDictionaryRows(IExecuteReader)"/>: the same rows, with this definition's options.</remarks>
	/// <returns>The mapped definition.</returns>
	public MappedDbQuery<DictionaryRow> AsDictionaryRows()
		=> new(this, DictionaryRowSource.Instance);

	/// <summary>
	/// Reads one value, the first column of the first row, as a <typeparamref name="T"/>,
	/// and returns the scalar definition.
	/// </summary>
	/// <remarks>
	/// The result is immutable and has no connection yet, so it can be held in a static field;
	/// give it a source with <c>Using</c> and run it with <c>ExecuteScalarAsync</c>.
	/// It keeps the timeout of this definition.
	/// </remarks>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <returns>The scalar definition.</returns>
	public ScalarDbQuery<T> Scalar<T>()
		=> new(this);

	/// <summary>
	/// Declares the type of the values: a <see langword="struct"/>
	/// (a <see langword="readonly"/> <see langword="record"/>
	/// <see langword="struct"/> is the intended shape)
	/// whose public properties and fields are the parameters.
	/// </summary>
	/// <remarks>
	/// The values are then given in one call, which the compiler checks:
	/// <c>.Using(db).AddParams(new(AuthorId: authorId, Day: day))</c>.
	/// A class object that already exists (for example a deserialized request) is added with <c>AddParams</c> on the untyped definition.
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
		=> new(
			this,
			connection ?? throw new ArgumentNullException(
				nameof(connection)));

	/// <summary>
	/// Binds the query to a transaction, which runs it on the
	/// transaction's connection.
	/// </summary>
	/// <remarks>
	/// The transaction must not be completed, and its connection must be open, when a terminal runs; otherwise the run throws <see cref="InvalidOperationException"/>.
	/// The query never opens, closes or gives back the transaction's connection.
	/// </remarks>
	/// <param name="transaction">The transaction to run in.</param>
	/// <returns>The query, bound.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="transaction"/> is <see langword="null"/>.
	/// </exception>
	public DbQueryUsing Using(DbTransaction transaction)
		=> new(
			this,
			transaction ?? throw new ArgumentNullException(
				nameof(transaction)));

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
		=> new(
			this,
			pool ?? throw new ArgumentNullException(
				nameof(pool)));

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
		=> new(
			this,
			factory ?? throw new ArgumentNullException(
				nameof(factory)));

	/// <inheritdoc />
	public bool Equals(DbQuery? other)
		=> other is not null
		&& (ReferenceEquals(this, other)
			|| Timeout == other.Timeout
				&& Kind == other.Kind
				&& string.Equals(
					CommandText,
					other.CommandText,
					StringComparison.Ordinal));

	/// <inheritdoc />
	public override bool Equals(object? obj)
		=> Equals(obj as DbQuery);

	/// <inheritdoc />
	public override int GetHashCode()
	{
		unchecked
		{
			int h = StringComparer.Ordinal.GetHashCode(CommandText);
			h = h * 31 + (int)Kind;
			return h * 31 + Timeout;
		}
	}
}

/// <summary>
/// Represents a <see cref="DbQuery"/> that declared the type of its values.
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

	/// <summary>Maps each row to a <typeparamref name="T"/> by column name, and returns the mapped definition.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <returns>The mapped definition, whose parameters are the struct.</returns>
	public MappedDbQuery<TParams, T> Map<T>()
		=> new(_definition.Map<T>());

	/// <summary>Maps each row to a <typeparamref name="T"/> with a map the caller declared, and returns the mapped definition.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="map">The map. <see langword="null"/> reads as <see cref="Map{T}()"/> does.</param>
	/// <returns>The mapped definition, whose parameters are the struct.</returns>
	// A bare null does not compile here (it also fits the selector overload):
	// write (RowMap<T>?)null.
	public MappedDbQuery<TParams, T> Map<T>(RowMap<T>? map)
		=> new(_definition.Map(map));

	/// <summary>Maps each row with a selector, and returns the mapped definition.</summary>
	/// <typeparam name="T">The type each row is mapped to.</typeparam>
	/// <param name="selector">Called for each row, from any thread and by overlapping calls of the same definition, so it must be safe for concurrent use.</param>
	/// <returns>The mapped definition, whose parameters are the struct.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="selector"/> is <see langword="null"/>.</exception>
	public MappedDbQuery<TParams, T> Map<T>(Func<IDataRecord, T> selector)
		// DbQuery.Map throws ArgumentNullException for a null selector.
		=> new(_definition.Map(selector));

	/// <inheritdoc cref="DbQuery.AsObjectArrays"/>
	/// <returns>The mapped definition, whose parameters are the struct.</returns>
	public MappedDbQuery<TParams, object?[]> AsObjectArrays()
		=> new(_definition.AsObjectArrays());

	/// <inheritdoc cref="DbQuery.AsDictionaryRows"/>
	/// <returns>The mapped definition, whose parameters are the struct.</returns>
	public MappedDbQuery<TParams, DictionaryRow> AsDictionaryRows()
		=> new(_definition.AsDictionaryRows());

	/// <inheritdoc cref="DbQuery.Scalar{T}"/>
	/// <returns>The scalar definition, whose parameters are the struct.</returns>
	public ScalarDbQuery<TParams, T> Scalar<T>()
		=> new(this);

	/// <inheritdoc cref="DbQuery.Using(DbConnection)"/>
	public DbQueryUsing<TParams> Using(DbConnection connection)
		=> new(
			_definition,
			connection ?? throw new ArgumentNullException(
				nameof(connection)));

	/// <inheritdoc cref="DbQuery.Using(DbTransaction)"/>
	public DbQueryUsing<TParams> Using(DbTransaction transaction)
		=> new(
			_definition,
			transaction ?? throw new ArgumentNullException(
				nameof(transaction)));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionPool{TConnection})"/>
	public DbQueryUsing<TParams> Using<TConnection>(
		IDbConnectionPool<TConnection> pool)
		where TConnection : DbConnection
		=> new(
			_definition,
			pool ?? throw new ArgumentNullException(
				nameof(pool)));

	/// <inheritdoc cref="DbQuery.Using{TConnection}(IDbConnectionFactory{TConnection})"/>
	public DbQueryUsing<TParams> Using<TConnection>(
		IDbConnectionFactory<TConnection> factory)
		where TConnection : DbConnection
		=> new(
			_definition,
			factory ?? throw new ArgumentNullException(
				nameof(factory)));
}
