using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// The one run path of a <see cref="DbQuery"/>.
/// </summary>
/// <remarks>
/// A run takes the connection, opens it if it is not open, builds one
/// <see cref="DbCommand"/> with the bound parameters, runs the caller's
/// function on the reader, and releases the connection.
/// A connection of the caller's is closed again if the run opened it, and stays open if it was open;
/// one from a factory is disposed and one from a pool is given back (see <see cref="Lease"/>).
/// </remarks>
static class DbQueryRunner
{
	/// <summary>
	/// Makes the exception thrown when a default (unbound) value is used.
	/// </summary>
	/// <returns>The exception.</returns>
	public static InvalidOperationException Unbound()
		=> new("This query was not created by Using(...); a default value cannot be used.");

	/// <summary>
	/// What one run holds: the connection, and what is owed when the run ends.
	/// </summary>
	/// <remarks>
	/// <see cref="Dispose"/> settles what is owed, so a run is
	/// <c>using Lease lease = Lease.Open(...)</c>.
	/// It ends the way the source calls for:
	/// <list type="bullet">
	/// <item>a connection of the caller's is closed again if the run opened it, and is never disposed;</item>
	/// <item>a connection from a factory is disposed;</item>
	/// <item>a connection from a pool is given back (and what that means is the pool's, as in <c>pool.Open</c>);</item>
	/// <item>a transaction's connection is the transaction's: it is never opened, closed or given back here.</item>
	/// </list>
	/// All of this holds when the run throws.
	/// </remarks>
	// It wraps a ConnectionScope.
	public readonly struct Lease : IDisposable
	{
		readonly ConnectionScope<DbConnection> _scope;

		Lease(
			ConnectionScope<DbConnection> scope,
			DbConnection connection,
			DbTransaction? transaction)
		{
			_scope = scope;
			Connection = connection;
			Transaction = transaction;
		}

		public DbConnection Connection { get; }

		public DbTransaction? Transaction { get; }

		/// <summary>
		/// What a source supplies, before anything is opened.
		/// </summary>
		readonly struct Taken(
			DbConnection connection,
			DbTransaction? transaction,
			ScopeEnd end,
			IDbConnectionPool? pool)
		{
			public DbConnection Connection => connection;

			public DbTransaction? Transaction => transaction;

			public ScopeEnd End => end;

			public IDbConnectionPool? Pool => pool;
		}

		/// <summary>
		/// Takes a connection from the source and opens it if it needs to be.
		/// </summary>
		/// <param name="source">The connection, transaction, pool or factory.</param>
		/// <returns>The lease.</returns>
		/// <exception cref="InvalidOperationException">
		/// The source supplied no connection, or one that is not a
		/// <see cref="DbConnection"/>; or the transaction has completed or its
		/// connection is not open.
		/// </exception>
		public static Lease Open(object source)
		{
			Taken taken = Take(source);
			return taken.Transaction is { } transaction
				? new(default, taken.Connection, transaction)
				: Of(Open(taken), taken);
		}

		/// <summary>
		/// Asynchronously takes a connection from the source and opens it if it
		/// needs to be.
		/// </summary>
		/// <param name="source">The connection, transaction, pool or factory.</param>
		/// <param name="cancellationToken">The token that governs the call.</param>
		/// <returns>A task that represents the asynchronous operation; its result is the lease.</returns>
		/// <inheritdoc cref="Open(object)" path="/exception"/>
		public static async ValueTask<Lease> OpenAsync(
			object source,
			CancellationToken cancellationToken)
		{
			Taken taken = Take(source);
			return taken.Transaction is { } transaction
				? new(default, taken.Connection, transaction)
				: Of(
					await OpenAsync(taken, cancellationToken).ConfigureAwait(false),
					taken);
		}

		static Lease Of(ConnectionScope<DbConnection> scope, Taken taken)
			=> new(scope, taken.Connection, null);

		/// <summary>
		/// Opens the connection if it needs to be.
		/// </summary>
		/// <param name="taken">What the source supplied.</param>
		/// <returns>The scope that settles the connection when the run ends.</returns>
		// Done after the supplied type is known to be right: a source that gave
		// the wrong kind of connection never has it opened.
		static ConnectionScope<DbConnection> Open(Taken taken)
			=> taken.End == ScopeEnd.Close
				? taken.Connection.EnsureOpenScope()
				: ConnectionExtensions.OpenOwned(taken.Connection, taken.Pool);

		static ValueTask<ConnectionScope<DbConnection>> OpenAsync(
			Taken taken,
			CancellationToken cancellationToken)
			=> taken.End == ScopeEnd.Close
				? taken.Connection.EnsureOpenScopeAsync(cancellationToken)
				: ConnectionExtensions.OpenOwnedAsync(
					taken.Connection,
					taken.Pool,
					cancellationToken);

		static Taken Take(object source)
		{
			Debug.Assert(source is not null);
			DbConnection? connection;
			switch (source)
			{
				case DbConnection direct:
					return new(direct, null, ScopeEnd.Close, null);
				case DbTransaction tx:
					return new(OpenConnectionOf(tx), tx, ScopeEnd.Close, null);
				case IDbConnectionFactory factory:
					connection = AsDbConnection(factory.Create(), factory);
					return connection is null
						? throw NoConnection()
						: new(connection, null, ScopeEnd.Dispose, null);
				default:
					var pool = (IDbConnectionPool)source;
					connection = AsDbConnection(pool.Take(), source);
					return connection is null
						? throw NoConnection()
						: new(connection, null, ScopeEnd.Give, pool);
			}
		}

		static InvalidOperationException NoConnection()
			=> new("The connection source supplied no connection.");

		/// <summary>
		/// Casts what a source supplied to a <see cref="DbConnection"/>.
		/// </summary>
		/// <remarks>
		/// A source that supplies something that is not a
		/// <see cref="DbConnection"/> has it let go the way its source lets go
		/// of a connection (disposed for a factory, given back to a pool), not
		/// leaked, and the run fails with the type named.
		/// <see langword="null"/> is left to the caller's check.
		/// </remarks>
		/// <param name="supplied">What the source supplied.</param>
		/// <param name="source">The factory or pool it came from.</param>
		/// <returns>The connection, or <see langword="null"/> when none was supplied.</returns>
		/// <exception cref="InvalidOperationException">The source supplied something that is not a <see cref="DbConnection"/>.</exception>
		static DbConnection? AsDbConnection(IDbConnection? supplied, object source)
		{
			if (supplied is null or DbConnection)
				return supplied as DbConnection;

			if (source is not IDbConnectionFactory and IDbConnectionPool pool)
				pool.Give(supplied);
			else
				supplied.Dispose();

			throw new InvalidOperationException(
				$"The connection source supplied a {supplied.GetType()}, "
				+ "which is not a DbConnection.");
		}

		/// <summary>
		/// Gets the connection of a transaction.
		/// </summary>
		/// <remarks>
		/// A transaction is begun on an open connection and holds it: it is
		/// neither opened nor reopened here.
		/// </remarks>
		/// <param name="transaction">The transaction.</param>
		/// <returns>The transaction's open connection.</returns>
		/// <exception cref="InvalidOperationException">The transaction has completed, or its connection is not open.</exception>
		static DbConnection OpenConnectionOf(DbTransaction transaction)
		{
			DbConnection connection = transaction.Connection
				?? throw new InvalidOperationException(
					"The transaction has completed, so it has no connection.");

			return ConnectionExtensions.NeedsOpen(connection.State)
				? throw new InvalidOperationException(
					"The transaction's connection is not open.")
				: connection;
		}

		/// <summary>
		/// Gets the behavior to run the reader with.
		/// </summary>
		/// <remarks>
		/// A transaction's connection is never opened here, so it adds nothing.
		/// </remarks>
		/// <param name="behavior">The behavior the caller asked for.</param>
		/// <returns>The behavior to use.</returns>
		public CommandBehavior Behavior(CommandBehavior behavior)
			=> Transaction is null
				? _scope.ReaderBehavior(behavior)
				: behavior;

		public void Dispose() => _scope.Dispose();

		/// <summary>
		/// Gets the scope's own awaitable, for <see langword="await"/> <see langword="using"/>.
		/// </summary>
		/// <param name="continueOnCapturedContext">The value passed on to the scope's own <c>ConfigureAwait</c>.</param>
		/// <returns>The configured scope.</returns>
		// Exists so that this struct is not boxed (it holds no setting; the
		// library never returns to a context).
		public ConfiguredConnectionScope<DbConnection> ConfigureAwait(
			bool continueOnCapturedContext)
			=> _scope.ConfigureAwait(continueOnCapturedContext);
	}
	// The connection is opened before the command is created, as some providers
	// need.
	static DbCommand Make(
		DbConnection connection,
		DbTransaction? transaction,
		DbQuery definition,
		ParamList call,
		NameSet? names)
	{
		DbCommand command = connection.CreateCommand();
		try
		{
			command.Transaction = transaction;
			command.CommandText = definition.CommandText;
			command.CommandType = definition.Kind;
			if (definition.Timeout >= 0)
				command.CommandTimeout = definition.Timeout;
			call.BindAll(command, definition, names);
			return command;
		}
		catch
		{
			command.Dispose();
			throw;
		}
	}

	// ---- the real names of derived parameters ----

	/// <summary>
	/// Gets the object that remembered names belong to: the object
	/// connections come from.
	/// </summary>
	/// <remarks>
	/// That is the factory or pool (not the adapter <c>AsPool</c> makes around
	/// a factory), or the connection itself.
	/// </remarks>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="connection">The connection of the run.</param>
	/// <returns>The owner of the remembered names.</returns>
	static object NameCacheOwner(object source, DbConnection connection)
		=> source switch
		{
			DbTransaction => connection,
			IConnectionIdentity adapter => adapter.Identity,
			_ => source,
		};

	/// <summary>
	/// Makes the key a procedure's names are remembered under.
	/// </summary>
	/// <param name="connection">The connection of the run.</param>
	/// <param name="definition">The query definition.</param>
	/// <returns>The key.</returns>
	// The server and the database are part of the key: one connection can be
	// switched to another database, and one factory can hand out connections to
	// different servers.
	static ProcedureKey KeyOf(DbConnection connection, DbQuery definition)
		=> new(connection.DataSource, connection.Database, definition.CommandText);

	static NameSet? Remember(
		object owner,
		ProcedureKey key,
		ImmutableArray<string> names,
		bool afterMiss)
	{
		if (!names.IsDefaultOrEmpty) return ProcedureNameCache.Set(owner, key, names, afterMiss);

		// An empty answer is remembered as unknown for a short while, as a command remembers it (ExpressiveCommandBase.Remember),
		// so a repeat run does not ask the provider again.
		ProcedureNameCache.SetUnknown(owner, key);
		return null;
	}

	/// <summary>
	/// Finds the real names that the derived parameters are matched to.
	/// </summary>
	/// <remarks>
	/// A text definition has the <c>@name</c> markers it found when it was made;
	/// a procedure's names are remembered per source and read again once when they
	/// do not fit what is asked of them.
	/// </remarks>
	/// <param name="definition">The query definition.</param>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="connection">The connection of the run.</param>
	/// <param name="call">The call whose parameters are matched.</param>
	/// <returns>The set of names, or <see langword="null"/> when they are not known.</returns>
	// The same rules as a command (ExpressiveCommandBase). A procedure's names
	// are remembered per source (ProcedureNameCache) for five minutes, so a hit
	// costs no schema lookup.
	static NameSet? Resolve(
		DbQuery definition,
		object source,
		DbConnection connection,
		ParamList call)
	{
		if (definition.Kind != CommandType.StoredProcedure)
			return definition.TextNames;

		object owner = NameCacheOwner(source, connection);
		ProcedureKey key = KeyOf(connection, definition);
		bool found = ProcedureNameCache.TryGet(owner, key, out NameSet? remembered);
		if (found && Fits(remembered, call, definition)) return remembered;

		// Nothing is remembered, or the remembered names did not fit: the
		// procedure may have changed since they were read.
		return Remember(
			owner,
			key,
			ProcedureSchemaNames.Get(connection, definition.CommandText),
			found);
	}

	/// <summary>
	/// Asynchronously finds the real names that the derived parameters are
	/// matched to. See <see cref="Resolve"/>.
	/// </summary>
	/// <param name="definition">The query definition.</param>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="connection">The connection of the run.</param>
	/// <param name="call">The call whose parameters are matched.</param>
	/// <param name="cancellationToken">The token that governs the call.</param>
	/// <returns>A task that represents the asynchronous operation; its result is the set of names, or <see langword="null"/> when they are not known.</returns>
	// Not an async method itself: when the names are remembered (the usual
	// case) it finishes without a state machine; only reading them is
	// asynchronous.
	static ValueTask<NameSet?> ResolveAsync(
		DbQuery definition,
		object source,
		DbConnection connection,
		ParamList call,
		CancellationToken cancellationToken)
	{
		if (definition.Kind != CommandType.StoredProcedure)
			return new(definition.TextNames);

		object owner = NameCacheOwner(source, connection);
		ProcedureKey key = KeyOf(connection, definition);
		bool found = ProcedureNameCache.TryGet(owner, key, out NameSet? remembered);
		return found && Fits(remembered, call, definition)
			? new(remembered)
			: ReadNamesAsync(
				owner,
				key,
				found,
				connection,
				definition,
				cancellationToken);
	}

	static async ValueTask<NameSet?> ReadNamesAsync(
		object owner,
		ProcedureKey key,
		bool afterMiss,
		DbConnection connection,
		DbQuery definition,
		CancellationToken cancellationToken)
		=> Remember(
			owner,
			key,
			await ProcedureSchemaNames
				.GetAsync(
					connection,
					definition.CommandText,
					cancellationToken)
				.ConfigureAwait(false),
			afterMiss);

	/// <summary>
	/// Determines whether the remembered names fit the call.
	/// </summary>
	/// <remarks>
	/// "Not known" (no names) fits anything: each argument keeps its last
	/// member as written.
	/// </remarks>
	/// <param name="remembered">The remembered names, or <see langword="null"/>.</param>
	/// <param name="call">The call.</param>
	/// <param name="definition">The query definition.</param>
	/// <returns>
	/// <see langword="true"/> if the names fit; otherwise,
	/// <see langword="false"/>.
	/// </returns>
	static bool Fits(NameSet? remembered, ParamList call, DbQuery definition)
		=> remembered is null || call.Fits(remembered, definition.CommandText);

	/// <summary>
	/// Runs the query and passes the reader to a function.
	/// </summary>
	/// <typeparam name="TState">The type of the state passed to the function.</typeparam>
	/// <typeparam name="TResult">The type of the result.</typeparam>
	/// <param name="definition">The query definition.</param>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="call">The bound parameters, or <see langword="default"/> for none.</param>
	/// <param name="state">The state to pass to <paramref name="transform"/>.</param>
	/// <param name="transform">The function that reads the reader.</param>
	/// <param name="behavior">The behavior to run the reader with.</param>
	/// <returns>The result of <paramref name="transform"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	public static TResult Run<TState, TResult>(
		DbQuery definition,
		object source,
		ParamList call,
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		using var lease = Lease.Open(source);
		NameSet? names = call.NeedsNames
			? Resolve(definition, source, lease.Connection, call)
			: null;
		using DbCommand command = Make(
			lease.Connection,
			lease.Transaction,
			definition,
			call,
			names);
		using DbDataReader reader = command.ExecuteReader(
			lease.Behavior(behavior));
		return transform(reader, state);
	}

	/// <summary>
	/// Asynchronously runs the query and passes the reader to a function.
	/// </summary>
	/// <typeparam name="TState">The type of the state passed to the function.</typeparam>
	/// <typeparam name="TResult">The type of the result.</typeparam>
	/// <param name="definition">The query definition.</param>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="call">The bound parameters, or <see langword="default"/> for none.</param>
	/// <param name="state">The state to pass to <paramref name="transform"/>.</param>
	/// <param name="transform">The function that reads the reader.</param>
	/// <param name="behavior">The behavior to run the reader with.</param>
	/// <param name="cancellationToken">The token that governs the call.</param>
	/// <returns>A task that represents the asynchronous operation; its result is the result of <paramref name="transform"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	/// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
	public static async ValueTask<TResult> RunAsync<TState, TResult>(
		DbQuery definition,
		object source,
		ParamList call,
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		// Cancelled awaited tasks throw, so this does too.
		cancellationToken.ThrowIfCancellationRequested();

		Lease lease = await Lease
			.OpenAsync(source, cancellationToken)
			.ConfigureAwait(false);
		await using var leaseDisposal = lease.ConfigureAwait(false);
		NameSet? names = call.NeedsNames
			? await ResolveAsync(
					definition,
					source,
					lease.Connection,
					call,
					cancellationToken)
				.ConfigureAwait(false)
			: null;
#if NETSTANDARD2_0
		using DbCommand command = Make(
			lease.Connection,
			lease.Transaction,
			definition,
			call,
			names);
#else
		DbCommand command = Make(
			lease.Connection,
			lease.Transaction,
			definition,
			call,
			names);
		await using var commandDisposal = command.ConfigureAwait(false);
#endif
		CommandBehavior b = lease.Behavior(behavior);
#if NETSTANDARD2_0
		using DbDataReader reader = await command
			.ExecuteReaderAsync(b, cancellationToken)
			.ConfigureAwait(false);
#else
		DbDataReader reader = await command
			.ExecuteReaderAsync(b, cancellationToken)
			.ConfigureAwait(false);
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return await transform(reader, state).ConfigureAwait(false);
	}

	// ---- a scalar and a write: the same run, with no reader ----

	/// <summary>
	/// Asynchronously runs the query and converts its scalar, after the
	/// connection is released.
	/// </summary>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <inheritdoc cref="ExecuteAsync{TValue, TResult}" path="/param"/>
	/// <returns>A task that represents the asynchronous operation; its result is the value.</returns>
	// The rule of ExecuteScalarAsync<T> on a command.
	public static ValueTask<T> ScalarAsync<T>(
		DbQuery definition,
		object source,
		ParamList call,
		CancellationToken cancellationToken)
		=> ExecuteAsync(
			definition,
			source,
			call,
			static (command, token) => command.ExecuteScalarAsync(token),
			static value => RowMap<T>.ConvertScalar(
				value,
				nameof(BoundCallExtensions.ExecuteScalarAsync)),
			cancellationToken);

	/// <summary>Asynchronously runs the write.</summary>
	/// <inheritdoc cref="ExecuteAsync{TValue, TResult}" path="/param"/>
	/// <returns>A task that represents the asynchronous operation; its result is the number of rows affected.</returns>
	public static ValueTask<int> NonQueryAsync(
		DbQuery definition,
		object source,
		ParamList call,
		CancellationToken cancellationToken)
		=> ExecuteAsync(
			definition,
			source,
			call,
			static (command, token) => command.ExecuteNonQueryAsync(token),
			static count => count,
			cancellationToken);

	/// <summary>
	/// Takes the connection, builds the command, runs it, releases the
	/// connection and then converts what the command returned.
	/// </summary>
	/// <param name="definition">The query definition.</param>
	/// <param name="source">The connection, transaction, pool or factory.</param>
	/// <param name="call">The bound parameters, or <see langword="default"/> for none.</param>
	/// <param name="execute">Runs the command.</param>
	/// <param name="convert">Converts what the command returned; it runs with the connection released.</param>
	/// <param name="cancellationToken">The token that governs the call.</param>
	// One state machine for the whole call.
	static async ValueTask<TResult> ExecuteAsync<TValue, TResult>(
		DbQuery definition,
		object source,
		ParamList call,
		Func<DbCommand, CancellationToken, Task<TValue>> execute,
		Func<TValue, TResult> convert,
		CancellationToken cancellationToken)
	{
		// Cancelled awaited tasks throw, so this does too.
		cancellationToken.ThrowIfCancellationRequested();

		TValue value;
		// The run is a block of its own, so everything is released at its end.
		{
			Lease lease = await Lease
				.OpenAsync(source, cancellationToken)
				.ConfigureAwait(false);
			await using var leaseDisposal = lease.ConfigureAwait(false);
			NameSet? names = call.NeedsNames
				? await ResolveAsync(
						definition,
						source,
						lease.Connection,
						call,
						cancellationToken)
					.ConfigureAwait(false)
				: null;
#if NETSTANDARD2_0
			using DbCommand command = Make(
				lease.Connection,
				lease.Transaction,
				definition,
				call,
				names);
#else
			DbCommand command = Make(
				lease.Connection,
				lease.Transaction,
				definition,
				call,
				names);
			await using var commandDisposal = command.ConfigureAwait(false);
#endif
			value = await execute(command, cancellationToken)
				.ConfigureAwait(false);
		}

		return convert(value);
	}

	// The members of IExecuteReader that need no state, made from Run and RunAsync.

	public static void Run(
		DbQuery definition,
		object source,
		ParamList call,
		Action<IDataReader> handler,
		CommandBehavior behavior)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		Run(
			definition,
			source,
			call,
			handler,
			static (reader, h) =>
			{
				h(reader);
				return false;
			},
			behavior);
	}

	public static T Run<T>(
		DbQuery definition,
		object source,
		ParamList call,
		Func<IDataReader, T> transform,
		CommandBehavior behavior)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return Run(
			definition,
			source,
			call,
			transform,
			static (reader, f) => f(reader),
			behavior);
	}

	public static ValueTask RunAsync(
		DbQuery definition,
		object source,
		ParamList call,
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior)
	{
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return ValueTaskHelpers.Discard(RunAsync(
			definition,
			source,
			call,
			handler,
			static async (reader, h) =>
			{
				await h(reader).ConfigureAwait(false);
				return false;
			},
			behavior,
			default));
	}

	public static ValueTask<T> RunAsync<T>(
		DbQuery definition,
		object source,
		ParamList call,
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return RunAsync(
			definition,
			source,
			call,
			transform,
			static (reader, f) => f(reader),
			behavior,
			default);
	}
}
