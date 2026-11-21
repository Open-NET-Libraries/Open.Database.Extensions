using System.Diagnostics;

namespace Open.Database.Extensions;

// The one run path of a DbQuery: take the connection, open it if it is not
// open, build one DbCommand with the bound parameters, run the caller's
// function on the reader, release the connection.
// A connection that was closed when taken is closed again (never disposed
// unless it came from a factory); one that was open stays open.
// Open happens before the command is created, as some providers need.
static class DbQueryRunner
{
	public static InvalidOperationException Unbound()
		=> new("This query was not created by Using(...); a default value cannot be used.");

	// What one run holds: the connection, and what is owed when the run ends,
	// which Dispose does (so a run is `using Lease lease = Lease.Open(...)`).
	// It is a ConnectionScope, so it ends the way the source calls for: a
	// connection of the caller's is closed again if the run opened it (and is
	// never disposed), one from a factory is disposed, one from a pool is given
	// back (and what that means is the pool's, as in pool.Open). A transaction's
	// connection is the transaction's: it is never opened, closed or given back
	// here. All of this holds when the run throws.
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

		// What a source supplies, before anything is opened.
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

		public static Lease Open(object source)
		{
			Taken taken = Take(source);
			return taken.Transaction is { } transaction
				? new(default, taken.Connection, transaction)
				: Of(Open(taken), taken);
		}

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

		// Opened if it needs to be, after the supplied type is known to be right:
		// a source that gave the wrong kind of connection never has it opened.
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

		// A source that supplies something that is not a DbConnection: it is
		// let go the way its source lets go of a connection (disposed for a
		// factory, given back to a pool), not leaked, and the run fails with
		// the type named. Null is left to the caller's check.
		static DbConnection? AsDbConnection(IDbConnection? supplied, object source)
		{
			if (supplied is null or DbConnection)
				return supplied as DbConnection;

			if (source is not IDbConnectionFactory && source is IDbConnectionPool pool)
				pool.Give(supplied);
			else
				supplied.Dispose();

			throw new InvalidOperationException(
				$"The connection source supplied a {supplied.GetType()}, "
				+ "which is not a DbConnection.");
		}

		// A transaction is begun on an open connection and holds it: it is
		// neither opened nor reopened here.
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

		// A transaction's connection is never opened here, so it adds nothing.
		public CommandBehavior Behavior(CommandBehavior behavior)
			=> Transaction is null
				? _scope.ReaderBehavior(behavior)
				: behavior;

		public void Dispose() => _scope.Dispose();

		// For `await using var d = lease.ConfigureAwait(false)`: the scope's own,
		// which does not box.
		public ConfiguredConnectionScope<DbConnection> ConfigureAwait(
			bool continueOnCapturedContext)
			=> _scope.ConfigureAwait(continueOnCapturedContext);
	}
	static DbCommand Make(
		DbConnection connection,
		DbTransaction? transaction,
		DbQuery definition,
		DbQueryCall? call,
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
			call?.BindAll(command, definition, names, call);
			return command;
		}
		catch
		{
			command.Dispose();
			throw;
		}
	}

	// ---- the real names of derived parameters ----
	// The same rules as a command (ExpressiveCommandBase): a text definition
	// has the @name markers it found when it was made; a procedure's names
	// are remembered per source (ProcedureNameCache) for five minutes, and are
	// read again once when they do not fit what is asked of them, so a hit
	// costs no schema lookup.

	// Remembered names belong to the object connections come from: the factory
	// or pool (not the adapter AsPool makes around a factory), or the
	// connection itself.
	static object NameCacheOwner(object source, DbConnection connection)
		=> source switch
		{
			DbTransaction => connection,
			IConnectionIdentity adapter => adapter.Identity,
			_ => source,
		};

	// The server and the database are part of the key: one connection can be
	// switched to another database, and one factory can hand out connections
	// to different servers.
	static ProcedureKey KeyOf(DbConnection connection, DbQuery definition)
		=> new(connection.DataSource, connection.Database, definition.CommandText);

	static NameSet? Remember(
		object owner,
		ProcedureKey key,
		ImmutableArray<string> names,
		bool afterMiss)
		=> names.IsDefaultOrEmpty
			? null
			: ProcedureNameCache.Set(owner, key, names, afterMiss);

	static NameSet? Resolve(
		DbQuery definition,
		object source,
		DbConnection connection,
		DbQueryCall call)
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

	// Not an async method itself: when the names are remembered (the usual
	// case) it finishes without a state machine; only reading them is async.
	static ValueTask<NameSet?> ResolveAsync(
		DbQuery definition,
		object source,
		DbConnection connection,
		DbQueryCall call,
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

	// "Not known" (no names) fits anything: each argument keeps its last
	// member as written.
	static bool Fits(NameSet? remembered, DbQueryCall call, DbQuery definition)
		=> remembered is null || call.Fits(remembered, definition.CommandText);

	public static TResult Run<TState, TResult>(
		DbQuery definition,
		object source,
		DbQueryCall? call,
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		using Lease lease = Lease.Open(source);
		NameSet? names = call is { NeedsNames: true }
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

	public static async ValueTask<TResult> RunAsync<TState, TResult>(
		DbQuery definition,
		object source,
		DbQueryCall? call,
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
		NameSet? names = call is { NeedsNames: true }
			? await ResolveAsync(
					definition,
					source,
					lease.Connection,
					call,
					cancellationToken)
				.ConfigureAwait(false)
			: null;
		using DbCommand command = Make(
			lease.Connection,
			lease.Transaction,
			definition,
			call,
			names);
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
	// The members of IExecuteReader that need no state, made from the two above.

	public static void Run(
		DbQuery definition,
		object source,
		DbQueryCall? call,
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
		DbQueryCall? call,
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
		DbQueryCall? call,
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
		DbQueryCall? call,
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
