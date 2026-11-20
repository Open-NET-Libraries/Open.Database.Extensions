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
	internal static InvalidOperationException Unbound()
		=> new("This query was not created by Using(...); a default value cannot be used.");

	static DbConnection Acquire(object source, out DbTransaction? transaction)
	{
		Debug.Assert(source is not null);
		transaction = null;
		DbConnection? connection;
		switch (source)
		{
			case DbConnection direct:
				return direct;
			case DbTransaction tx:
				transaction = tx;
				connection = tx.Connection;
				break;
			case IDbConnectionFactory factory:
				connection = (DbConnection?)factory.Create();
				break;
			default:
				connection = (DbConnection?)((IDbConnectionPool)source).Take();
				break;
		}

		return connection
			?? throw new InvalidOperationException(
				"The connection source supplied no connection.");
	}

	static void Release(
		object source,
		DbConnection connection,
		ConnectionState before)
	{
		try
		{
			if (before == ConnectionState.Closed) connection.Close();
		}
		finally
		{
			// Same order as Acquire. A connection or transaction is the
			// caller's, and is neither disposed nor given back.
			if (source is DbConnection or DbTransaction)
			{
				// Nothing more.
			}
			else if (source is IDbConnectionFactory)
			{
				connection.Dispose();
			}
			else
			{
				((IDbConnectionPool)source).Give(connection);
			}
		}
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

	// An open connection (the usual case) needs no awaiting at all.
	static ValueTask<ConnectionState> OpenIfNeededAsync(
		DbConnection connection,
		CancellationToken cancellationToken)
	{
		ConnectionState state = connection.State;
		return (state & (ConnectionState.Open | ConnectionState.Broken))
			== ConnectionState.Open
			? new ValueTask<ConnectionState>(state)
			: connection.EnsureOpenAsync(cancellationToken);
	}

	internal static TResult Run<TState, TResult>(
		DbQuery definition,
		object source,
		DbQueryCall? call,
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		DbConnection connection = Acquire(source, out DbTransaction? tx);
		ConnectionState before = connection.State;
		try
		{
			connection.EnsureOpen();
			NameSet? names = call is { NeedsNames: true }
				? Resolve(definition, source, connection, call)
				: null;
			using DbCommand command = Make(connection, tx, definition, call, names);
			using DbDataReader reader = command.ExecuteReader(
				before == ConnectionState.Closed
					? behavior | CommandBehavior.CloseConnection
					: behavior);
			return transform(reader, state);
		}
		finally
		{
			Release(source, connection, before);
		}
	}

	internal static async ValueTask<TResult> RunAsync<TState, TResult>(
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

		DbConnection connection = Acquire(source, out DbTransaction? tx);
		ConnectionState before = connection.State;
		try
		{
			await OpenIfNeededAsync(connection, cancellationToken)
				.ConfigureAwait(false);
			NameSet? names = call is { NeedsNames: true }
				? await ResolveAsync(
						definition,
						source,
						connection,
						call,
						cancellationToken)
					.ConfigureAwait(false)
				: null;
			using DbCommand command = Make(connection, tx, definition, call, names);
			CommandBehavior b = before == ConnectionState.Closed
				? behavior | CommandBehavior.CloseConnection
				: behavior;
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
		finally
		{
			Release(source, connection, before);
		}
	}

	// The members of IExecuteReader that need no state, made from the two above.

	internal static void Run(
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

	internal static T Run<T>(
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

	internal static ValueTask RunAsync(
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

	internal static ValueTask<T> RunAsync<T>(
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
