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
	// which Dispose does (so a run is `using Lease lease = Lease.Take(...)`).
	// A connection or a transaction is the caller's: it is neither disposed nor
	// given back. A transaction's connection is also never opened or closed
	// here. A connection that needed opening (it was closed, or broken) is
	// closed again, even when the run throws; one from a factory is disposed
	// and one from a pool is given back, after that.
	public readonly struct Lease : IDisposable
	{
		readonly object _source;
		readonly bool _opened;

		Lease(
			object source,
			DbConnection connection,
			DbTransaction? transaction,
			bool opened)
		{
			_source = source;
			Connection = connection;
			Transaction = transaction;
			_opened = opened;
		}

		public DbConnection Connection { get; }

		public DbTransaction? Transaction { get; }

		public static Lease Take(object source)
		{
			Debug.Assert(source is not null);
			DbConnection? connection;
			switch (source)
			{
				case DbConnection direct:
					return new(
						source,
						direct,
						null,
						ConnectionExtensions.NeedsOpen(direct.State));
				case DbTransaction tx:
					return new(source, OpenConnectionOf(tx), tx, false);
				case IDbConnectionFactory factory:
					connection = AsDbConnection(factory.Create(), factory);
					break;
				default:
					connection = AsDbConnection(
						((IDbConnectionPool)source).Take(),
						source);
					break;
			}

			return connection is null
				? throw new InvalidOperationException(
					"The connection source supplied no connection.")
				: new(
					source,
					connection,
					null,
					ConnectionExtensions.NeedsOpen(connection.State));
		}

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
				$"The connection source supplied a {supplied.GetType()}, which is not a DbConnection.");
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

		public void Open()
		{
			if (_opened) Connection.EnsureOpen();
		}

		// An open connection (the usual case) needs no awaiting at all.
		public ValueTask OpenAsync(CancellationToken cancellationToken)
			=> _opened
				? ValueTaskHelpers.Discard(
					Connection.EnsureOpenAsync(cancellationToken))
				: default;

		public CommandBehavior Behavior(CommandBehavior behavior)
			=> _opened
				? behavior | CommandBehavior.CloseConnection
				: behavior;

		public void Dispose()
		{
			try
			{
				if (_opened) Connection.Close();
			}
			finally
			{
				if (_source is IDbConnectionFactory)
					Connection.Dispose();
				else if (_source is IDbConnectionPool pool)
					pool.Give(Connection);
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

		using Lease lease = Lease.Take(source);
		lease.Open();
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

		using Lease lease = Lease.Take(source);
		await lease.OpenAsync(cancellationToken).ConfigureAwait(false);
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
