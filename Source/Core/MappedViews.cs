using System.Diagnostics;

namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> over a data reader the caller owns and returns a deferred <see cref="MappedReader{T}"/>.
	/// Nothing runs until a terminal (<c>ToList</c>, <c>FirstOrDefault</c>, and so on) is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The reader belongs to the caller: it is never opened, closed or disposed, and <see cref="IDataReader.NextResult"/> is never called on it.
	/// Rows are read from the current position of the current result set, and the rows read stay read.
	/// </para>
	/// <para>
	/// How <typeparamref name="T"/> is mapped is the same as for
	/// <see cref="Map{T}(IExecuteReader, RowMap{T}?)"/>:
	/// a <see cref="RowMap{T}"/> reads columns whose names differ from the properties,
	/// and the overload without one reads by column name
	/// (exactly, else ignoring case, else ignoring case and underscores, so <c>author_id</c> feeds <c>AuthorId</c>).
	/// A single value such as <see cref="int"/> is read from the first column.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The model type to map the values to.</typeparam>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <param name="map">The prebuilt <see cref="RowMap{T}"/> to read with; <see langword="null"/> reads as <c>Map&lt;T&gt;()</c> does (before 11.0.0-rc.4 it threw).</param>
	/// <returns>A deferred query whose results are mapped to <typeparamref name="T"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(1)]
	public static MappedReader<T> Map<T>(
		this IDataReader reader,
		RowMap<T>? map)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		// No map reads exactly as Map<T>() does:
		// a single value from the first column, a model by name.
		return map is null
			? Map<T>(reader)
			: new MappedReader<T>(Views.ReaderCore(reader, map));
	}

	/// <inheritdoc cref="Map{T}(IDataReader, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <param name="mappings">The (property name, column name) pairs of a new <see cref="RowMap{T}"/>, built on every call; none reads by column name.</param>
	[OverloadResolutionPriority(2)]
	public static MappedReader<T> Map<T>(
		this IDataReader reader,
		params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> mappings.IsEmpty
			? Map<T>(reader)
			: Map(reader, RowMap<T>.From(mappings));

	/// <inheritdoc cref="Map{T}(IDataReader, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <param name="mapping">The one (property name, column name) pair of a new <see cref="RowMap{T}"/>, built on every call.</param>
	[OverloadResolutionPriority(3)]
	public static MappedReader<T> Map<T>(
		this IDataReader reader,
		KeyValuePair<string, string?> mapping)
		=> Map(reader, new RowMap<T>(mapping));

	/// <inheritdoc cref="Map{T}(IDataReader, RowMap{T}?)"/>
	[OverloadResolutionPriority(4)]
	public static MappedReader<T> Map<T>(
		this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		// A single value such as int is read from the first column;
		// any other T maps by column name.
		return FirstColumn<T>.Instance is { } firstColumn
			? new MappedReader<T>(Views.ReaderCore(reader, firstColumn))
			: Map(reader, RowMap<T>.Default);
	}

	/// <summary>
	/// Locks in a custom projection over a data reader the caller owns and returns a deferred <see cref="MappedReader{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The reader belongs to the caller: it is never opened, closed or disposed, and <see cref="IDataReader.NextResult"/> is never called on it.
	/// Rows are read from the current position of the current result set, and the rows read stay read.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type each record is projected to.</typeparam>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <param name="selector">The transform applied to each <see cref="IDataRecord"/>.</param>
	/// <returns>A deferred query whose results are produced by <paramref name="selector"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
	[OverloadResolutionPriority(-1)]
	public static MappedReader<T> Map<T>(
		this IDataReader reader,
		Func<IDataRecord, T> selector)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new MappedReader<T>(
			Views.ReaderCore(reader, new SelectorSource<T>(selector)));
	}

	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> over a command and returns a deferred <see cref="MappedCommand{T}"/>.
	/// Nothing runs until a terminal (<c>ToList</c>, <c>FirstOrDefault</c>, and so on) is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The command belongs to the caller and is never disposed.
	/// The connection is opened only if it is closed, and a connection this call opened is closed (not disposed) when it ends; one that was already open is left open.
	/// Like an expressive command,
	/// a terminal reads through the later result sets after the first, so a failure in a later statement is thrown;
	/// <c>.SingleResult()</c> turns that off.
	/// </para>
	/// <para>
	/// How <typeparamref name="T"/> is mapped is the same as for
	/// <see cref="Map{T}(IExecuteReader, RowMap{T}?)"/>:
	/// a <see cref="RowMap{T}"/> reads columns whose names differ from the properties,
	/// and the overload without one reads by column name
	/// (exactly, else ignoring case, else ignoring case and underscores, so <c>author_id</c> feeds <c>AuthorId</c>).
	/// A single value such as <see cref="int"/> is read from the first column.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The model type to map the values to.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="map">The prebuilt <see cref="RowMap{T}"/> to read with; <see langword="null"/> reads as <c>Map&lt;T&gt;()</c> does (before 11.0.0-rc.4 it threw).</param>
	/// <returns>A deferred query whose results are mapped to <typeparamref name="T"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(1)]
	public static MappedCommand<T> Map<T>(
		this IDbCommand command,
		RowMap<T>? map)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// No map reads exactly as Map<T>() does:
		// a single value from the first column, a model by name.
		return map is null
			? Map<T>(command)
			: new MappedCommand<T>(Views.CommandCore(command, map));
	}

	/// <inheritdoc cref="Map{T}(IDbCommand, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="command">The command to execute.</param>
	/// <param name="mappings">The (property name, column name) pairs of a new <see cref="RowMap{T}"/>, built on every call; none reads by column name.</param>
	[OverloadResolutionPriority(2)]
	public static MappedCommand<T> Map<T>(
		this IDbCommand command,
		params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> mappings.IsEmpty
			? Map<T>(command)
			: Map(command, RowMap<T>.From(mappings));

	/// <inheritdoc cref="Map{T}(IDbCommand, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="command">The command to execute.</param>
	/// <param name="mapping">The one (property name, column name) pair of a new <see cref="RowMap{T}"/>, built on every call.</param>
	[OverloadResolutionPriority(3)]
	public static MappedCommand<T> Map<T>(
		this IDbCommand command,
		KeyValuePair<string, string?> mapping)
		=> Map(command, new RowMap<T>(mapping));

	/// <inheritdoc cref="Map{T}(IDbCommand, RowMap{T}?)"/>
	[OverloadResolutionPriority(4)]
	public static MappedCommand<T> Map<T>(
		this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// A single value such as int is read from the first column;
		// any other T maps by column name.
		return FirstColumn<T>.Instance is { } firstColumn
			? new MappedCommand<T>(Views.CommandCore(command, firstColumn))
			: Map(command, RowMap<T>.Default);
	}

	/// <summary>
	/// Locks in a custom projection over a command and returns a deferred <see cref="MappedCommand{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The command belongs to the caller and is never disposed.
	/// The connection is opened only if it is closed, and a connection this call opened is closed (not disposed) when it ends; one that was already open is left open.
	/// Like an expressive command,
	/// a terminal reads through the later result sets after the first, so a failure in a later statement is thrown;
	/// <c>.SingleResult()</c> turns that off.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type each record is projected to.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="selector">The transform applied to each <see cref="IDataRecord"/>.</param>
	/// <returns>A deferred query whose results are produced by <paramref name="selector"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
	[OverloadResolutionPriority(-1)]
	public static MappedCommand<T> Map<T>(
		this IDbCommand command,
		Func<IDataRecord, T> selector)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new MappedCommand<T>(
			Views.CommandCore(command, new SelectorSource<T>(selector)));
	}

	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> over the rows of a <see cref="DataTable"/> you already have and returns a deferred <see cref="MappedTable{T}"/>.
	/// A shortcut for <c>table.CreateDataReader().Map&lt;T&gt;()</c>, with the difference that the view owns and disposes the readers it creates.
	/// Nothing runs until a terminal (<c>ToList</c>, <c>FirstOrDefault</c>, and so on) is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Each terminal reads the table's current rows through a new <see cref="DataTableReader"/> and disposes it.
	/// The table is not changed or cleared, so the view can be read more than once.
	/// </para>
	/// <para>
	/// How <typeparamref name="T"/> is mapped is the same as for
	/// <see cref="Map{T}(IExecuteReader, RowMap{T}?)"/>:
	/// a <see cref="RowMap{T}"/> reads columns whose names differ from the properties,
	/// and the overload without one reads by column name
	/// (exactly, else ignoring case, else ignoring case and underscores, so <c>author_id</c> feeds <c>AuthorId</c>).
	/// A single value such as <see cref="int"/> is read from the first column.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The model type to map the values to.</typeparam>
	/// <param name="table">The table whose rows are mapped.</param>
	/// <param name="map">The prebuilt <see cref="RowMap{T}"/> to read with; <see langword="null"/> reads as <c>Map&lt;T&gt;()</c> does.</param>
	/// <returns>A deferred query whose results are mapped to <typeparamref name="T"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="table"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(1)]
	public static MappedTable<T> Map<T>(
		this DataTable table,
		RowMap<T>? map)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		Contract.EndContractBlock();

		// No map reads exactly as Map<T>() does:
		// a single value from the first column, a model by name.
		return map is null
			? Map<T>(table)
			: new MappedTable<T>(Views.TableCore(table, map));
	}

	/// <inheritdoc cref="Map{T}(DataTable, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="table">The table whose rows are mapped.</param>
	/// <param name="mappings">The (property name, column name) pairs of a new <see cref="RowMap{T}"/>, built on every call; none reads by column name.</param>
	[OverloadResolutionPriority(2)]
	public static MappedTable<T> Map<T>(
		this DataTable table,
		params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> mappings.IsEmpty
			? Map<T>(table)
			: Map(table, RowMap<T>.From(mappings));

	/// <inheritdoc cref="Map{T}(DataTable, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="table">The table whose rows are mapped.</param>
	/// <param name="mapping">The one (property name, column name) pair of a new <see cref="RowMap{T}"/>, built on every call.</param>
	[OverloadResolutionPriority(3)]
	public static MappedTable<T> Map<T>(
		this DataTable table,
		KeyValuePair<string, string?> mapping)
		=> Map(table, new RowMap<T>(mapping));

	/// <inheritdoc cref="Map{T}(DataTable, RowMap{T}?)"/>
	[OverloadResolutionPriority(4)]
	public static MappedTable<T> Map<T>(
		this DataTable table)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		Contract.EndContractBlock();

		// A single value such as int is read from the first column;
		// any other T maps by column name.
		return FirstColumn<T>.Instance is { } firstColumn
			? new MappedTable<T>(Views.TableCore(table, firstColumn))
			: Map(table, RowMap<T>.Default);
	}

	/// <summary>
	/// Locks in a custom projection over the rows of a <see cref="DataTable"/> you already have and returns a deferred <see cref="MappedTable{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the table is read.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Each terminal reads the table's current rows through a new <see cref="DataTableReader"/> and disposes it.
	/// The table is not changed or cleared, so the view can be read more than once.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type each record is projected to.</typeparam>
	/// <param name="table">The table whose rows are projected.</param>
	/// <param name="selector">The transform applied to each <see cref="IDataRecord"/>.</param>
	/// <returns>A deferred query whose results are produced by <paramref name="selector"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="table"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
	[OverloadResolutionPriority(-1)]
	public static MappedTable<T> Map<T>(
		this DataTable table,
		Func<IDataRecord, T> selector)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new MappedTable<T>(
			Views.TableCore(table, new SelectorSource<T>(selector)));
	}
}

// Builds the core a view runs over.
// A caller's reader is read from where it stands and is never read through,
// so it takes the single-result path of the core,
// which never calls NextResult.
// A command takes the same path as an expressive command.
static class Views
{
	internal static MappedQueryCore<T> ReaderCore<T>(
		IDataReader reader,
		IRowSource<T> source)
		=> new(new ReaderExecutor(reader), source, 0, -1, true);

	internal static MappedQueryCore<T> CommandCore<T>(
		IDbCommand command,
		IRowSource<T> source)
		=> new(new CommandExecutor(command), source, 0, -1, false);

	internal static MappedQueryCore<T> TableCore<T>(
		DataTable table,
		IRowSource<T> source)
		=> new(new TableExecutor(table), source, 0, -1, true);
}

// The two executors let MappedQueryCore<T> run over a reader or a raw command.
// The core only uses the two state forms; the rest are derived from them.
abstract class ViewExecutor : IExecuteReader
{
	public CancellationToken CancellationToken => default;

	public abstract TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default);

	public abstract ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken);

	public void ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		Debug.Assert(handler is not null);
		ExecuteReader(handler, static (reader, h) =>
		{
			h(reader);
			return false;
		}, behavior);
	}

	public T ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		Debug.Assert(transform is not null);
		return ExecuteReader(
			transform,
			static (reader, f) => f(reader),
			behavior);
	}

	public ValueTask ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		Debug.Assert(handler is not null);
		return ValueTaskHelpers.Discard(ExecuteReaderAsync(
			handler,
			static async (reader, h) =>
			{
				await h(reader).ConfigureAwait(false);
				return false;
			},
			behavior));
	}

	public ValueTask<T> ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		Debug.Assert(transform is not null);
		return ExecuteReaderAsync(
			transform,
			static (reader, f) => f(reader),
			behavior);
	}

	public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> ExecuteReaderAsync(state, transform, behavior, default);
}

// The caller's reader: handed to the transform as it is.
// It is never opened, closed or disposed here,
// and NextResult is never called on it.
sealed class ReaderExecutor(IDataReader reader) : ViewExecutor
{
	public override TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> transform(reader, state);

	public override ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
	{
		// A cancelled token gives a cancelled task, not an exception at the call.
		if (cancellationToken.IsCancellationRequested)
		{
			return new ValueTask<TResult>(
				Task.FromCanceled<TResult>(cancellationToken));
		}

		return transform(reader, state);
	}
}

// The caller's command, run as an expressive command runs:
// the connection is opened only if it is closed
// and closed again (not disposed) only if it was closed when the call began.
// The command is never disposed.
sealed class CommandExecutor(IDbCommand command) : ViewExecutor
{
	public override TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		IDbConnection? connection = command.Connection;
		ConnectionState before
			= connection?.State ?? ConnectionState.Closed;
		try
		{
			connection?.EnsureOpen();
			using IDataReader reader = command.ExecuteReader(
				before == ConnectionState.Closed
					? behavior | CommandBehavior.CloseConnection
					: behavior);
			return transform(reader, state);
		}
		finally
		{
			if (before == ConnectionState.Closed) connection?.Close();
		}
	}

	public override async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		IDbConnection? connection = command.Connection;
		ConnectionState before
			= connection?.State ?? ConnectionState.Closed;
		try
		{
			// An open connection (the usual case) needs no awaiting at all.
			if (connection is not null
				&& (before & (ConnectionState.Open | ConnectionState.Broken))
					!= ConnectionState.Open)
			{
				await connection.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
			}

			CommandBehavior b = before == ConnectionState.Closed
				? behavior | CommandBehavior.CloseConnection
				: behavior;

			if (command is DbCommand dbCommand)
			{
#if NETSTANDARD2_0
				using DbDataReader dbReader = await dbCommand
					.ExecuteReaderAsync(b, cancellationToken)
					.ConfigureAwait(false);
#else
				DbDataReader dbReader = await dbCommand
					.ExecuteReaderAsync(b, cancellationToken)
					.ConfigureAwait(false);
				await using var dbReaderDisposal = dbReader.ConfigureAwait(false);
#endif
				return await transform(dbReader, state).ConfigureAwait(false);
			}

			using IDataReader reader = command.ExecuteReader(b);
			return await transform(reader, state).ConfigureAwait(false);
		}
		finally
		{
			if (before == ConnectionState.Closed) connection?.Close();
		}
	}
}

// A DataTable the caller has: each call reads it through a new
// DataTableReader, which is disposed when the call is done.
// There is no connection, and a table has one result set,
// so NextResult is never called.
// The rows are in memory, so the asynchronous form completes synchronously.
sealed class TableExecutor(DataTable table) : ViewExecutor
{
	public override TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		using DataTableReader reader = table.CreateDataReader();
		return transform(reader, state);
	}

	public override ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
	{
		// A cancelled token gives a cancelled task, not an exception at the call.
		if (cancellationToken.IsCancellationRequested)
		{
			return new ValueTask<TResult>(
				Task.FromCanceled<TResult>(cancellationToken));
		}

		return ReadAsync(state, transform);
	}

	async ValueTask<TResult> ReadAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform)
	{
#if NETSTANDARD2_0
		using DataTableReader reader = table.CreateDataReader();
#else
		DataTableReader reader = table.CreateDataReader();
		await using var readerDisposal = reader.ConfigureAwait(false);
#endif
		return await transform(reader, state).ConfigureAwait(false);
	}
}

/// <summary>
/// A deferred, type-locked read over a data reader the caller owns.
/// Terminals are methods here, with more as extensions on <see cref="IMappedQuery{T}"/>.
/// </summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
/// <remarks>
/// <para>
/// The reader belongs to the caller.
/// A terminal never opens, closes or disposes it,
/// and never calls <see cref="IDataReader.NextResult"/> on it:
/// it reads the rows of the current result set from the reader's current position and stops,
/// leaving the reader open and positioned after that set's last row.
/// A caller who wants the next result set calls <c>NextResult</c> and maps again.
/// The asynchronous terminals read the rows with <c>Read()</c>,
/// as an expressive command does by default.
/// </para>
/// <para>
/// <c>Skip</c> and <c>Take</c> return the types of <see cref="MappedQuery{T}"/>;
/// the <c>SingleResult</c> they carry changes nothing here, since a reader view never reads through later result sets.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedCommand{T}"/>
public readonly record struct MappedReader<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	readonly MappedQueryCore<T> _core;

	internal MappedReader(MappedQueryCore<T> core)
		=> _core = core;

	/// <inheritdoc cref="MappedQuery{T}.Skip(int)"/>
	public MappedQueryWithSkip<T> Skip(int count)
		=> new(_core.WithSkip(count));

	/// <inheritdoc cref="MappedQuery{T}.Take(int)"/>
	public MappedQueryWithTake<T> Take(int count)
		=> new(_core.WithTake(count));

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public TResult Read<TResult>(
		Func<IEnumerable<T>, TResult> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public ValueTask ReadAsync(
		Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(
		Func<
			IAsyncEnumerable<T>,
			CancellationToken,
			ValueTask<TResult>> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToList"/>
	public List<T> ToList()
		=> _core.ToList();

	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToListAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToArray"/>
	public T[] ToArray()
		=> _core.ToArray();

	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefault"/>
	public T? FirstOrDefault()
		=> _core.FirstOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefaultAsync"/>
	public ValueTask<T?> FirstOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefault"/>
	public T? SingleOrDefault()
		=> _core.SingleOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefaultAsync"/>
	public ValueTask<T?> SingleOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.First"/>
	public T First()
		=> _core.First();

	/// <inheritdoc cref="MappedQuery{T}.FirstAsync"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.Single"/>
	public T Single()
		=> _core.Single();

	/// <inheritdoc cref="MappedQuery{T}.SingleAsync"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArray"/>
	public ImmutableArray<T> ToImmutableArray()
		=> _core.ToImmutableArray();

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToImmutableArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey}"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey, TValue}"/>
	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSet"/>
	public HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer = null,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);
}

/// <summary>
/// A deferred, type-locked query over a raw <see cref="IDbCommand"/>.
/// Terminals are methods here, with more as extensions on <see cref="IMappedQuery{T}"/>.
/// </summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
/// <remarks>
/// <para>
/// The command belongs to the caller and is never disposed.
/// Each terminal executes it again.
/// The connection is opened only if it is closed,
/// and a connection the terminal opened is closed (<c>Close</c>, not <c>Dispose</c>) when it ends;
/// one that was already open is left open.
/// The asynchronous terminals open and execute asynchronously when the command is a <see cref="DbCommand"/>,
/// then read the rows with <c>Read()</c>, as an expressive command does by default.
/// </para>
/// <para>
/// A terminal returns the rows of the first result set only,
/// then reads through the later sets without reading their rows,
/// so a failure in a later statement is thrown as the provider raised it;
/// <see cref="SingleResult"/> turns that off.
/// See <see cref="MappedQuery{T}"/> for what a read-through does and does not do.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedReader{T}"/>
public readonly record struct MappedCommand<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	readonly MappedQueryCore<T> _core;

	internal MappedCommand(MappedQueryCore<T> core)
		=> _core = core;

	/// <inheritdoc cref="MappedQuery{T}.Skip(int)"/>
	public MappedQueryWithSkip<T> Skip(int count)
		=> new(_core.WithSkip(count));

	/// <inheritdoc cref="MappedQuery{T}.Take(int)"/>
	public MappedQueryWithTake<T> Take(int count)
		=> new(_core.WithTake(count));

	/// <inheritdoc cref="MappedQuery{T}.SingleResult"/>
	public MappedCommand<T> SingleResult()
		=> new(_core.WithSingleResult());

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public TResult Read<TResult>(
		Func<IEnumerable<T>, TResult> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public ValueTask ReadAsync(
		Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(
		Func<
			IAsyncEnumerable<T>,
			CancellationToken,
			ValueTask<TResult>> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToList"/>
	public List<T> ToList()
		=> _core.ToList();

	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToListAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToArray"/>
	public T[] ToArray()
		=> _core.ToArray();

	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefault"/>
	public T? FirstOrDefault()
		=> _core.FirstOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefaultAsync"/>
	public ValueTask<T?> FirstOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefault"/>
	public T? SingleOrDefault()
		=> _core.SingleOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefaultAsync"/>
	public ValueTask<T?> SingleOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.First"/>
	public T First()
		=> _core.First();

	/// <inheritdoc cref="MappedQuery{T}.FirstAsync"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.Single"/>
	public T Single()
		=> _core.Single();

	/// <inheritdoc cref="MappedQuery{T}.SingleAsync"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArray"/>
	public ImmutableArray<T> ToImmutableArray()
		=> _core.ToImmutableArray();

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToImmutableArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey}"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey, TValue}"/>
	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSet"/>
	public HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer = null,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);
}

/// <summary>
/// A deferred, type-locked read over the rows of a <see cref="DataTable"/>.
/// Terminals are methods here, with more as extensions on <see cref="IMappedQuery{T}"/>.
/// </summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
/// <remarks>
/// <para>
/// Each terminal reads the table's current rows through a new <see cref="DataTableReader"/> and disposes it.
/// The table itself is not changed or cleared, so the view can be read more than once.
/// The view never touches a connection and never calls <see cref="IDataReader.NextResult"/>.
/// The asynchronous terminals complete synchronously: the rows are already in memory.
/// </para>
/// <para>
/// <c>Skip</c> and <c>Take</c> return the types of <see cref="MappedQuery{T}"/>;
/// the <c>SingleResult</c> they carry changes nothing here, since a table has one result set.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedReader{T}"/>
public readonly record struct MappedTable<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	readonly MappedQueryCore<T> _core;

	internal MappedTable(MappedQueryCore<T> core)
		=> _core = core;

	/// <inheritdoc cref="MappedQuery{T}.Skip(int)"/>
	public MappedQueryWithSkip<T> Skip(int count)
		=> new(_core.WithSkip(count));

	/// <inheritdoc cref="MappedQuery{T}.Take(int)"/>
	public MappedQueryWithTake<T> Take(int count)
		=> new(_core.WithTake(count));

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public TResult Read<TResult>(
		Func<IEnumerable<T>, TResult> handler)
		=> _core.Read(handler);

	/// <inheritdoc />
	public ValueTask ReadAsync(
		Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(
		Func<
			IAsyncEnumerable<T>,
			CancellationToken,
			ValueTask<TResult>> handler,
		CancellationToken cancellationToken)
		=> _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToList"/>
	public List<T> ToList()
		=> _core.ToList();

	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToListAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToArray"/>
	public T[] ToArray()
		=> _core.ToArray();

	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefault"/>
	public T? FirstOrDefault()
		=> _core.FirstOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefaultAsync"/>
	public ValueTask<T?> FirstOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefault"/>
	public T? SingleOrDefault()
		=> _core.SingleOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefaultAsync"/>
	public ValueTask<T?> SingleOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleOrDefaultAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.First"/>
	public T First()
		=> _core.First();

	/// <inheritdoc cref="MappedQuery{T}.FirstAsync"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.Single"/>
	public T Single()
		=> _core.Single();

	/// <inheritdoc cref="MappedQuery{T}.SingleAsync"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArray"/>
	public ImmutableArray<T> ToImmutableArray()
		=> _core.ToImmutableArray();

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToImmutableArrayAsync(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey}"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey, TValue}"/>
	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSet"/>
	public HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer = null,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);
}
