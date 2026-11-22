using Open.Database.Extensions.Synchronous;

using System.Diagnostics;

namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> over a data
	/// reader the caller owns and returns a deferred
	/// <see cref="MappedReader{T}"/>.
	/// Nothing runs until a terminal, such as
	/// <see cref="MappedReader{T}.ToListAsync(CancellationToken)"/>, is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The reader belongs to the caller: it is never opened, closed or disposed,
	/// and <see cref="IDataReader.NextResult"/> is never called on it.
	/// Rows are read from the current position of the current result set, and
	/// the rows read stay read.
	/// </para>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-how']/*" />
	/// </remarks>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-overload']/*" />
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
	/// Locks in a custom projection over a data reader the caller owns and
	/// returns a deferred <see cref="MappedReader{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The reader belongs to the caller: it is never opened, closed or disposed,
	/// and <see cref="IDataReader.NextResult"/> is never called on it.
	/// Rows are read from the current position of the current result set, and
	/// the rows read stay read.
	/// </para>
	/// </remarks>
	/// <param name="reader">The reader to read from, positioned on the result set to map.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-selector']/*" />
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
	/// Locks in a mapping to model type <typeparamref name="T"/> over a command
	/// and returns a deferred <see cref="MappedCommand{T}"/>.
	/// Nothing runs until a terminal, such as
	/// <see cref="MappedCommand{T}.ToListAsync(CancellationToken)"/>, is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The command belongs to the caller and is never disposed.
	/// The connection is opened only if it is closed, and a connection this call
	/// opened is closed (not disposed) when it ends; one that was already open
	/// is left open.
	/// Like an expressive command, a terminal reads through the later result
	/// sets after the first, so a failure in a later statement is thrown;
	/// <c>command.SingleResult().Map&lt;T&gt;()</c> turns that off
	/// (<see cref="MappedCommand{T}.SingleResult"/> after <c>Map</c> does too).
	/// </para>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-how']/*" />
	/// </remarks>
	/// <param name="command">The command to execute.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-overload']/*" />
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
	/// Locks in a custom projection over a command and returns a deferred
	/// <see cref="MappedCommand{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The command belongs to the caller and is never disposed.
	/// The connection is opened only if it is closed, and a connection this call
	/// opened is closed (not disposed) when it ends; one that was already open
	/// is left open.
	/// Like an expressive command, a terminal reads through the later result
	/// sets after the first, so a failure in a later statement is thrown;
	/// <c>command.SingleResult().Map&lt;T&gt;()</c> turns that off
	/// (<see cref="MappedCommand{T}.SingleResult"/> after <c>Map</c> does too).
	/// </para>
	/// </remarks>
	/// <param name="command">The command to execute.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-selector']/*" />
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
	/// Locks in a mapping to model type <typeparamref name="T"/> over the rows of
	/// a <see cref="DataTable"/> you already have and returns a deferred
	/// <see cref="MappedTable{T}"/>.
	/// Nothing runs until a terminal, such as
	/// <see cref="MappedTable{T}.ToListAsync(CancellationToken)"/>, is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A shortcut for <c>table.CreateDataReader().Map&lt;T&gt;()</c>, with the
	/// difference that the view owns and disposes the readers it creates.
	/// Each terminal reads the table's current rows through a new
	/// <see cref="DataTableReader"/> and disposes it.
	/// The table is not changed or cleared, so the view can be read more than
	/// once.
	/// </para>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-how']/*" />
	/// </remarks>
	/// <param name="table">The table whose rows are mapped.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-overload']/*" />
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
	/// Locks in a custom projection over the rows of a <see cref="DataTable"/>
	/// you already have and returns a deferred <see cref="MappedTable{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the table is read.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Each terminal reads the table's current rows through a new
	/// <see cref="DataTableReader"/> and disposes it.
	/// The table is not changed or cleared, so the view can be read more than
	/// once.
	/// </para>
	/// </remarks>
	/// <param name="table">The table whose rows are projected.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-selector']/*" />
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

/// <summary>
/// Builds the core a view runs over.
/// </summary>
/// <remarks>
/// A caller's reader is read from where it stands and is never read through,
/// so it takes the single-result path of the core, which never calls
/// <see cref="IDataReader.NextResult"/>.
/// A command takes the same path as an expressive command.
/// A command that came through <c>SingleResult()</c> is unwrapped here:
/// the query runs against the command itself with the core's
/// <c>singleResult</c> flag set.
/// </remarks>
static class Views
{
	internal static MappedQueryCore<T> QueryCore<T>(
		IExecuteReader command,
		IRowSource<T> source)
		=> command is SingleResultReader single
			? NewCore(single.Command, source, true, AsyncReads(single.Command))
			: NewCore(command, source, false, AsyncReads(command));

	/// <summary>
	/// Gets the command's own choice, made by <c>cmd.AsyncReads()</c> before
	/// <c>Map</c>, which is staged into the view.
	/// </summary>
	/// <param name="command">The command.</param>
	/// <returns><see langword="true"/> if the command reads its rows asynchronously; otherwise, <see langword="false"/>.</returns>
	static bool AsyncReads(IExecuteReader command)
		=> command is IExecuteReaderAsync { UseAsyncRead: true };

	internal static MappedQueryCore<T> ReaderCore<T>(
		IDataReader reader,
		IRowSource<T> source)
		=> NewCore(new ReaderExecutor(reader), source, true, false);

	internal static MappedQueryCore<T> CommandCore<T>(
		IDbCommand command,
		IRowSource<T> source)
		=> command is SingleResultDbCommand single
			? NewCore(new CommandExecutor(single.Command), source, true, false)
			: NewCore(new CommandExecutor(command), source, false, false);

	internal static MappedQueryCore<T> TableCore<T>(
		DataTable table,
		IRowSource<T> source)
		=> NewCore(new TableExecutor(table), source, true, false);

	/// <summary>
	/// Makes a query core: the one door every query is made through.
	/// </summary>
	/// <remarks>
	/// It is the one place a default pool configuration is chosen: when the query
	/// is made, before any terminal runs.
	/// Everything after this (a terminal, <c>UsePool</c>, a collector) uses the
	/// query's own configuration and never picks one.
	/// </remarks>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
	/// <param name="command">The command the rows are read from.</param>
	/// <param name="source">Reads and maps one row.</param>
	/// <param name="singleResult"><see langword="true"/> to stop after the first result set and not read through the later ones; otherwise, <see langword="false"/>.</param>
	/// <param name="asyncReads"><see langword="true"/> to read the rows of an asynchronous terminal with <c>ReadAsync</c>; otherwise, <see langword="false"/>.</param>
	/// <returns>The new query core, with the default configuration for <typeparamref name="T"/>.</returns>
	static MappedQueryCore<T> NewCore<T>(
		IExecuteReader command,
		IRowSource<T> source,
		bool singleResult,
		bool asyncReads)
		=> new(
			command,
			source,
			0,
			-1,
			singleResult,
			asyncReads,
			Defaults<T>.Config);

	/// <summary>
	/// Holds the shared array pool, the shipped sizes and the default bin, per
	/// row type.
	/// </summary>
	/// <remarks>
	/// Private to <see cref="Views"/>: nothing else can ask for it.
	/// </remarks>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
	static class Defaults<T>
	{
		/// <summary>
		/// The default configuration for the row type.
		/// </summary>
		/// <remarks>
		/// It asks for no clearing choice (<see langword="null"/>): the
		/// configuration resolves it, once, from the app-wide default and the
		/// reference-row floor.
		/// </remarks>
		public static readonly ChunkConfig<T> Config = new(
			ArrayPool<T>.Shared,
			null,
			ChunkSizes.Default,
			RecycleBin.Default,
			ChunkConfig<T>.DefaultDeferFromClearedBytes);
	}
}
/// <summary>
/// Lets <see cref="MappedQueryCore{T}"/> run over a caller's reader, a raw
/// command or a <see cref="DataTable"/>.
/// </summary>
/// <remarks>
/// The core only uses the two state forms of <c>ExecuteReader</c>; the rest
/// are derived from them.
/// </remarks>
abstract class ViewExecutor : IExecuteReaderSync
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

/// <summary>
/// Runs a query over the caller's reader, handing it to the transform as it
/// is.
/// </summary>
/// <remarks>
/// The reader is never opened, closed or disposed here, and
/// <see cref="IDataReader.NextResult"/> is never called on it.
/// </remarks>
/// <param name="reader">The caller's reader.</param>
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

/// <summary>
/// Runs a query over the caller's command, as an expressive command runs.
/// </summary>
/// <remarks>
/// The connection is opened only if it is closed and closed again (not
/// disposed) only if it was closed when the call began.
/// The command is never disposed.
/// </remarks>
/// <param name="command">The caller's command.</param>
sealed class CommandExecutor(IDbCommand command) : ViewExecutor
{
	public override TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		// No connection: nothing to open, and the command says so itself (the
		// provider throws InvalidOperationException), as it always has.
		using var scope = command.EnsureOpenScope();
		using IDataReader reader = command.ExecuteReader(scope.ReaderBehavior(behavior));
		return transform(reader, state);
	}

	public override async ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		// No connection: nothing to open, and the command says so itself (the
		// provider throws InvalidOperationException), as it always has.
		var scope = await command
			.EnsureOpenScopeAsync(cancellationToken)
			.ConfigureAwait(false);
		await using var scopeDisposal = scope.ConfigureAwait(false);

		CommandBehavior b = scope.ReaderBehavior(behavior);

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
}

/// <summary>
/// Runs a query over a <see cref="DataTable"/> the caller has.
/// </summary>
/// <remarks>
/// Each call reads the table through a new <see cref="DataTableReader"/>,
/// which is disposed when the call is done.
/// There is no connection, and a table has one result set, so
/// <see cref="IDataReader.NextResult"/> is never called.
/// The rows are in memory, so the asynchronous form completes synchronously.
/// </remarks>
/// <param name="table">The caller's table.</param>
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
/// Terminals are methods here.
/// </summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
/// <remarks>
/// <para>
/// The reader belongs to the caller.
/// A terminal never opens, closes or disposes it,
/// and never calls <see cref="IDataReader.NextResult"/> on it:
/// it reads the rows of the current result set from the reader's current position and stops,
/// leaving the reader open and positioned after that set's last row.
/// A caller who wants the next result set calls <see cref="IDataReader.NextResult"/> and maps again.
/// The asynchronous terminals read the rows with <see cref="IDataReader.Read"/>,
/// as an expressive command does by default.
/// </para>
/// <para>
/// <see cref="Skip(int)"/> and <see cref="Take(int)"/> return the types of <see cref="MappedQuery{T}"/>;
/// the <see cref="MappedQuery{T}.SingleResult"/> they carry changes nothing here, since a reader view never reads through later result sets.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedCommand{T}"/>
public readonly record struct MappedReader<T>
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

	/// <inheritdoc cref="ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey>(keySelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
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

	/// <inheritdoc cref="ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey, TValue>(keySelector, valueSelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSet"/>
	public HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="ToHashSetAsync(IEqualityComparer{T}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		CancellationToken cancellationToken = default)
		=> ToHashSetAsync(null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync(IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);
}

/// <summary>
/// A deferred, type-locked query over a raw <see cref="IDbCommand"/>.
/// Terminals are methods here.
/// </summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
/// <remarks>
/// <para>
/// The command belongs to the caller and is never disposed.
/// Each terminal executes it again.
/// The connection is opened only if it is closed,
/// and a connection the terminal opened is closed (<see cref="IDbConnection.Close"/>, not <see cref="IDisposable.Dispose"/>) when it ends;
/// one that was already open is left open.
/// The asynchronous terminals open and execute asynchronously when the command is a <see cref="DbCommand"/>,
/// then read the rows with <see cref="IDataReader.Read"/>, as an expressive command does by default.
/// </para>
/// <para>
/// A terminal returns the rows of the first result set only,
/// then reads through the later sets without reading their rows,
/// so a failure in a later statement is thrown as the provider raised it;
/// <c>command.SingleResult().Map&lt;T&gt;()</c> turns that off, and so does <see cref="SingleResult"/> after <c>Map</c>.
/// See <see cref="MappedQuery{T}"/> for what a read-through does and does not do.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedReader{T}"/>
public readonly record struct MappedCommand<T>
	: IMappedQuery<T>
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

	/// <inheritdoc cref="MappedQuery{T}.AsyncReads"/>
	public MappedCommand<T> AsyncReads()
		=> new(_core.WithAsyncReads());

	/// <inheritdoc cref="MappedQuery{T}.UsePool(ArrayPool{T}, bool)"/>
	public MappedCommand<T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
		=> new(_core.WithPool(pool, clearOnReturn));

	/// <inheritdoc cref="MappedQuery{T}.WithConfig(ChunkConfig{T})"/>
	internal MappedCommand<T> WithConfig(ChunkConfig<T> config)
		=> new(_core.WithConfig(config));

	internal List<T> ToList()
		=> _core.ToList();

	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToListAsync(cancellationToken);

	internal T[] ToArray()
		=> _core.ToArray();

	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToArrayAsync(cancellationToken);

	internal T? FirstOrDefault()
		=> _core.FirstOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.FirstOrDefaultAsync"/>
	public ValueTask<T?> FirstOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstOrDefaultAsync(cancellationToken);

	internal T? SingleOrDefault()
		=> _core.SingleOrDefault();

	/// <inheritdoc cref="MappedQuery{T}.SingleOrDefaultAsync"/>
	public ValueTask<T?> SingleOrDefaultAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleOrDefaultAsync(cancellationToken);

	internal T First()
		=> _core.First();

	/// <inheritdoc cref="MappedQuery{T}.FirstAsync"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);

	internal T Single()
		=> _core.Single();

	/// <inheritdoc cref="MappedQuery{T}.SingleAsync"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);

	internal ImmutableArray<T> ToImmutableArray()
		=> _core.ToImmutableArray();

	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(
		CancellationToken cancellationToken = default)
		=> _core.ToImmutableArrayAsync(cancellationToken);

	internal Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, comparer);

	/// <inheritdoc cref="ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey>(keySelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);

	internal Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, valueSelector, comparer);

	/// <inheritdoc cref="ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey, TValue>(keySelector, valueSelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	internal HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="ToHashSetAsync(IEqualityComparer{T}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		CancellationToken cancellationToken = default)
		=> ToHashSetAsync(null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync(IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);

	#region Buffer terminals

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBuffer(CancellationToken)"/>
	public PooledBuffer<T> ToAsyncBuffer(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBuffer(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBufferStream(CancellationToken)"/>
	public IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBufferStream(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.Stream(TimeSpan, CancellationToken)"/>
	public IAsyncEnumerable<T> Stream(
		TimeSpan stallTimeout,
		CancellationToken cancellationToken = default)
		=> _core.Stream(stallTimeout, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToBufferedAsyncStream(CancellationToken)"/>
	public IAsyncEnumerable<T> ToBufferedAsyncStream(
		CancellationToken cancellationToken = default)
		=> _core.ToBufferedAsyncStream(cancellationToken);

	#endregion
}

/// <summary>
/// A deferred, type-locked read over the rows of a <see cref="DataTable"/>.
/// Terminals are methods here.
/// </summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
/// <remarks>
/// <para>
/// Each terminal reads the table's current rows through a new <see cref="DataTableReader"/> and disposes it.
/// The table itself is not changed or cleared, so the view can be read more than once.
/// The view never touches a connection and never calls <see cref="IDataReader.NextResult"/>.
/// The asynchronous terminals complete synchronously: the rows are already in memory.
/// </para>
/// <para>
/// <see cref="Skip(int)"/> and <see cref="Take(int)"/> return the types of <see cref="MappedQuery{T}"/>;
/// the <see cref="MappedQuery{T}.SingleResult"/> they carry changes nothing here, since a table has one result set.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedReader{T}"/>
public readonly record struct MappedTable<T>
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

	/// <inheritdoc cref="ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey>(keySelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
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

	/// <inheritdoc cref="ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> ToDictionaryAsync<TKey, TValue>(keySelector, valueSelector, null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSet"/>
	public HashSet<T> ToHashSet(
		IEqualityComparer<T>? comparer = null)
		=> _core.ToHashSet(comparer);

	/// <inheritdoc cref="ToHashSetAsync(IEqualityComparer{T}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		CancellationToken cancellationToken = default)
		=> ToHashSetAsync(null, cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync(IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer,
		CancellationToken cancellationToken = default)
		=> _core.ToHashSetAsync(comparer, cancellationToken);
}
