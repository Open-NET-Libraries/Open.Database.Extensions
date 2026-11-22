namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> and returns a
	/// deferred <see cref="MappedQuery{T}"/>.
	/// Nothing runs until a terminal, such as
	/// <see cref="MappedQuery{T}.ToListAsync(CancellationToken)"/>, is invoked.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The mapping is compiled once per result-set shape and cached, so repeated
	/// queries pay no reflection or compilation cost;
	/// values are read with typed getters, so nothing is boxed.
	/// </para>
	/// <para>
	/// A type with a public parameterless constructor is built with
	/// <c>new T()</c>, and its public settable properties
	/// (<see langword="init"/> and <see langword="required"/> ones too)
	/// are set from the columns.
	/// Any other type is built through its one public constructor
	/// (a positional record, a record struct, a class with one constructor),
	/// and the properties the constructor does not cover are set afterwards.
	/// A <see langword="required"/> property needs a column.
	/// A single value such as <see cref="int"/>
	/// (a primitive, an enum, <see cref="string"/>, <see cref="decimal"/>,
	/// a date or time, <see cref="Guid"/>, <c>byte[]</c>
	/// or a <see cref="Nullable{T}"/> of one)
	/// is read from the first column of each row,
	/// converted as a property of that type is;
	/// a NULL is <see langword="default"/> for a non-nullable value type,
	/// so declare <c>int?</c> to tell NULL from 0.
	/// A <typeparamref name="T"/> that cannot be mapped
	/// (an interface, an abstract class,
	/// a type with two or more public constructors and none without parameters)
	/// throws <see cref="InvalidOperationException"/> when the map is built or
	/// <see cref="RowMap{T}.Default"/> is read,
	/// which the overloads without a <see cref="RowMap{T}"/> do at the call.
	/// See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html#records-and-constructors">Records and constructors</see>.
	/// </para>
	/// <para>
	/// Pass a <see cref="RowMap{T}"/> to read columns whose names differ from the
	/// properties; the overload without a map reads by column name.
	/// Columns match a property exactly, else ignoring case, else ignoring case
	/// and underscores (<c>author_id</c> feeds <c>AuthorId</c>);
	/// the resolved layout is cached per result-set layout.
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the
	/// call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the
	/// argument) is rebuilt, and its row reader compiled, on every call
	/// (about 29 KB and a reader compile for a one-row read of a 12-property
	/// model, against 464 B through a declared map; see
	/// <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html#columns-named-differently">Columns named differently</see>).
	/// </para>
	/// <para>
	/// A terminal returns the rows of the first result set only
	/// (a <em>result</em> here is one whole result set, what
	/// <see cref="IDataReader.NextResult"/> moves between; never a row)
	/// and never maps a later set.
	/// It then reads through the later sets without reading their rows, so a
	/// failure in a later statement is thrown as the provider raised it.
	/// That is not done with <c>SingleResult()</c>, written before <c>Map</c>
	/// (<c>command.SingleResult().Map&lt;T&gt;()</c>, see
	/// <see cref="SingleResult(IExecuteReader)"/>) or after it
	/// (<c>command.Map&lt;T&gt;().SingleResult()</c>, the same),
	/// when the caller stops reading early,
	/// or when the mapping, a selector or the caller's handler throws.
	/// A later failure can follow a write that already committed, so a caller
	/// that retries can repeat it, and the rows read are lost when it throws.
	/// </para>
	/// </remarks>
	/// <param name="command">The command to read from.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-overload']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(1)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		RowMap<T>? map)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// No map reads exactly as Map<T>() does:
		// a single value from the first column, a model by name.
		return map is null
			? Map<T>(command)
			: new MappedQuery<T>(Views.QueryCore(command, map));
	}

	/// <inheritdoc cref="Map{T}(IExecuteReader, RowMap{T}?)"/>
	[OverloadResolutionPriority(4)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// A single value such as int is read from the first column;
		// any other T maps by column name.
		return FirstColumn<T>.Instance is { } firstColumn
			? new MappedQuery<T>(Views.QueryCore(command, firstColumn))
			: Map(command, RowMap<T>.Default);
	}

	/// <summary>
	/// Locks in a custom projection and returns a deferred
	/// <see cref="MappedQuery{T}"/>.
	/// Nothing runs until a terminal is invoked;
	/// <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <param name="command">The command to read from.</param>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='map-selector']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
	[OverloadResolutionPriority(-1)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		Func<IDataRecord, T> selector)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new MappedQuery<T>(Views.QueryCore(command, new SelectorSource<T>(selector)));
	}
}

/// <summary>
/// The implementation shared by every mapped query type: the command, how
/// the rows are mapped, and the pending skip and take.
/// </summary>
/// <remarks>
/// The execution itself is the command's own <c>ExecuteReader</c> or
/// <c>ExecuteReaderAsync</c>.
/// Terminals run the read loops in <see cref="RowReader"/> directly against
/// the live reader (real async when the reader is a
/// <see cref="DbDataReader"/>).
/// Skipped rows are advanced without mapping; a take stops reading once
/// satisfied.
/// </remarks>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
/// <param name="command">The command the rows are read from.</param>
/// <param name="source">Reads and maps one row.</param>
/// <param name="skip">The number of rows to skip before the first mapped row.</param>
/// <param name="take">The maximum number of rows to map, or -1 for no limit.</param>
/// <param name="singleResult"><see langword="true"/> to stop after the first result set and not read through the later ones; otherwise, <see langword="false"/>.</param>
/// <param name="asyncReads"><see langword="true"/> to read the rows of an asynchronous terminal with <c>ReadAsync</c>; otherwise, <see langword="false"/>, which reads them with <c>Read</c>.</param>
/// <param name="config">The pool, the clear rule, the chunk sizes and the bin the pooling terminals use.</param>
readonly struct MappedQueryCore<T>(
	IExecuteReader command,
	IRowSource<T> source,
	int skip,
	int take,
	bool singleResult,
	bool asyncReads,
	ChunkConfig<T> config)
	: IEquatable<MappedQueryCore<T>>
{
	/// <summary>
	/// The reader behavior of a generated mapping.
	/// </summary>
	/// <remarks>
	/// Generated mappings read each column once, in order, so the provider may
	/// stream the row.
	/// A caller-supplied selector may read in any order, so it runs with the
	/// default access mode.
	/// </remarks>
	const CommandBehavior GeneratedMapping = CommandBehavior.SingleResult | CommandBehavior.SequentialAccess;

	readonly IRowSource<T> _source = source;
	readonly int _skip = skip;
	readonly int _take = take; // -1 == unbounded.
	readonly bool _singleResult = singleResult; // SingleResult() was called: stop after the first result set and do not read through the rest.
	readonly bool _asyncReads = asyncReads; // AsyncReads() was called: the rows of an async terminal are read with ReadAsync (the default is Read).

	/// <summary>
	/// The command, or <see langword="null"/> for a default value.
	/// </summary>
	/// <remarks>
	/// A default struct has no command: any use of it says so, the same way for
	/// every type of the family.
	/// </remarks>
	readonly IExecuteReader? _command = command;

	/// <summary>
	/// Gets the command the query reads from.
	/// </summary>
	/// <exception cref="InvalidOperationException">A default value was not created by <c>Map</c>.</exception>
	public IExecuteReader Command
		=> _command ?? throw new InvalidOperationException(
			"This query was not created by Map<T>(); a default value cannot be used.");

	/// <summary>
	/// Determines whether another query is the same query: the same command and
	/// source, skip, take and flags, and the same configuration instance.
	/// </summary>
	/// <remarks>
	/// Equality is for a caller that keys on the whole query; it never runs on
	/// the path that executes one.
	/// Without it a record struct holding this would compare and hash its fields
	/// by reflection.
	/// </remarks>
	/// <param name="other">The query to compare with.</param>
	/// <returns><see langword="true"/> if <paramref name="other"/> is the same query; otherwise, <see langword="false"/>.</returns>
	public bool Equals(MappedQueryCore<T> other)
		=> Equals(_command, other._command)
		&& Equals(_source, other._source)
		&& _skip == other._skip
		&& _take == other._take
		&& _singleResult == other._singleResult
		&& _asyncReads == other._asyncReads
		&& ReferenceEquals(Config, other.Config);

	public override bool Equals(object? obj)
		=> obj is MappedQueryCore<T> other && Equals(other);

	public override int GetHashCode()
	{
		unchecked
		{
			int h = _command?.GetHashCode() ?? 0;
			h = (h * 31) + (_source?.GetHashCode() ?? 0);
			h = (h * 31) + _skip;
			h = (h * 31) + _take;
			return (h * 31)
				+ (_singleResult ? 2 : 0)
				+ (_asyncReads ? 1 : 0)
				+ RuntimeHelpers.GetHashCode(Config); // Reference equality, null for a default value.
		}
	}

	/// <summary>
	/// Gets the <see cref="CommandBehavior"/> the reader is asked for.
	/// </summary>
	/// <remarks>
	/// Without <c>SingleResult()</c> the reader is asked for every result set,
	/// because a terminal reads through the later ones to report their failures.
	/// </remarks>
	CommandBehavior Behavior => _singleResult
		? (_source is SelectorSource<T> ? CommandBehavior.SingleResult : GeneratedMapping)
		: (_source is SelectorSource<T> ? CommandBehavior.Default : CommandBehavior.SequentialAccess);

	public MappedQueryCore<T> WithSingleResult() => new(Command, _source, _skip, _take, true, _asyncReads, Config);

	public MappedQueryCore<T> WithAsyncReads() => new(Command, _source, _skip, _take, _singleResult, true, Config);

	/// <summary>
	/// Gets the configuration the query was given.
	/// </summary>
	/// <remarks>
	/// It holds the pool every pooling terminal rents from and whether it
	/// clears, provided when the query is made (the default of the entry point,
	/// or <c>UsePool</c>).
	/// </remarks>
	ChunkConfig<T> Config { get; } = config;

	public MappedQueryCore<T> WithPool(ArrayPool<T> pool, bool clearOnReturn)
	{
		if (pool is null) throw new ArgumentNullException(nameof(pool));
		Contract.EndContractBlock();

		// A default value has no command: say so the way the whole family does,
		// before anything reads its (null) configuration.
		_ = Command;

		// The pool is the caller's; the sizes and the bin stay what the query had.
		return WithConfig(new ChunkConfig<T>(
			pool,
			clearOnReturn,
			Config.Sizes,
			Config.Bin,
			Config.DeferFromBytes));
	}

	/// <summary>
	/// Returns the query with a whole configuration
	/// (the pool, the clear, the chunk sizes, the bin).
	/// </summary>
	/// <param name="config">The configuration to use.</param>
	/// <returns>A copy of the query that uses <paramref name="config"/>.</returns>
	public MappedQueryCore<T> WithConfig(ChunkConfig<T> config)
		=> new(Command, _source, _skip, _take, _singleResult, _asyncReads, config);

	public MappedQueryCore<T> WithSkip(int count)
		=> count >= 0
			? new(
				Command,
				_source,
				_skip + count,
				_take,
				_singleResult,
				_asyncReads,
				Config)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	/// <summary>
	/// Returns the query limited to at most <paramref name="count"/> rows.
	/// </summary>
	/// <remarks>
	/// Take is set exactly once (the staging structs only expose it before any
	/// take exists), so this just records the limit; it applies after the skip,
	/// as <c>Enumerable.Skip(a).Take(b)</c> does.
	/// </remarks>
	/// <param name="count">The maximum number of rows to map.</param>
	/// <returns>A copy of the query with the limit.</returns>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
	public MappedQueryCore<T> WithTake(int count)
		=> count >= 0
			? new(
				Command,
				_source,
				_skip,
				count,
				_singleResult,
				_asyncReads,
				Config)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	/// <summary>
	/// Combines the token passed to a terminal with the command's own.
	/// </summary>
	/// <remarks>
	/// One token governs a whole call: the one passed to the terminal together
	/// with the command's own (either cancels), for the connection open, the
	/// name lookup, the execute and the reads.
	/// No token, one token, or the same token twice needs no source, so those
	/// calls allocate nothing for it.
	/// Two different tokens need a linked source, which the call disposes when
	/// it ends.
	/// </remarks>
	/// <param name="cancellationToken">The token passed to the terminal.</param>
	/// <param name="linked">The linked source to dispose when the call ends, or <see langword="null"/> when none was needed.</param>
	/// <returns>The token that governs the call.</returns>
	CancellationToken Combine(CancellationToken cancellationToken, out CancellationTokenSource? linked)
		=> CancellationTokens.Combine(Command.CancellationToken, cancellationToken, out linked);

	/// <summary>
	/// Executes the command asynchronously and passes the reader to
	/// <paramref name="transform"/>.
	/// </summary>
	/// <remarks>
	/// <paramref name="token"/> is the combined token; it is also in
	/// <paramref name="state"/>, which the terminal's <see langword="static"/> lambda reads it
	/// from.
	/// </remarks>
	/// <typeparam name="TState">The type of the state passed to the transform.</typeparam>
	/// <typeparam name="TResult">The type the transform returns.</typeparam>
	/// <param name="state">The state to pass to the transform.</param>
	/// <param name="transform">The function that reads the rows from the reader.</param>
	/// <param name="behavior">The behavior the reader is asked for.</param>
	/// <param name="linked">The linked source to dispose when the call ends, or <see langword="null"/>.</param>
	/// <param name="token">The combined token.</param>
	/// <returns>A task that produces the result of the transform.</returns>
	ValueTask<TResult> ExecuteAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationTokenSource? linked, CancellationToken token)
		=> linked is null
			? Command.ExecuteReaderAsync(state, transform, behavior, token)
			: ExecuteLinkedAsync(Command, state, transform, behavior, linked, token);

	static async ValueTask<TResult> ExecuteLinkedAsync<TState, TResult>(IExecuteReader command, TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationTokenSource linked, CancellationToken token)
	{
		using (linked)
			return await command.ExecuteReaderAsync(state, transform, behavior, token).ConfigureAwait(false);
	}

	/// <summary>
	/// Runs a direct terminal: the transform reads the first result set, then
	/// the later sets are read through.
	/// </summary>
	/// <remarks>
	/// The later sets are read through unless <c>SingleResult()</c> was called,
	/// so a failure in one is thrown.
	/// Nothing is read through when the transform throws, so the caller's own
	/// exception is never replaced.
	/// </remarks>
	/// <typeparam name="TState">The type of the state passed to the transform.</typeparam>
	/// <typeparam name="TResult">The type the transform returns.</typeparam>
	/// <param name="state">The state to pass to the transform.</param>
	/// <param name="transform">The function that reads the rows from the reader.</param>
	/// <param name="behavior">The behavior the reader is asked for.</param>
	/// <returns>The result of the transform.</returns>
	TResult Run<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior)
		=> _singleResult
			? Command.ExecuteReader(state, transform, behavior)
			: Command.ExecuteReader((state, transform), static (reader, s) =>
			{
				TResult result = s.transform(reader, s.state);
				RowReader.ReadThrough(reader);
				return result;
			}, behavior);

	ValueTask<TResult> RunAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationTokenSource? linked, CancellationToken token)
		=> _singleResult
			? ExecuteAsync(state, transform, behavior, linked, token)
			: ExecuteAsync((state, transform, token), static async (reader, s) =>
			{
				TResult result = await s.transform(reader, s.state).ConfigureAwait(false);
				await RowReader.ReadThroughAsync(reader, s.token).ConfigureAwait(false);
				return result;
			}, behavior, linked, token);

	// The terminals pass their inputs to the command as state and use static lambdas, so running a
	// query allocates no closures or delegates of its own.

	#region Handler (streaming) terminals

	public void Read(Action<IEnumerable<T>> handler)
		=> Command.ExecuteReader((handler, _source, _skip, _take, _singleResult), static (reader, s) =>
		{
			s.handler(RowReader.Stream(s._source, reader, s._skip, s._take, !s._singleResult));
			return false;
		}, Behavior);

	public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
		=> Command.ExecuteReader((handler, _source, _skip, _take, _singleResult), static (reader, s)
			=> s.handler(RowReader.Stream(s._source, reader, s._skip, s._take, !s._singleResult)), Behavior);

	public ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ValueTaskHelpers.Discard(ExecuteAsync((handler, _source, _skip, _take, _singleResult, _asyncReads, token), static async (reader, s) =>
		{
			await s.handler(StreamAsync(s._source, reader, s._skip, s._take, s._singleResult, s._asyncReads, s.token), s.token).ConfigureAwait(false);
			return false;
		}, Behavior, linked, token));
	}

	public ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((handler, _source, _skip, _take, _singleResult, _asyncReads, token), static (reader, s)
			=> s.handler(StreamAsync(s._source, reader, s._skip, s._take, s._singleResult, s._asyncReads, s.token), s.token), Behavior, linked, token);
	}

	/// <summary>
	/// Streams the rows asynchronously.
	/// </summary>
	/// <remarks>
	/// The streams read through the later result sets after the last row, so a
	/// caller who enumerates to the end (or to the take limit) sees a later
	/// failure.
	/// An enumeration the caller abandons, or whose handler throws, is disposed
	/// before that point and reads nothing more.
	/// </remarks>
	/// <param name="source">Reads and maps one row.</param>
	/// <param name="reader">The reader the rows are read from.</param>
	/// <param name="skip">The number of rows to skip.</param>
	/// <param name="take">The maximum number of rows to map, or -1 for no limit.</param>
	/// <param name="singleResult"><see langword="true"/> to stop after the first result set; otherwise, <see langword="false"/>, which reads through the later ones.</param>
	/// <param name="useAsync"><see langword="true"/> to read the rows with <c>ReadAsync</c>; otherwise, <see langword="false"/>.</param>
	/// <param name="token">The combined token that governs the call.</param>
	/// <returns>The rows as an asynchronous sequence.</returns>
	static IAsyncEnumerable<T> StreamAsync(IRowSource<T> source, IDataReader reader, int skip, int take, bool singleResult, bool useAsync, CancellationToken token)
		=> reader is DbDataReader db
			? RowReader.StreamAsync(source, db, skip, take, useAsync, !singleResult, token)
			: RowReader
				.Stream(source, reader, skip, take, !singleResult, token)
				.ToAsyncEnumerable();

	#endregion

	#region Direct terminals

	public List<T> ToList()
		=> Run((_source, _skip, _take), static (reader, s)
			=> RowReader.ToList(s._source, reader, s._skip, s._take), Behavior);

	public T[] ToArray()
		=> Run((_source, _skip, _take, Config), static (reader, s)
			=> RowReader.ToArray(s._source, reader, s._skip, s._take, s.Config), Behavior);

	public T? FirstOrDefault()
		=> Run((_source, _skip, _take), static (reader, s)
			=> RowReader.TryFirst(s._source, reader, s._skip, s._take, out T? item) ? item : default, FirstBehavior);

	/// <summary>
	/// Gets the behavior for the first-row terminals.
	/// </summary>
	/// <remarks>
	/// <see cref="CommandBehavior.SingleRow"/> tells the provider that only one
	/// row is needed.
	/// It is added only when nothing is skipped and <c>SingleResult()</c> was
	/// called.
	/// </remarks>
	CommandBehavior FirstBehavior => _skip == 0 && _singleResult ? Behavior | CommandBehavior.SingleRow : Behavior;

	public T? SingleOrDefault()
		=> Run((_source, _skip, _take), static (reader, s)
			=> RowReader.TrySingle(s._source, reader, s._skip, s._take, out T? item) ? item : default, Behavior);

	/// <summary>
	/// Reads the first row; throws if there is none.
	/// </summary>
	/// <remarks>
	/// The throwing twins (<see cref="First"/>, <see cref="Single"/>) read
	/// exactly as the <c>OrDefault</c> forms do.
	/// They throw for no rows only after the call has cleaned up (the
	/// connection closed if this call opened it, the later result sets read
	/// through), so a failure from a later result set is thrown instead.
	/// </remarks>
	/// <returns>The first row.</returns>
	public T First()
	{
		(bool found, T? row) = Run(
			(_source, _skip, _take),
			static (reader, s) => RowReader.TryFirst(
				s._source, reader, s._skip, s._take, out T? item)
				? (true, item)
				: (false, default),
			FirstBehavior);

		return found ? row! : throw RowReader.NoRows(nameof(First));
	}

	public T Single()
	{
		(bool found, T? row) = Run(
			(_source, _skip, _take),
			static (reader, s) => RowReader.TrySingle(
				s._source, reader, s._skip, s._take, out T? item)
				? (true, item)
				: (false, default),
			Behavior);

		return found ? row! : throw RowReader.NoRows(nameof(Single));
	}

	/// <summary>
	/// Reads the rows into a chain of pooled arrays and builds the result from
	/// it, asynchronously.
	/// </summary>
	/// <remarks>
	/// The collecting terminals (<c>ToList</c>, <c>ToArray</c>, <c>Queue</c>,
	/// <c>HashSet</c> and <c>Dictionary</c>, async) read into a chain of pooled
	/// arrays (see <c>ChunkChain</c>) and build the result from it in the same
	/// read method, right after the last row (one memcpy for a list or an
	/// array; the connection is released when the call returns).
	/// The read and the build are <em>one</em> async state machine
	/// (<c>RowReader.CollectAsync</c>).
	/// The build is a struct (<c>IChainBuild</c>), so it rides in the state tuple
	/// at no more than its own fields, and the lambdas are <see langword="static"/>: a call
	/// allocates no closure or delegate.
	/// </remarks>
	/// <typeparam name="TBuild">The type of the build, a struct that makes the result from the chain.</typeparam>
	/// <typeparam name="TResult">The type of the result.</typeparam>
	/// <param name="build">The build that makes the result.</param>
	/// <param name="cancellationToken">The token passed to the terminal.</param>
	/// <returns>A task that produces the result.</returns>
	public ValueTask<TResult> CollectAsync<TBuild, TResult>(
		TBuild build,
		CancellationToken cancellationToken)
		where TBuild : struct, IChainBuild<T, TResult>
	{
		CancellationToken token = Combine(
			cancellationToken, out CancellationTokenSource? linked);
		return RunAsync(
			(_source, _skip, _take, _asyncReads, Config, token, build),
			static (reader, s) => reader is DbDataReader db
				? RowReader.CollectAsync<T, TBuild, TResult>(
					s._source, db, s._skip, s._take, s._asyncReads,
					s.build, s.Config, s.token)
				: new ValueTask<TResult>(RowReader.Collect<T, TBuild, TResult>(
					s._source, reader, s._skip, s._take, s.build,
					s.Config, s.token)),
			Behavior, linked, token);
	}

	public TResult Collect<TBuild, TResult>(TBuild build)
		where TBuild : struct, IChainBuild<T, TResult>
		=> Run(
			(_source, _skip, _take, Config, build),
			static (reader, s) => RowReader.Collect<T, TBuild, TResult>(
				s._source, reader, s._skip, s._take, s.build,
				s.Config),
			Behavior);

	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken)
		=> CollectAsync<ListBuild<T>, List<T>>(default, cancellationToken);

	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken)
		=> CollectAsync<ArrayBuild<T>, T[]>(default, cancellationToken);

	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return RunAsync((_source, _skip, _take, _asyncReads, token), static async (reader, s) =>
		{
			if (reader is DbDataReader db)
			{
				(bool found, T? item) = await RowReader.TryFirstAsync(s._source, db, s._skip, s._take, s._asyncReads, s.token).ConfigureAwait(false);
				return found ? item : default;
			}

			return RowReader.TryFirst(s._source, reader, s._skip, s._take, out T? i, s.token) ? i : default;
		}, FirstBehavior, linked, token);
	}

	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return RunAsync((_source, _skip, _take, _asyncReads, token), static async (reader, s) =>
		{
			if (reader is DbDataReader db)
			{
				(bool found, T? item) = await RowReader.TrySingleAsync(s._source, db, s._skip, s._take, s._asyncReads, s.token).ConfigureAwait(false);
				return found ? item : default;
			}

			return RowReader.TrySingle(s._source, reader, s._skip, s._take, out T? i, s.token) ? i : default;
		}, Behavior, linked, token);
	}

	public async ValueTask<T> FirstAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(
			cancellationToken, out CancellationTokenSource? linked);

		(bool found, T? row) = await RunAsync(
			(_source, _skip, _take, _asyncReads, token),
			static async (reader, s) =>
			{
				if (reader is DbDataReader db)
				{
					return await RowReader.TryFirstAsync(
						s._source, db, s._skip, s._take,
						s._asyncReads, s.token).ConfigureAwait(false);
				}

				bool has = RowReader.TryFirst(
					s._source, reader, s._skip, s._take, out T? item, s.token);
				return (has, item);
			},
			FirstBehavior, linked, token).ConfigureAwait(false);

		return found ? row! : throw RowReader.NoRows(nameof(FirstAsync));
	}

	public async ValueTask<T> SingleAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(
			cancellationToken, out CancellationTokenSource? linked);

		(bool found, T? row) = await RunAsync(
			(_source, _skip, _take, _asyncReads, token),
			static async (reader, s) =>
			{
				if (reader is DbDataReader db)
				{
					return await RowReader.TrySingleAsync(
						s._source, db, s._skip, s._take,
						s._asyncReads, s.token).ConfigureAwait(false);
				}

				bool has = RowReader.TrySingle(
					s._source, reader, s._skip, s._take, out T? item, s.token);
				return (has, item);
			},
			Behavior, linked, token).ConfigureAwait(false);

		return found ? row! : throw RowReader.NoRows(nameof(SingleAsync));
	}

	public ImmutableArray<T> ToImmutableArray()
	{
		T[] array = ToArray();
#if NET8_0_OR_GREATER
		return System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsImmutableArray(array);
#else
		return ImmutableArray.Create(array);
#endif
	}

	/// <summary>
	/// Reads the rows as <c>ToArrayAsync</c> does and wraps the array in an
	/// <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <remarks>
	/// The array is wrapped in the build, with no <see langword="async"/> method of its own around
	/// it, which a read that really suspends would box.
	/// </remarks>
	/// <param name="cancellationToken">The token passed to the terminal.</param>
	/// <returns>A task that produces the immutable array.</returns>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken)
		=> CollectAsync<ImmutableArrayBuild<T>, ImmutableArray<T>>(
			default, cancellationToken);

	/// <summary>
	/// Reads the rows into a dictionary keyed by <paramref name="keySelector"/>.
	/// </summary>
	/// <remarks>
	/// The dictionary and hash set terminals read the rows into the chain, then
	/// add them (the exact capacity known): a duplicate key throws after every
	/// row has been read, and the selectors run after the read.
	/// </remarks>
	/// <typeparam name="TKey">The type of the dictionary key.</typeparam>
	/// <param name="keySelector">Produces the key for each mapped row.</param>
	/// <param name="comparer">The comparer for the keys, or <see langword="null"/> to use the default comparer.</param>
	/// <returns>A dictionary of all the rows.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
	public Dictionary<TKey, T> ToDictionary<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();

		return Collect<DictionaryBuild<T, TKey>, Dictionary<TKey, T>>(
			new(keySelector, comparer));
	}

	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
		Func<T, TKey> keySelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();

		return CollectAsync<DictionaryBuild<T, TKey>, Dictionary<TKey, T>>(
			new(keySelector, comparer), cancellationToken);
	}

	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		if (valueSelector is null) throw new ArgumentNullException(nameof(valueSelector));
		Contract.EndContractBlock();

		return Collect<
			DictionaryValueBuild<T, TKey, TValue>,
			Dictionary<TKey, TValue>>(
			new(keySelector, valueSelector, comparer));
	}

	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer,
		CancellationToken cancellationToken)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		if (valueSelector is null) throw new ArgumentNullException(nameof(valueSelector));
		Contract.EndContractBlock();

		return CollectAsync<
			DictionaryValueBuild<T, TKey, TValue>,
			Dictionary<TKey, TValue>>(
			new(keySelector, valueSelector, comparer), cancellationToken);
	}

	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer)
		=> Collect<HashSetBuild<T>, HashSet<T>>(new(comparer));

	public ValueTask<HashSet<T>> ToHashSetAsync(
		IEqualityComparer<T>? comparer,
		CancellationToken cancellationToken)
		=> CollectAsync<HashSetBuild<T>, HashSet<T>>(
			new(comparer), cancellationToken);

	#endregion

	#region Buffer terminals

	/// <summary>
	/// Reads every row into a sink.
	/// </summary>
	/// <remarks>
	/// The producer reads straight into a sink by the same internal path as
	/// <c>ToArrayAsync</c> (<c>RowReader.FillAsync</c> / <c>Fill</c>): the
	/// connection, the read mode, <c>Skip</c>, <c>Take</c> and the read-through
	/// are the same, and there is no per-row enumerator.
	/// The sink is a struct (so the loop specializes) that holds a reference to
	/// its state, so the copy the loop passes across awaits is harmless.
	/// </remarks>
	/// <typeparam name="TSink">The type of the sink.</typeparam>
	/// <param name="sink">The sink the rows are written to.</param>
	/// <param name="cancellationToken">The token passed to the terminal.</param>
	/// <returns>A task that produces <see langword="true"/> when the read has completed.</returns>
	public ValueTask<bool> FillAsync<TSink>(
		TSink sink,
		CancellationToken cancellationToken)
		where TSink : struct, IRowSink<T>
	{
		CancellationToken token = Combine(
			cancellationToken, out CancellationTokenSource? linked);
		return RunAsync(
			(_source, _skip, _take, _asyncReads, token, sink),
			static async (reader, s) =>
			{
				if (reader is DbDataReader db)
				{
					await RowReader.FillAsync(
						s._source, db, s._skip, s._take,
						s._asyncReads, s.sink, s.token)
						.ConfigureAwait(false);
				}
				else
				{
					TSink copy = s.sink;
					RowReader.Fill(
						s._source, reader, s._skip, s._take,
						ref copy, s.token);
				}

				return true;
			},
			Behavior, linked, token);
	}

	/// <summary>
	/// Starts reading the rows into a pooled buffer.
	/// </summary>
	/// <remarks>
	/// The buffer terminals of every view run the query through the sink path
	/// above (see ChunkProducer.cs).
	/// <c>ToAsyncBuffer</c> starts at the call; the two streams start at the
	/// first <see cref="IAsyncEnumerator{T}.MoveNextAsync"/>.
	/// A default struct is refused here, at the call, as everywhere else.
	/// They rent from the pool of <c>UsePool</c> (the shared array pool when
	/// there was none) and clear by the rule of <c>PoolClearing</c>.
	/// </remarks>
	/// <param name="cancellationToken">The token that cancels the read.</param>
	/// <returns>The buffer, already reading.</returns>
	public PooledBuffer<T> ToAsyncBuffer(
		CancellationToken cancellationToken)
	{
		_ = Command;
		return new(this, Config, cancellationToken);
	}

	public IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken)
	{
		_ = Command;
		return new ChunkStream<T>(this, Config, cancellationToken);
	}

	public IAsyncEnumerable<T> ToBufferedAsyncStream(
		CancellationToken cancellationToken)
	{
		_ = Command;
		// The stream carries the caller's token; the token Flatten takes is
		// the enumeration's own (WithCancellation), so none is passed here.
		return Flatten(
			new ChunkStream<T>(this, Config, cancellationToken),
			CancellationToken.None);
	}

	/// <summary>
	/// Yields the elements of a chunk stream one by one.
	/// </summary>
	/// <remarks>
	/// Each element is copied into a local before it is yielded, so no span
	/// lives across a yield; the chunk's array goes back at the next chunk
	/// step, as for the chunk stream, and the one-shot guard is the chunk
	/// stream's.
	/// </remarks>
	/// <param name="chunks">The chunk stream to flatten.</param>
	/// <param name="cancellationToken">The token of the enumeration.</param>
	/// <returns>The elements of the chunks, in order.</returns>
	static async IAsyncEnumerable<T> Flatten(
		IAsyncEnumerable<ReadOnlyMemory<T>> chunks,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		await foreach (ReadOnlyMemory<T> chunk in chunks
			.WithCancellation(cancellationToken)
			.ConfigureAwait(false))
		{
			for (int i = 0; i < chunk.Length; ++i)
			{
				T item = chunk.Span[i];
				yield return item;
			}
		}
	}

	#endregion
}

/// <summary>A deferred, type-locked query over any <see cref="IExecuteReader"/>. Terminals are methods here, with more as extensions on <see cref="IMappedQuery{T}"/>.</summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
public readonly record struct MappedQuery<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	MappedQueryCore<T> IMappedTerminals<T>.Core => _core;

	readonly MappedQueryCore<T> _core;
	internal MappedQuery(MappedQueryCore<T> core) => _core = core;

	/// <summary>
	/// Gets the command this query reads from.
	/// </summary>
	/// <value>The command the query reads from.</value>
	/// <remarks>
	/// It is for a caller that keys on the query (the memoize seam).
	/// Nothing runs by reading it.
	/// </remarks>
	/// <exception cref="InvalidOperationException">A default value was not created by <c>Map</c>.</exception>
	public IExecuteReader Command => _core.Command;

	/// <summary>Skips the first <paramref name="count"/> mapped rows.</summary>
	/// <param name="count">The number of rows to skip.</param>
	/// <returns>A query that can still be limited with <see cref="MappedQueryWithSkip{T}.Take(int)"/>.</returns>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
	public MappedQueryWithSkip<T> Skip(int count) => new(_core.WithSkip(count));

	/// <summary>Limits the result to at most <paramref name="count"/> mapped rows.</summary>
	/// <remarks>The limit applies after any skip.</remarks>
	/// <param name="count">The maximum number of rows to map.</param>
	/// <returns>A query that is ready for a terminal.</returns>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
	public MappedQueryWithTake<T> Take(int count) => new(_core.WithTake(count));

	/// <summary>Stops after the first result set and does not read through the later ones.</summary>
	/// <remarks>
	/// A <em>result</em> here is one whole result set, what <see cref="IDataReader.NextResult"/> moves between; never a row.
	/// By default a terminal returns the rows of the first result set only, then reads through the later ones without mapping them, so a failure in a later statement is thrown instead of lost.
	/// A later failure can follow a write that already committed, so a caller that retries can repeat it, and the rows read are lost when it throws.
	/// This call asserts nothing: it does not check that there is only one result set. It stops after the first, and a later failure goes unseen.
	/// A read-through also does not happen when the caller stops reading early or the mapping, the selector or the caller's handler throws.
	/// <c>FirstOrDefault</c> and <c>First</c> with nothing skipped also ask the provider for a single row.
	/// <c>command.SingleResult().Map&lt;T&gt;()</c> (see <see cref="CoreExtensions.SingleResult(IExecuteReader)"/>) is the same as <c>command.Map&lt;T&gt;().SingleResult()</c>.
	/// </remarks>
	/// <returns>The same query, without the read-through.</returns>
	public MappedQuery<T> SingleResult() => new(_core.WithSingleResult());

	/// <summary>Reads the rows of an async terminal with <c>ReadAsync</c> instead of <c>Read</c>.</summary>
	/// <remarks>
	/// By default every terminal, async ones included, executes asynchronously and reads the rows with <c>Read</c>, the fastest and lightest way, also for large values.
	/// This opts in to <c>ReadAsync</c> (the getters stay synchronous, and the later result sets are read through with <c>NextResultAsync</c>) for this call only.
	/// A selector map (<c>Map(Func)</c>) on one very large value reads slower and uses more memory this way.
	/// It changes nothing on a synchronous terminal. It can be called in any order with <c>Skip</c>, <c>Take</c> and <c>SingleResult</c>.
	/// </remarks>
	/// <returns>The same query, reading its rows asynchronously.</returns>
	public MappedQuery<T> AsyncReads() => new(_core.WithAsyncReads());

	/// <summary>
	/// Chooses the pool this query's pooled arrays are rented from, and
	/// whether their rows are cleared when they go back.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Call it after <c>Map</c> (the pool is typed by the row).
	/// Both arguments are required: there is no overload without
	/// <paramref name="clearOnReturn"/>, so the choice is always visible at the call,
	/// and it is written by name,
	/// <c>.UsePool(ArrayPool&lt;Post&gt;.Shared, clearOnReturn: true)</c>.
	/// <paramref name="clearOnReturn"/> controls the rows that hold no references;
	/// rows that hold references are always cleared.
	/// An explicit <c>clearOnReturn: false</c> wins over the app-wide switch for this
	/// query, and an explicit <c>clearOnReturn: true</c> clears whatever the switch
	/// says.
	/// </para>
	/// <para>
	/// It applies to every terminal of this query that rents pooled arrays:
	/// </para>
	/// <list type="bullet">
	/// <item><description><see cref="ToArray"/></description></item>
	/// <item><description><see cref="ToArrayAsync(CancellationToken)"/></description></item>
	/// <item><description><see cref="ToListAsync(CancellationToken)"/></description></item>
	/// <item><description><see cref="ToImmutableArray"/></description></item>
	/// <item><description><see cref="ToImmutableArrayAsync(CancellationToken)"/></description></item>
	/// <item><description><see cref="ToHashSet(IEqualityComparer{T})"/></description></item>
	/// <item><description><see cref="ToHashSetAsync(IEqualityComparer{T}, CancellationToken)"/></description></item>
	/// <item><description><see cref="ToDictionary{TKey}(Func{T, TKey}, IEqualityComparer{TKey})"/></description></item>
	/// <item><description><see cref="ToDictionary{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})"/></description></item>
	/// <item><description><see cref="ToDictionaryAsync{TKey}(Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/></description></item>
	/// <item><description><see cref="ToDictionaryAsync{TKey, TValue}(Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)"/></description></item>
	/// <item><description><see cref="ToAsyncBuffer(CancellationToken)"/></description></item>
	/// <item><description><see cref="ToAsyncBufferStream(CancellationToken)"/></description></item>
	/// <item><description><see cref="ToBufferedAsyncStream(CancellationToken)"/></description></item>
	/// </list>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	/// <param name="pool">
	/// The pool to rent from and give back to. It is yours: ODX never
	/// disposes it. <see cref="ArrayPool{T}.Shared"/> is what a query without
	/// <c>UsePool</c> uses.
	/// </param>
	/// <param name="clearOnReturn">
	/// <see langword="true"/> to clear the used rows of every array before it
	/// goes back, value rows included; otherwise, <see langword="false"/>,
	/// which does not clear rows that hold no references, whatever the
	/// app-wide switch says.
	/// </param>
	/// <returns>The same query, renting from <paramref name="pool"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="pool"/> is <see langword="null"/>.</exception>
	public MappedQuery<T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
		=> new(_core.WithPool(pool, clearOnReturn));

	/// <summary>
	/// Returns the query with a whole configuration
	/// (pool, clear, chunk sizes, bin), for a test or a benchmark.
	/// </summary>
	/// <param name="config">The configuration to use.</param>
	/// <returns>A copy of the query that uses <paramref name="config"/>.</returns>
	internal MappedQuery<T> WithConfig(ChunkConfig<T> config)
		=> new(_core.WithConfig(config));

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler) => _core.Read(handler);
	/// <inheritdoc />
	public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler) => _core.Read(handler);
	/// <inheritdoc />
	public ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);
	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQueryExtensions.ToList{T}(IMappedQuery{T})"/>
	public List<T> ToList() => _core.ToList();
	/// <inheritdoc cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public T First()
		=> _core.First();
	/// <inheritdoc cref="MappedQueryExtensions.FirstAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public T Single()
		=> _core.Single();
	/// <inheritdoc cref="MappedQueryExtensions.SingleAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null)
		where TKey : notnull
		=> _core.ToDictionary(keySelector, valueSelector, comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey, TValue}(IMappedQuery{T}, Func{T, TKey}, Func{T, TValue}, IEqualityComparer{TKey}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
		Func<T, TKey> keySelector,
		Func<T, TValue> valueSelector,
		IEqualityComparer<TKey>? comparer = null,
		CancellationToken cancellationToken = default)
		where TKey : notnull
		=> _core.ToDictionaryAsync(
			keySelector, valueSelector, comparer, cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)" path="/*[not(self::remarks)]"/>
	/// <remarks>
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)" path="/remarks/node()"/>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// </remarks>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);

	#region Buffer terminals

	/// <summary>
	/// Reads every row into pooled arrays and keeps them until the
	/// returned buffer is disposed.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The call returns at once and starts the read on a background
	/// producer, once, even if nobody reads the buffer yet
	/// (it may finish before any read happens).
	/// Await <see cref="PooledBuffer{T}.Items"/> for the complete
	/// result. The buffer is not readable while it fills: to read
	/// while the query is still running, use
	/// <see cref="ToAsyncBufferStream(CancellationToken)"/> or
	/// <see cref="ToBufferedAsyncStream(CancellationToken)"/>.
	/// </para>
	/// <para>
	/// The rows stay valid until the buffer is disposed:
	/// dispose it (<see langword="await"/> <see langword="using"/>) when you are done.
	/// Disposing it while the read is still running cancels the
	/// query; after the read finished it only releases the arrays.
	/// A buffer that is never disposed holds its arrays until the
	/// garbage collector takes it (there is no finalizer).
	/// The connection ODX opened is closed when the read ends,
	/// however slowly the buffer is read.
	/// The producer is unbounded: it never waits for a reader, so a
	/// slow reader lets rows pile up in memory (the read finishes at
	/// full speed and frees the connection, while memory grows with
	/// the backlog), as with an unbounded channel.
	/// </para>
	/// <para>
	/// Use it for a reusable, potentially indexable list to work with
	/// for a while.
	/// <see cref="ToAsyncBufferStream(CancellationToken)"/> reads
	/// the elements once, through the buffers, without copying them
	/// twice;
	/// <see cref="ToBufferedAsyncStream(CancellationToken)"/> is
	/// the same without buffers.
	/// </para>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// <para>
	/// Here the clearing happens when the buffer, or the
	/// <see cref="PooledArray{T}"/> view of it, is disposed (the buffer
	/// keeps its arrays until then). A large array of rows that hold
	/// references is handed to an internal background queue, so the read and
	/// the consumer do not wait for it; anything else is cleared where it is
	/// released. If the queue is full (10,240 waiting), or cannot take the
	/// work, the array is cleared where it is released too, so a dirty array
	/// never reaches the pool.
	/// </para>
	/// </remarks>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='buffer-token']/*" />
	/// <returns>The buffer, already reading.</returns>
	public PooledBuffer<T> ToAsyncBuffer(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBuffer(cancellationToken);

	/// <summary>
	/// Reads the query in a rolling stream of pooled arrays,
	/// once, for one consumer.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Lazy: nothing executes until the first
	/// <see cref="IAsyncEnumerator{T}.MoveNextAsync"/>.
	/// The stream can be enumerated once:
	/// a second <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator(CancellationToken)"/> throws
	/// <see cref="InvalidOperationException"/>.
	/// </para>
	/// <para>
	/// Each <see cref="ReadOnlyMemory{T}"/> holds exactly the rows of
	/// one chunk and is valid until the next
	/// <see cref="IAsyncEnumerator{T}.MoveNextAsync"/> or the end of the enumeration,
	/// when its array goes back to the pool:
	/// copy what you keep.
	/// Nothing guards a memory held longer (it then reads rows
	/// that were reused), so do not buffer the chunks themselves, for
	/// example with <c>ToListAsync</c> or <c>Buffer</c>.
	/// An enumerator that is never disposed never returns its arrays
	/// (there is no finalizer): they go to the garbage collector.
	/// A producer reads ahead without waiting for the consumer
	/// (the queue is unbounded), so the connection ODX opened is
	/// closed when the read ends, not when the consumer is done,
	/// while memory grows with the backlog: a slow consumer lets
	/// chunks pile up, as with an unbounded channel.
	/// </para>
	/// <para>
	/// Disposing the enumerator, which a <see langword="break"/> out of
	/// <see langword="await"/> <see langword="foreach"/> always does, cancels the query and
	/// returns every array still queued.
	/// </para>
	/// <para>
	/// Use it for sequential, read-only access to the elements through
	/// the buffers, without copying them twice, for raw speed and the
	/// shortest connection time.
	/// <see cref="ToBufferedAsyncStream(CancellationToken)"/> is
	/// the same without buffers;
	/// <see cref="ToAsyncBuffer(CancellationToken)"/> keeps the
	/// rows as a list.
	/// </para>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// <para>
	/// Here the clearing happens as each chunk goes back, once the consumer
	/// is done with it (at the next <see cref="IAsyncEnumerator{T}.MoveNextAsync"/>, or when the
	/// enumeration ends or is disposed). A large chunk of rows that hold
	/// references is handed to an internal background queue, so the read and
	/// the consumer do not wait for it; anything else is cleared where it is
	/// released. If the queue is full (10,240 waiting), or cannot take the
	/// work, the chunk is cleared where it is released too, so a dirty array
	/// never reaches the pool.
	/// </para>
	/// </remarks>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='buffer-token']/*" />
	/// <returns>The lazy, single-use stream.</returns>
	public IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBufferStream(cancellationToken);

	/// <summary>
	/// Reads the query in a rolling stream of pooled arrays,
	/// once, and yields the elements one by one.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The same pipeline as
	/// <see cref="ToAsyncBufferStream(CancellationToken)"/>
	/// (lazy, one consumer, a background producer, rolling pooled
	/// chunks, a <see langword="break"/> or a dispose cancels the query),
	/// but each chunk is flattened into its elements,
	/// so there is no buffer to handle and the stream works with
	/// <c>System.Linq.Async</c>.
	/// Each element is copied out before it is yielded, so it has
	/// no lifetime to mind.
	/// The producer is unbounded, as for that stream: a slow consumer
	/// lets chunks pile up in memory.
	/// A second enumeration throws
	/// <see cref="InvalidOperationException"/> at its first
	/// <see cref="IAsyncEnumerator{T}.MoveNextAsync"/> (not at <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator(CancellationToken)"/>).
	/// </para>
	/// <para>
	/// Which one to use:
	/// <see cref="ToAsyncBufferStream(CancellationToken)"/> is
	/// sequential, read-only access to the elements through the
	/// buffers, without copying them twice, for raw speed and the
	/// shortest connection time;
	/// this method is the same, without buffers;
	/// <see cref="ToAsyncBuffer(CancellationToken)"/> is a
	/// reusable, potentially indexable list to work with for a while.
	/// </para>
	/// <include file="PooledMemory.docs.xml" path="docs/members[@name='pooled']/*" />
	/// <para>
	/// Here the clearing happens as each chunk goes back, once the consumer
	/// is done with its elements (the chunk is released at the next chunk
	/// step, or when the enumeration ends or is disposed). A large chunk of
	/// rows that hold references is handed to an internal background queue, so
	/// the read and the consumer do not wait for it; anything else is cleared
	/// where it is released. If the queue is full (10,240 waiting), or cannot
	/// take the work, the chunk is cleared where it is released too, so a
	/// dirty array never reaches the pool.
	/// </para>
	/// </remarks>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='buffer-token']/*" />
	/// <returns>The lazy, single-use stream of elements.</returns>
	public IAsyncEnumerable<T> ToBufferedAsyncStream(
		CancellationToken cancellationToken = default)
		=> _core.ToBufferedAsyncStream(cancellationToken);

	#endregion
}

/// <summary>A <see cref="MappedQuery{T}"/> with a pending skip; can still be limited with <see cref="Take(int)"/>.</summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
public readonly record struct MappedQueryWithSkip<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	MappedQueryCore<T> IMappedTerminals<T>.Core => _core;

	readonly MappedQueryCore<T> _core;
	internal MappedQueryWithSkip(MappedQueryCore<T> core) => _core = core;

	/// <inheritdoc cref="MappedQuery{T}.Command"/>
	public IExecuteReader Command => _core.Command;

	/// <inheritdoc cref="MappedQuery{T}.Take(int)"/>
	public MappedQueryWithTake<T> Take(int count) => new(_core.WithTake(count));

	/// <inheritdoc cref="MappedQuery{T}.SingleResult"/>
	public MappedQueryWithSkip<T> SingleResult() => new(_core.WithSingleResult());

	/// <inheritdoc cref="MappedQuery{T}.AsyncReads"/>
	public MappedQueryWithSkip<T> AsyncReads() => new(_core.WithAsyncReads());

	/// <inheritdoc cref="MappedQuery{T}.UsePool(ArrayPool{T}, bool)"/>
	public MappedQueryWithSkip<T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
		=> new(_core.WithPool(pool, clearOnReturn));

	/// <inheritdoc cref="MappedQuery{T}.WithConfig(ChunkConfig{T})"/>
	internal MappedQueryWithSkip<T> WithConfig(ChunkConfig<T> config)
		=> new(_core.WithConfig(config));

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler) => _core.Read(handler);
	/// <inheritdoc />
	public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler) => _core.Read(handler);
	/// <inheritdoc />
	public ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);
	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQueryExtensions.ToList{T}(IMappedQuery{T})"/>
	public List<T> ToList() => _core.ToList();
	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToArray"/>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public T First()
		=> _core.First();
	/// <inheritdoc cref="MappedQueryExtensions.FirstAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public T Single()
		=> _core.Single();
	/// <inheritdoc cref="MappedQueryExtensions.SingleAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArray"/>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey}"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
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
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);

	#region Buffer terminals

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBuffer(CancellationToken)"/>
	public PooledBuffer<T> ToAsyncBuffer(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBuffer(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBufferStream(CancellationToken)"/>
	public IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBufferStream(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToBufferedAsyncStream(CancellationToken)"/>
	public IAsyncEnumerable<T> ToBufferedAsyncStream(
		CancellationToken cancellationToken = default)
		=> _core.ToBufferedAsyncStream(cancellationToken);

	#endregion
}

/// <summary>A fully-configured <see cref="MappedQuery{T}"/> with a take (and any preceding skip); terminal-only.</summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
public readonly record struct MappedQueryWithTake<T>
	: IMappedQuery<T>, IMappedTerminals<T>
{
	MappedQueryCore<T> IMappedTerminals<T>.Core => _core;

	readonly MappedQueryCore<T> _core;
	internal MappedQueryWithTake(MappedQueryCore<T> core) => _core = core;

	/// <inheritdoc cref="MappedQuery{T}.Command"/>
	public IExecuteReader Command => _core.Command;

	/// <inheritdoc cref="MappedQuery{T}.SingleResult"/>
	public MappedQueryWithTake<T> SingleResult() => new(_core.WithSingleResult());

	/// <inheritdoc cref="MappedQuery{T}.AsyncReads"/>
	public MappedQueryWithTake<T> AsyncReads() => new(_core.WithAsyncReads());

	/// <inheritdoc cref="MappedQuery{T}.UsePool(ArrayPool{T}, bool)"/>
	public MappedQueryWithTake<T> UsePool(ArrayPool<T> pool, bool clearOnReturn)
		=> new(_core.WithPool(pool, clearOnReturn));

	/// <inheritdoc cref="MappedQuery{T}.WithConfig(ChunkConfig{T})"/>
	internal MappedQueryWithTake<T> WithConfig(ChunkConfig<T> config)
		=> new(_core.WithConfig(config));

	/// <inheritdoc />
	public void Read(Action<IEnumerable<T>> handler) => _core.Read(handler);
	/// <inheritdoc />
	public TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler) => _core.Read(handler);
	/// <inheritdoc />
	public ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);
	/// <inheritdoc />
	public ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken) => _core.ReadAsync(handler, cancellationToken);

	/// <inheritdoc cref="MappedQueryExtensions.ToList{T}(IMappedQuery{T})"/>
	public List<T> ToList() => _core.ToList();
	/// <inheritdoc cref="MappedQuery{T}.ToListAsync"/>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToArray"/>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQuery{T}.ToArrayAsync"/>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.First{T}(IMappedQuery{T})"/>
	public T First()
		=> _core.First();
	/// <inheritdoc cref="MappedQueryExtensions.FirstAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default)
		=> _core.FirstAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.Single{T}(IMappedQuery{T})"/>
	public T Single()
		=> _core.Single();
	/// <inheritdoc cref="MappedQueryExtensions.SingleAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default)
		=> _core.SingleAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArray"/>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQuery{T}.ToImmutableArrayAsync"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQuery{T}.ToDictionary{TKey}"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQuery{T}.ToDictionaryAsync{TKey}"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
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
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQuery{T}.ToHashSetAsync"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);

	#region Buffer terminals

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBuffer(CancellationToken)"/>
	public PooledBuffer<T> ToAsyncBuffer(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBuffer(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToAsyncBufferStream(CancellationToken)"/>
	public IAsyncEnumerable<ReadOnlyMemory<T>> ToAsyncBufferStream(
		CancellationToken cancellationToken = default)
		=> _core.ToAsyncBufferStream(cancellationToken);

	/// <inheritdoc cref="MappedQuery{T}.ToBufferedAsyncStream(CancellationToken)"/>
	public IAsyncEnumerable<T> ToBufferedAsyncStream(
		CancellationToken cancellationToken = default)
		=> _core.ToBufferedAsyncStream(cancellationToken);

	#endregion
}
