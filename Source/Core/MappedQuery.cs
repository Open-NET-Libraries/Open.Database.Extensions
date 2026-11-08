namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Locks in a mapping to model type <typeparamref name="T"/> and returns a deferred
	/// <see cref="MappedQuery{T}"/>. Nothing runs until a terminal (<c>ToList</c>/<c>FirstOrDefault</c>/…)
	/// is invoked. The mapping is compiled once per result-set shape and cached, so repeated queries pay no
	/// reflection or compilation cost; values are read with typed getters, so nothing is boxed.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Pass a <see cref="RowMap{T}"/> to read columns whose names differ from the properties; the overload without a map reads by column name.
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call
	/// (about 29 KB and a reader compile for a one-row read of a 12-property model, against 464 B through a declared map; see
	/// <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/mapping.html#columns-named-differently">Columns named differently</see>).
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The model type to map the values to.</typeparam>
	/// <param name="command">The command to read from.</param>
	/// <param name="map">The prebuilt <see cref="RowMap{T}"/> to read with.</param>
	/// <returns>A deferred query whose results are mapped to <typeparamref name="T"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="map"/> is <see langword="null"/>.</exception>
	/// <seealso cref="RowMap{T}"/>
	[OverloadResolutionPriority(1)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		RowMap<T> map)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (map is null) throw new ArgumentNullException(nameof(map));
		Contract.EndContractBlock();

		return new MappedQuery<T>(new MappedQueryCore<T>(command, map, 0, -1));
	}

	/// <inheritdoc cref="Map{T}(IExecuteReader, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="command">The command to read from.</param>
	/// <param name="mappings">The (property name, column name) pairs of a new <see cref="RowMap{T}"/>, built on every call; none reads by column name.</param>
	[OverloadResolutionPriority(2)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> Map(command, RowMap<T>.From(mappings));

	/// <inheritdoc cref="Map{T}(IExecuteReader, RowMap{T}?)" path="/*[not(self::param)]"/>
	/// <param name="command">The command to read from.</param>
	/// <param name="mapping">The one (property name, column name) pair of a new <see cref="RowMap{T}"/>, built on every call.</param>
	[OverloadResolutionPriority(3)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		KeyValuePair<string, string?> mapping)
		=> Map(command, new RowMap<T>(mapping));

	/// <inheritdoc cref="Map{T}(IExecuteReader, RowMap{T}?)"/>
	[OverloadResolutionPriority(4)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command)
		=> Map(command, RowMap<T>.Default);

	/// <summary>
	/// Locks in a custom projection and returns a deferred <see cref="MappedQuery{T}"/>. Nothing runs until
	/// a terminal is invoked; <paramref name="selector"/> is applied per row as the reader streams.
	/// </summary>
	/// <typeparam name="T">The type each record is projected to.</typeparam>
	/// <param name="command">The command to read from.</param>
	/// <param name="selector">The transform applied to each <see cref="IDataRecord"/>.</param>
	/// <returns>A deferred query whose results are produced by <paramref name="selector"/>.</returns>
	[OverloadResolutionPriority(-1)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		Func<IDataRecord, T> selector)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return new MappedQuery<T>(new MappedQueryCore<T>(command, new SelectorSource<T>(selector), 0, -1));
	}
}

// The shared implementation: holds the command handle (any IExecuteReader), how rows are mapped, and the
// pending skip/take. The execution itself is the command's own ExecuteReader/ExecuteReaderAsync.
// Terminals run the read loops in RowReader directly against the live reader (real async when the reader
// is a DbDataReader). Skipped rows are advanced without mapping; a take stops reading once satisfied.
readonly struct MappedQueryCore<T>
{
	// Generated mappings read each column once, in order, so the provider may stream the row.
	// A caller-supplied selector may read in any order, so it runs with the default access mode.
	const CommandBehavior GeneratedMapping = CommandBehavior.SingleResult | CommandBehavior.SequentialAccess;

	readonly IExecuteReader _command;
	readonly IRowSource<T> _source;
	readonly int _skip;
	readonly int _take; // -1 == unbounded.

	internal MappedQueryCore(IExecuteReader command, IRowSource<T> source, int skip, int take)
	{ _command = command; _source = source; _skip = skip; _take = take; }

	CommandBehavior Behavior => _source is SelectorSource<T> ? CommandBehavior.SingleResult : GeneratedMapping;

	internal MappedQueryCore<T> WithSkip(int count)
		=> count >= 0 ? new(_command, _source, _skip + count, _take)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	// Take is set exactly once (the staging structs only expose it before any take exists), so this
	// just records the limit; it applies after the skip, i.e. Enumerable.Skip(a).Take(b).
	internal MappedQueryCore<T> WithTake(int count)
		=> count >= 0 ? new(_command, _source, _skip, count)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	// Honors the command's UseAsyncRead choice (default false: rows are pulled with Read() after an async execute).
	bool UseAsyncRead => _command is IExecuteReaderAsync { UseAsyncRead: true };

	// One token governs a whole call: the one passed to the terminal together with the command's own (either cancels), for the connection open, the name lookup, the execute and the reads.
	// No token, one token, or the same token twice needs no source, so those calls allocate nothing for it. Two different tokens need a linked source, which the call disposes when it ends.
	CancellationToken Combine(CancellationToken cancellationToken, out CancellationTokenSource? linked)
		=> CancellationTokens.Combine(_command.CancellationToken, cancellationToken, out linked);

	// `token` is the combined token; it is also in `state`, which the terminal's static lambda reads it from.
	ValueTask<TResult> ExecuteAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationTokenSource? linked, CancellationToken token)
		=> linked is null
			? _command.ExecuteReaderAsync(state, transform, behavior, token)
			: ExecuteLinkedAsync(_command, state, transform, behavior, linked, token);

	static async ValueTask<TResult> ExecuteLinkedAsync<TState, TResult>(IExecuteReader command, TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationTokenSource linked, CancellationToken token)
	{
		using (linked)
			return await command.ExecuteReaderAsync(state, transform, behavior, token).ConfigureAwait(false);
	}

	// The terminals pass their inputs to the command as state and use static lambdas, so running a
	// query allocates no closures or delegates of its own.

	#region Handler (streaming) terminals

	internal void Read(Action<IEnumerable<T>> handler)
		=> _command.ExecuteReader((handler, _source, _skip, _take), static (reader, s) =>
		{
			s.handler(RowReader.Stream(s._source, reader, s._skip, s._take));
			return false;
		}, Behavior);

	internal TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
		=> _command.ExecuteReader((handler, _source, _skip, _take), static (reader, s)
			=> s.handler(RowReader.Stream(s._source, reader, s._skip, s._take)), Behavior);

	internal ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ValueTaskHelpers.Discard(ExecuteAsync((handler, _source, _skip, _take, UseAsyncRead, token), static async (reader, s) =>
		{
			await s.handler(StreamAsync(s._source, reader, s._skip, s._take, s.UseAsyncRead, s.token), s.token).ConfigureAwait(false);
			return false;
		}, Behavior, linked, token));
	}

	internal ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((handler, _source, _skip, _take, UseAsyncRead, token), static (reader, s)
			=> s.handler(StreamAsync(s._source, reader, s._skip, s._take, s.UseAsyncRead, s.token), s.token), Behavior, linked, token);
	}

	static IAsyncEnumerable<T> StreamAsync(IRowSource<T> source, IDataReader reader, int skip, int take, bool useAsync, CancellationToken token)
		=> reader is DbDataReader db
			? RowReader.StreamAsync(source, db, skip, take, useAsync, token)
			: RowReader.Stream(source, reader, skip, take).ToAsyncEnumerable();

	#endregion

	#region Direct terminals

	internal List<T> ToList()
		=> _command.ExecuteReader((_source, _skip, _take), static (reader, s)
			=> RowReader.ToList(s._source, reader, s._skip, s._take), Behavior);

	internal T[] ToArray()
		=> _command.ExecuteReader((_source, _skip, _take), static (reader, s)
			=> RowReader.ToArray(s._source, reader, s._skip, s._take), Behavior);

	internal T? FirstOrDefault()
		=> _command.ExecuteReader((_source, _skip, _take), static (reader, s)
			=> RowReader.TryFirst(s._source, reader, s._skip, s._take, out T? item) ? item : default, FirstBehavior);

	// SingleRow tells the provider only one row is needed; it can't be used when rows must be skipped first.
	CommandBehavior FirstBehavior => _skip == 0 ? Behavior | CommandBehavior.SingleRow : Behavior;

	internal T? SingleOrDefault()
		=> _command.ExecuteReader((_source, _skip, _take), static (reader, s)
			=> RowReader.TrySingle(s._source, reader, s._skip, s._take, out T? item) ? item : default, Behavior);

	internal ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, UseAsyncRead, token), static (reader, s)
			=> reader is DbDataReader db
				? RowReader.ToListAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, s.token)
				: new ValueTask<List<T>>(RowReader.ToList(s._source, reader, s._skip, s._take)), Behavior, linked, token);
	}

	internal ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, UseAsyncRead, token), static (reader, s)
			=> reader is DbDataReader db
				? RowReader.ToArrayAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, s.token)
				: new ValueTask<T[]>(RowReader.ToArray(s._source, reader, s._skip, s._take)), Behavior, linked, token);
	}

	internal ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, UseAsyncRead, token), static async (reader, s) =>
		{
			if (reader is DbDataReader db)
			{
				(bool found, T? item) = await RowReader.TryFirstAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, s.token).ConfigureAwait(false);
				return found ? item : default;
			}

			return RowReader.TryFirst(s._source, reader, s._skip, s._take, out T? i) ? i : default;
		}, FirstBehavior, linked, token);
	}

	internal ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, UseAsyncRead, token), static async (reader, s) =>
		{
			if (reader is DbDataReader db)
			{
				(bool found, T? item) = await RowReader.TrySingleAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, s.token).ConfigureAwait(false);
				return found ? item : default;
			}

			return RowReader.TrySingle(s._source, reader, s._skip, s._take, out T? i) ? i : default;
		}, Behavior, linked, token);
	}

	internal ImmutableArray<T> ToImmutableArray()
	{
		T[] array = ToArray();
#if NET8_0_OR_GREATER
		return System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsImmutableArray(array);
#else
		return ImmutableArray.Create(array);
#endif
	}

	internal async ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken)
	{
		T[] array = await ToArrayAsync(cancellationToken).ConfigureAwait(false);
#if NET8_0_OR_GREATER
		return System.Runtime.InteropServices.ImmutableCollectionsMarshal.AsImmutableArray(array);
#else
		return ImmutableArray.Create(array);
#endif
	}

	internal Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();

		return _command.ExecuteReader((_source, _skip, _take, keySelector, comparer), static (reader, s) =>
		{
			var sink = new DictionarySink<T, TKey>([with(s.comparer)], s.keySelector);
			RowReader.Fill(s._source, reader, s._skip, s._take, ref sink);
			return sink.Dictionary;
		}, Behavior);
	}

	internal ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken)
		where TKey : notnull
	{
		if (keySelector is null) throw new ArgumentNullException(nameof(keySelector));
		Contract.EndContractBlock();

		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, keySelector, comparer, UseAsyncRead, token), static async (reader, s) =>
		{
			var sink = new DictionarySink<T, TKey>([with(s.comparer)], s.keySelector);
			if (reader is DbDataReader db)
				return (await RowReader.FillAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, sink, s.token).ConfigureAwait(false)).Dictionary;

			RowReader.Fill(s._source, reader, s._skip, s._take, ref sink);
			return sink.Dictionary;
		}, Behavior, linked, token);
	}

	internal HashSet<T> ToHashSet(IEqualityComparer<T>? comparer)
		=> _command.ExecuteReader((_source, _skip, _take, comparer), static (reader, s) =>
		{
			var sink = new HashSetSink<T>([with(s.comparer)]);
			RowReader.Fill(s._source, reader, s._skip, s._take, ref sink);
			return sink.Set;
		}, Behavior);

	internal ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer, CancellationToken cancellationToken)
	{
		CancellationToken token = Combine(cancellationToken, out CancellationTokenSource? linked);
		return ExecuteAsync((_source, _skip, _take, comparer, UseAsyncRead, token), static async (reader, s) =>
		{
			var sink = new HashSetSink<T>([with(s.comparer)]);
			if (reader is DbDataReader db)
				return (await RowReader.FillAsync(s._source, db, s._skip, s._take, s.UseAsyncRead, sink, s.token).ConfigureAwait(false)).Set;

			RowReader.Fill(s._source, reader, s._skip, s._take, ref sink);
			return sink.Set;
		}, Behavior, linked, token);
	}

	#endregion
}

/// <summary>A deferred, type-locked query over any <see cref="IExecuteReader"/>. Terminals are methods here, with more as extensions on <see cref="IMappedQuery{T}"/>.</summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
public readonly record struct MappedQuery<T> : IMappedQuery<T>
{
	readonly MappedQueryCore<T> _core;
	internal MappedQuery(MappedQueryCore<T> core) => _core = core;

	/// <summary>Skips the first <paramref name="count"/> mapped records.</summary>
	public MappedQueryWithSkip<T> Skip(int count) => new(_core.WithSkip(count));

	/// <summary>Limits the result to at most <paramref name="count"/> mapped records.</summary>
	public MappedQueryWithTake<T> Take(int count) => new(_core.WithTake(count));

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
	/// <inheritdoc cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToArray{T}(IMappedQuery{T})"/>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})"/>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})"/>
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);
}

/// <summary>A <see cref="MappedQuery{T}"/> with a pending skip; can still be limited with <see cref="Take(int)"/>.</summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
public readonly record struct MappedQueryWithSkip<T> : IMappedQuery<T>
{
	readonly MappedQueryCore<T> _core;
	internal MappedQueryWithSkip(MappedQueryCore<T> core) => _core = core;

	/// <summary>Limits the result to at most <paramref name="count"/> mapped records.</summary>
	public MappedQueryWithTake<T> Take(int count) => new(_core.WithTake(count));

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
	/// <inheritdoc cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToArray{T}(IMappedQuery{T})"/>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})"/>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})"/>
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);
}

/// <summary>A fully-configured <see cref="MappedQuery{T}"/> with a take (and any preceding skip); terminal-only.</summary>
/// <typeparam name="T">The type each record is mapped to.</typeparam>
public readonly record struct MappedQueryWithTake<T> : IMappedQuery<T>
{
	readonly MappedQueryCore<T> _core;
	internal MappedQueryWithTake(MappedQueryCore<T> core) => _core = core;

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
	/// <inheritdoc cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default) => _core.ToListAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToArray{T}(IMappedQuery{T})"/>
	public T[] ToArray() => _core.ToArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T[]> ToArrayAsync(CancellationToken cancellationToken = default) => _core.ToArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>
	public T? FirstOrDefault() => _core.FirstOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.FirstOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default) => _core.FirstOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefault{T}(IMappedQuery{T})"/>
	public T? SingleOrDefault() => _core.SingleOrDefault();
	/// <inheritdoc cref="MappedQueryExtensions.SingleOrDefaultAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default) => _core.SingleOrDefaultAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArray{T}(IMappedQuery{T})"/>
	public ImmutableArray<T> ToImmutableArray() => _core.ToImmutableArray();
	/// <inheritdoc cref="MappedQueryExtensions.ToImmutableArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	public ValueTask<ImmutableArray<T>> ToImmutableArrayAsync(CancellationToken cancellationToken = default) => _core.ToImmutableArrayAsync(cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionary{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey})"/>
	public Dictionary<TKey, T> ToDictionary<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null) where TKey : notnull => _core.ToDictionary(keySelector, comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToDictionaryAsync{T, TKey}(IMappedQuery{T}, Func{T, TKey}, IEqualityComparer{TKey}, CancellationToken)"/>
	public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer = null, CancellationToken cancellationToken = default) where TKey : notnull => _core.ToDictionaryAsync(keySelector, comparer, cancellationToken);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSet{T}(IMappedQuery{T}, IEqualityComparer{T})"/>
	public HashSet<T> ToHashSet(IEqualityComparer<T>? comparer = null) => _core.ToHashSet(comparer);
	/// <inheritdoc cref="MappedQueryExtensions.ToHashSetAsync{T}(IMappedQuery{T}, IEqualityComparer{T}, CancellationToken)"/>
	public ValueTask<HashSet<T>> ToHashSetAsync(IEqualityComparer<T>? comparer = null, CancellationToken cancellationToken = default) => _core.ToHashSetAsync(comparer, cancellationToken);
}
