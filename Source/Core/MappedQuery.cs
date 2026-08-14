namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Locks in a reflection-based mapping to model type <typeparamref name="T"/> and returns a deferred
	/// <see cref="MappedQuery{T}"/>. Nothing runs until a terminal (an <see cref="IMappedQuery{T}"/>
	/// extension such as <c>ToList</c>/<c>FirstOrDefault</c>) is invoked; the mapping is applied per row
	/// as the reader streams, so terminals read only the rows they need.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="command">The command to read from.</param>
	/// <param name="fieldMappingOverrides">
	/// An override map of field (property) names to column names; a null column value ignores that field.
	/// </param>
	/// <returns>A deferred query whose results are mapped to <typeparamref name="T"/>.</returns>
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		params IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
		where T : new()
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return new MappedQuery<T>(new MappedQueryCore<T>(
			command,
			reader => reader.Results<T>(fieldMappingOverrides),
			(reader, token) => Transformer<T>.Create(fieldMappingOverrides).ResultsAsync(reader, token),
			0, -1));
	}

	/// <inheritdoc cref="Map{T}(IExecuteReader, IEnumerable{KeyValuePair{string, string?}}?)"/>
	[OverloadResolutionPriority(1)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command)
		where T : new()
		=> command.Map<T>(null);

	/// <inheritdoc cref="Map{T}(IExecuteReader, IEnumerable{KeyValuePair{string, string?}}?)"/>
	[OverloadResolutionPriority(-2)]
	public static MappedQuery<T> Map<T>(
		this IExecuteReader command,
		params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		where T : new()
		=> command.Map<T>(fieldMappingOverrides?.Select(mapping => new KeyValuePair<string, string?>(mapping.Field, mapping.Column)));

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

		return new MappedQuery<T>(new MappedQueryCore<T>(
			command,
			reader => reader.Select(selector),
			(reader, token) => reader.SelectAsync(selector, false, token),
			0, -1));
	}
}

// The shared implementation: holds the command handle (any IExecuteReader), the sync/async projections
// captured at Map-time, and the pending skip/take. The execution itself is the command's own
// ExecuteReader/ExecuteReaderAsync — this type never touches a reader directly. Real async streaming when
// the live reader is a DbDataReader; otherwise the synchronous projection is presented as an async sequence.
readonly struct MappedQueryCore<T>
{
	readonly IExecuteReader _command;
	readonly Func<IDataReader, IEnumerable<T>> _project;
	readonly Func<DbDataReader, CancellationToken, IAsyncEnumerable<T>> _projectAsync;
	readonly int _skip;
	readonly int _take; // -1 == unbounded.

	internal MappedQueryCore(
		IExecuteReader command,
		Func<IDataReader, IEnumerable<T>> project,
		Func<DbDataReader, CancellationToken, IAsyncEnumerable<T>> projectAsync,
		int skip, int take)
	{ _command = command; _project = project; _projectAsync = projectAsync; _skip = skip; _take = take; }

	internal MappedQueryCore<T> WithSkip(int count)
		=> count >= 0 ? new(_command, _project, _projectAsync, _skip + count, _take)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	// Take is set exactly once (the staging structs only expose it before any take exists), so this
	// just records the limit; Slice applies it after the skip, i.e. Enumerable.Skip(a).Take(b).
	internal MappedQueryCore<T> WithTake(int count)
		=> count >= 0 ? new(_command, _project, _projectAsync, _skip, count)
			: throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");

	internal void Read(Action<IEnumerable<T>> handler)
	{
		Func<IDataReader, IEnumerable<T>> project = _project;
		int skip = _skip, take = _take;
		_command.ExecuteReader(reader => handler(Slice(project(reader), skip, take)), CommandBehavior.SingleResult);
	}

	internal TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler)
	{
		Func<IDataReader, IEnumerable<T>> project = _project;
		int skip = _skip, take = _take;
		return _command.ExecuteReader(reader => handler(Slice(project(reader), skip, take)), CommandBehavior.SingleResult);
	}

	internal ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken)
	{
		Func<IDataReader, IEnumerable<T>> project = _project;
		Func<DbDataReader, CancellationToken, IAsyncEnumerable<T>> projectAsync = _projectAsync;
		int skip = _skip, take = _take;
		CancellationToken token = cancellationToken.CanBeCanceled ? cancellationToken : _command.CancellationToken;
		return _command.ExecuteReaderAsync(reader => handler(SliceAsync(project, projectAsync, skip, take, reader, token), token), CommandBehavior.SingleResult);
	}

	internal ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken)
	{
		Func<IDataReader, IEnumerable<T>> project = _project;
		Func<DbDataReader, CancellationToken, IAsyncEnumerable<T>> projectAsync = _projectAsync;
		int skip = _skip, take = _take;
		CancellationToken token = cancellationToken.CanBeCanceled ? cancellationToken : _command.CancellationToken;
		return _command.ExecuteReaderAsync(reader => handler(SliceAsync(project, projectAsync, skip, take, reader, token), token), CommandBehavior.SingleResult);
	}

	static IEnumerable<T> Slice(IEnumerable<T> sequence, int skip, int take)
	{
		if (skip > 0) sequence = sequence.Skip(skip);
		if (take >= 0) sequence = sequence.Take(take);
		return sequence;
	}

	static IAsyncEnumerable<T> SliceAsync(
		Func<IDataReader, IEnumerable<T>> project,
		Func<DbDataReader, CancellationToken, IAsyncEnumerable<T>> projectAsync,
		int skip, int take, IDataReader reader, CancellationToken token)
	{
		IAsyncEnumerable<T> sequence = reader is DbDataReader dbReader
			? projectAsync(dbReader, token)
			: project(reader).ToAsyncEnumerable();
		if (skip > 0) sequence = sequence.Skip(skip);
		if (take >= 0) sequence = sequence.Take(take);
		return sequence;
	}
}

/// <summary>A deferred, type-locked query over any <see cref="IExecuteReader"/>. Terminals are extension methods on <see cref="IMappedQuery{T}"/>.</summary>
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
}
