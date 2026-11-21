namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that read data from an
/// <see cref="IExecuteReader"/>.
/// </summary>
public static class IExecuteReaderExtensions
{
	const string LaterSets = "Map also reads the later result sets, so a failure in one is thrown; to read only the first, as this method does, call .SingleResult() before Map (cmd.SingleResult().Map(...); .Map(...).SingleResult() still works).";
	const string SkipDifference = "Map does not run the selector on skipped rows, so a selector that throws on one no longer fails the call.";
	const string ResultsObsolete = "Use .Map<T>().ToList() instead (.Map(map).ToList() when you pass a RowMap; Map does not accept a null map). " + LaterSets + " Map returns a List<T>. Removed in 12.";
	const string FirstOrDefaultObsolete = "Use .Map(selector).FirstOrDefault() instead. " + LaterSets + " Removed in 12.";
	const string SingleOrDefaultObsolete = "Use .Map(selector).SingleOrDefault() instead. " + LaterSets + " Removed in 12.";
	const string TakeObsolete = "Use .Map(selector).Take(count).ToList() instead. " + LaterSets + " Take(0) now runs the command; this method did not. Removed in 12.";
	const string SkipObsolete = "Use .Map(selector).Skip(count).ToList() instead. " + LaterSets + " " + SkipDifference + " Removed in 12.";
	const string SkipThenTakeObsolete = "Use .Map(selector).Skip(skip).Take(take).ToList() instead. " + LaterSets + " " + SkipDifference + " Take(0) now runs the command; this method did not. Removed in 12.";
	const string ToListObsolete = "Use .Map(transform).ToList() instead. " + LaterSets + " Map has no CommandBehavior argument: if you passed one, use cmd.ExecuteReader(r => r.Map(transform).ToList(), behavior | CommandBehavior.SingleResult). Removed in 12.";
	const string ToArrayObsolete = "Use .Map(transform).ToArray() instead. " + LaterSets + " Map has no CommandBehavior argument: if you passed one, use cmd.ExecuteReader(r => r.Map(transform).ToArray(), behavior | CommandBehavior.SingleResult). Removed in 12.";
	const string ToImmutableArrayObsolete = "Use .Map(transform).ToImmutableArray() instead. " + LaterSets + " Map has no CommandBehavior argument: if you passed one, use cmd.ExecuteReader(r => r.Map(transform).ToImmutableArray(), behavior | CommandBehavior.SingleResult). Removed in 12.";
	const string IterateReaderObsolete = "Use .Map(transform).Read(selector) instead. " + LaterSets + " Map has no CommandBehavior argument: if you passed one, use cmd.ExecuteReader(r => selector(r.Select(transform)), behavior | CommandBehavior.SingleResult). Removed in 12.";

	/// <summary>
	/// Calls <paramref name="handler"/> for each record of the first result set;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <note type="caution">
	/// <b>This is not the recommended way to read rows: it holds the
	/// connection while your code runs for each row.</b>
	/// The reader, and the connection under it, stay open for as long as
	/// your code takes per row, added up over all the rows.
	/// <br/>
	/// <br/>
	/// In async code that time has no bound: every <see langword="await"/> in the
	/// loop body can yield, and nothing says when the loop resumes.
	/// Until it does, the connection is on hold: a pooled connection is
	/// not returned to the pool, and the server may keep its locks and the
	/// open result set.
	/// <br/>
	/// <br/>
	/// These members suit work that must happen while the connection is
	/// open, for example a result too large to buffer or a row-by-row
	/// pipeline.
	/// Otherwise use a call that lets go of the connection first:
	/// <see cref="MappedQueryExtensions.ToListAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// and
	/// <see cref="MappedQueryExtensions.ToArrayAsync{T}(IMappedQuery{T}, CancellationToken)"/>
	/// buffer the rows and release the connection
	/// (call <c>.Map&lt;T&gt;()</c> first), and
	/// <see cref="MappedQuery{T}.ToBufferedAsyncStream(CancellationToken)"/>
	/// streams them to a consumer without holding it
	/// (the producer is unbounded: it reads without waiting for the
	/// consumer and frees the connection while the consumer takes its
	/// time, and the rows wait in memory until the consumer takes them;
	/// dispose the enumerator, as <see langword="await"/> <see langword="foreach"/> does).
	/// </note>
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="handler">The action called for each <see cref="IDataRecord"/>.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static void IterateReader(this IExecuteReader command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		command.ExecuteReader(
			reader => reader.ForEach(handler),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Calls <paramref name="handler"/> for each record of the first result set
	/// while it returns <see langword="true"/>;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <inheritdoc
	/// cref="IExecuteReaderExtensions.IterateReader(IExecuteReader, Action{IDataRecord}, CommandBehavior)"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="handler">The function called for each <see cref="IDataRecord"/>; it returns <see langword="true"/> to continue and <see langword="false"/> to stop.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static void IterateReaderWhile(this IExecuteReader command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		command.ExecuteReader(
			reader => reader.IterateWhile(handler),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Asynchronously calls <paramref name="handler"/> for each record of the
	/// first result set;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <inheritdoc
	/// cref="IExecuteReaderExtensions.IterateReader(IExecuteReader, Action{IDataRecord}, CommandBehavior)"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="handler">The action called for each <see cref="IDataRecord"/>.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token-behavior']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static ValueTask IterateReaderAsync(
		this IExecuteReaderAsync command,
		Action<IDataRecord> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return CancellationTokens.RunAsync(
			command.CancellationToken,
			cancellationToken,
			(command, handler, behavior),
			static (s, token)
				=> ValueTaskHelpers.Discard(s.command.ExecuteReaderAsync(
					(s.handler, s.command.UseAsyncRead, token),
					static async (reader, x) =>
					{
						if (reader is DbDataReader r)
						{
							await r
								.ForEachAsync(
									x.handler,
									x.UseAsyncRead,
									x.token)
								.ConfigureAwait(false);
						}
						else
						{
							reader.ForEach(x.handler, x.token);
						}

						return false;
					},
					s.behavior | CommandBehavior.SingleResult,
					token)));
	}

	/// <summary>
	/// Asynchronously calls <paramref name="handler"/> for each record of the
	/// first result set while it returns <see langword="true"/>;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <inheritdoc
	/// cref="IExecuteReaderExtensions.IterateReader(IExecuteReader, Action{IDataRecord}, CommandBehavior)"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="handler">The function called for each <see cref="IDataRecord"/>; it returns <see langword="true"/> to continue and <see langword="false"/> to stop.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token-behavior']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static ValueTask IterateReaderWhileAsync(
		this IExecuteReaderAsync command,
		Func<IDataRecord, bool> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return CancellationTokens.RunAsync(
			command.CancellationToken,
			cancellationToken,
			(command, handler, behavior),
			static (s, token)
				=> ValueTaskHelpers.Discard(s.command.ExecuteReaderAsync(
					(s.handler, s.command.UseAsyncRead, token),
					static async (reader, x) =>
					{
						if (reader is DbDataReader r)
						{
							await r
								.IterateWhileAsync(
									x.handler,
									x.UseAsyncRead,
									x.token)
								.ConfigureAwait(false);
						}
						else
						{
							reader.IterateWhile(x.handler, x.token);
						}

						return false;
					},
					s.behavior | CommandBehavior.SingleResult,
					token)));
	}

	/// <summary>
	/// Asynchronously calls <paramref name="handler"/> for each record of the
	/// first result set while its task produces <see langword="true"/>;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <inheritdoc
	/// cref="IExecuteReaderExtensions.IterateReader(IExecuteReader, Action{IDataRecord}, CommandBehavior)"
	/// path="/remarks"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="handler">The asynchronous function called for each <see cref="IDataRecord"/>; its task produces <see langword="true"/> to continue and <see langword="false"/> to stop.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token-behavior']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static ValueTask IterateReaderWhileAsync(
		this IExecuteReaderAsync command,
		Func<IDataRecord, ValueTask<bool>> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return CancellationTokens.RunAsync(
			command.CancellationToken,
			cancellationToken,
			(command, handler, behavior),
			static (s, token)
				=> ValueTaskHelpers.Discard(s.command.ExecuteReaderAsync(
					(s.handler, token),
					static async (reader, x) =>
					{
						await reader
							.IterateWhileAsync(x.handler, x.token)
							.ConfigureAwait(false);
						return false;
					},
					s.behavior | CommandBehavior.SingleResult,
					token)));
	}

	/// <summary>
	/// Obsolete: use <c>.Map(transform).Read(selector)</c>.
	/// Transforms each record and passes the sequence of results to
	/// <paramref name="selector"/>, returning its result.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <typeparam name="TEntity">The type the transform returns for each record.</typeparam>
	/// <typeparam name="TResult">The type returned by the selector.</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="transform">The function that converts each <see cref="IDataRecord"/> to a <typeparamref name="TEntity"/>.</param>
	/// <param name="selector">The function that receives the sequence of transformed records and produces the result.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <returns>The result of <paramref name="selector"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/>, <paramref name="transform"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
	[Obsolete(IterateReaderObsolete)]
	public static TResult IterateReader<TEntity, TResult>(
		this IExecuteReader command,
		Func<IDataRecord, TEntity> transform,
		Func<IEnumerable<TEntity>, TResult> selector, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => selector(reader.Select(transform)),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Returns the value the transform produces for the first record;
	/// throws if there is none.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <returns>The value from the transform.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="InvalidOperationException">The command returns no records.</exception>
	public static T First<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).First(),
			CommandBehavior.SingleRow | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/> then <see cref="MappedQuery{T}.FirstOrDefault"/>.
	/// Returns the value the transform produces for the first record,
	/// or <see langword="default"/> if there is none.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <returns>The value from the transform, or <see langword="default"/> if there is no record.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	[Obsolete(FirstOrDefaultObsolete)]
	public static T? FirstOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).FirstOrDefault(),
			CommandBehavior.SingleRow | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Returns the value the transform produces for the only record;
	/// throws if there is none or more than one.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <returns>The value from the transform.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="InvalidOperationException">The command returns no records, or more than one.</exception>
	public static T Single<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).Single(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/> then <see cref="MappedQuery{T}.SingleOrDefault"/>.
	/// Returns the value the transform produces for the only record,
	/// or <see langword="default"/> if there is none.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <returns>The value from the transform, or <see langword="default"/> if there is no record.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="InvalidOperationException">The command returns more than one record.</exception>
	[Obsolete(SingleOrDefaultObsolete)]
	public static T? SingleOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).SingleOrDefault(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Take(int)"/>, then <see cref="MappedQueryWithTake{T}.ToList"/>.
	/// Returns the values the transform produces for the first
	/// <paramref name="count"/> records.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <param name="count">The maximum number of records to return.</param>
	/// <returns>The results from the transform limited by the take count.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
	[Obsolete(TakeObsolete)]
	public static List<T> Take<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
		Contract.EndContractBlock();

		return count == 0 ? [] : command.ExecuteReader(
			reader => reader.Select(transform).Take(count).ToList(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Skip(int)"/>, then <see cref="MappedQueryWithSkip{T}.ToList"/>.
	/// Returns the values the transform produces for the records after the
	/// first <paramref name="count"/>.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <param name="count">The number of records to skip.</param>
	/// <returns>The results from the transform after the skip count.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
	[Obsolete(SkipObsolete)]
	public static List<T> Skip<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => count == 0
				? reader.Select(transform).ToList()
				: reader.Select(transform).Skip(count).ToList(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Skip(int)"/>, <see cref="MappedQueryWithSkip{T}.Take(int)"/> and <see cref="MappedQueryWithTake{T}.ToList"/>.
	/// Returns the values the transform produces for the records after the
	/// first <paramref name="skip"/>, at most <paramref name="take"/> of them.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <param name="skip">The number of records to skip before starting to take results.</param>
	/// <param name="take">The maximum number of records to return.</param>
	/// <returns>The results from the skip, transform and take operation.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="skip"/> or <paramref name="take"/> is negative.</exception>
	[Obsolete(SkipThenTakeObsolete)]
	public static List<T> SkipThenTake<T>(this IExecuteReader command, int skip, int take, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (skip < 0) throw new ArgumentOutOfRangeException(nameof(skip), skip, "Cannot be negative.");
		if (take < 0) throw new ArgumentOutOfRangeException(nameof(take), take, "Cannot be negative.");
		Contract.EndContractBlock();

		return take == 0
			? []
			: command.ExecuteReader(
				reader => reader.Select(transform).Skip(skip).Take(take).ToList(),
				CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <c>.Map(transform).ToList()</c>.
	/// Converts every record into a list item using a transform function.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <returns>The list of transformed records.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	[Obsolete(ToListObsolete)]
	public static List<T> ToList<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToList(),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <c>.Map(transform).ToArray()</c>.
	/// Converts every record into an array item using a transform function.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <returns>The array of transformed records.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	[Obsolete(ToArrayObsolete)]
	public static T[] ToArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToArray(),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <c>.Map(transform).ToImmutableArray()</c>.
	/// Converts every record into an item of an immutable array using a
	/// transform function.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command-transform']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior-single']/*" />
	/// <returns>The immutable array of transformed records.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	[Obsolete(ToImmutableArrayObsolete)]
	public static ImmutableArray<T> ToImmutableArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToImmutableArray(),
			behavior | CommandBehavior.SingleResult);
	}

	#region Async terminals that take the type: Map<T>() then the terminal

	// Only the async forms are on the command:
	// a synchronous caller writes command.Map<T>().ToList()
	// (the reader-level synchronous helpers are unchanged).
	// Each is one expression: no await, no state machine of its own.

	/// <summary>
	/// Reads every row into a list, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToListAsync(cancellationToken)</c>:
	/// a single value such as <see cref="int"/> reads the first column;
	/// any other <typeparamref name="T"/> maps by column name.
	/// </summary>
	/// <remarks>
	/// A single value (a primitive, an enum, <see cref="string"/>,
	/// <see cref="decimal"/>, <see cref="DateTime"/>,
	/// <see cref="DateTimeOffset"/>, <see cref="TimeSpan"/>,
	/// <see cref="Guid"/>, <c>byte[]</c>, <c>DateOnly</c>,
	/// <c>TimeOnly</c> or a <see cref="Nullable{T}"/> of one)
	/// is read from the first column of each row, whatever it is named;
	/// the other columns are ignored, so select only the one you need.
	/// The column is converted as the mapper converts a property of that type:
	/// its typed getter when the column type matches (no boxing),
	/// a range-checked integer conversion, else a general conversion.
	/// A NULL is <see langword="default"/> for a non-nullable value type
	/// (<c>0</c> for <see cref="int"/>)
	/// and <see langword="null"/> for <see cref="Nullable{T}"/>,
	/// <see cref="string"/> and <c>byte[]</c>;
	/// declare <c>int?</c> to tell NULL from 0.
	/// A value that cannot be converted throws <see cref="ValueMappingException"/>.
	/// Only the asynchronous terminals are on the command;
	/// for a synchronous read use <c>command.Map&lt;T&gt;().ToList()</c>.
	/// Pass the token or nothing:
	/// <c>ToListAsync&lt;int&gt;(default)</c> binds to the obsolete instance method
	/// <c>ToListAsync(transform, behavior)</c> of an expressive command,
	/// which is the one member that still wins over these extensions.
	/// </remarks>
	/// <typeparam name="T">The model type to map the rows to, or the single value to read from the first column.</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as a list.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped
	/// (an interface, an abstract class, ...):
	/// thrown at the call, before anything runs.
	/// </exception>
	/// <seealso cref="CoreExtensions.Map{T}(IExecuteReader)"/>
	public static ValueTask<List<T>> ToListAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToListAsync(cancellationToken);

	/// <summary>
	/// Reads every row into an array, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToArrayAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as an array.</returns>
	public static ValueTask<T[]> ToArrayAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToArrayAsync(cancellationToken);

	/// <summary>
	/// Reads every row into an immutable array, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToImmutableArrayAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as an immutable array.</returns>
	public static ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToImmutableArrayAsync(cancellationToken);

	/// <summary>
	/// Reads the first row, or <see langword="default"/> when there is none,
	/// asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().FirstOrDefaultAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The first row, or <see langword="default"/>.</returns>
	public static ValueTask<T?> FirstOrDefaultAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().FirstOrDefaultAsync(cancellationToken);

	/// <summary>
	/// Reads the only row, or <see langword="default"/> when there is none,
	/// asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().SingleOrDefaultAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The only row, or <see langword="default"/>.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is more than one row.
	/// </exception>
	public static ValueTask<T?> SingleOrDefaultAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().SingleOrDefaultAsync(cancellationToken);

	/// <summary>
	/// Reads the first row, asynchronously; throws when there is none.
	/// Equivalent to <c>command.Map&lt;T&gt;().FirstAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The first row.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is no row.
	/// </exception>
	public static ValueTask<T> FirstAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().FirstAsync(cancellationToken);

	/// <summary>
	/// Reads the only row, asynchronously; throws when there is none
	/// or more than one.
	/// Equivalent to <c>command.Map&lt;T&gt;().SingleAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The only row.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is no row, or more than one.
	/// </exception>
	public static ValueTask<T> SingleAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().SingleAsync(cancellationToken);

	/// <summary>
	/// Reads every row into a hash set, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToHashSetAsync(comparer, cancellationToken)</c>.
	/// The comparer comes first, as on <see cref="MappedQuery{T}"/>:
	/// pass the token by name,
	/// <c>ToHashSetAsync&lt;int&gt;(cancellationToken: ct)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="comparer">The comparer of the set, or <see langword="null"/> for the default.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as a hash set.</returns>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(
		this IExecuteReader command,
		IEqualityComparer<T>? comparer = null,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToHashSetAsync(comparer, cancellationToken);

	#endregion

	/// <summary>
	/// Reads every record of the first result set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the columns at the given ordinals from every record of the result
	/// set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<int> ordinals)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(ordinals),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the columns at the given ordinals from every record of the result
	/// set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, int n, params IEnumerable<int> others)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(n, others),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the named columns from every record of the first result set and
	/// returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="columnNames">The column names to select.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="columnNames"/> is <see langword="null"/>.</exception>
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<string> columnNames)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(columnNames),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the named columns from every record of the result set and returns
	/// the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="c">The first column name to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining column names to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, string c, params IEnumerable<string> others)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		//if (c is null) throw new ArgumentNullException(nameof(c));
		//if (others.Any(e => e is null)) throw new ArgumentNullException(nameof(c));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(c, others),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader)"/> then <see cref="MappedQuery{T}.ToList"/>.
	/// Iterates each record and maps the fields to type <typeparamref name="T"/>.
	/// The data is buffered in its entirety in a queue before the transform is
	/// applied for each iteration.
	/// </summary>
	/// <remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='obsolete-11']/*" />
	/// <para>
	/// Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped; this is thrown before the command runs.</exception>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsObsolete)]
	public static IEnumerable<T> Results<T>(this IExecuteReader command, RowMap<T>? map = null)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(
			reader => reader.ResultsBuffered(map),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record and returns the results as a
	/// list.
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The list of transformed records.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static IEnumerable<object?> FirstOrdinalResults(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record and returns the values cast to
	/// <typeparamref name="T0"/>.
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </summary>
	/// <typeparam name="T0">The type the values are cast to.</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The enumerable of cast values.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults<T0>(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Loads the records of the first result set into a <see cref="DataTable"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The resultant <see cref="DataTable"/>.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static DataTable LoadTable(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.ToDataTable(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Loads each result set of the command into its own <see cref="DataTable"/>.
	/// Calls <see cref="IDataReader.NextResult"/> to check for more results.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The resultant list of tables.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static List<DataTable> LoadTables(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.ToDataTables(),
			CommandBehavior.SequentialAccess);
	}
}
