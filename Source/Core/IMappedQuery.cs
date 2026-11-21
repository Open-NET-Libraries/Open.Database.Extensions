namespace Open.Database.Extensions;

/// <summary>
/// A deferred query whose rows are mapped to <typeparamref name="T"/>. Nothing runs until one of the terminal methods is called.
/// </summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
/// <remarks>
/// <para>
/// The <c>Read</c> and <c>ReadAsync</c> methods are the primitive terminals: they execute the command, hand the handler a forward-only stream of mapped rows backed by the open data reader, and release the connection when the handler returns. The convenience terminals in <see cref="MappedQueryExtensions"/> (for example <see cref="MappedQueryExtensions.ToList{T}(IMappedQuery{T})"/> and <see cref="MappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>) are built on them.
/// </para>
/// <para>
/// Rows are read as the handler enumerates, so enumerate inside the handler and do not let the sequence escape it: the reader is disposed as soon as the handler returns. Each terminal call executes the command again.
/// </para>
/// <para>
/// The asynchronous terminals take a <see cref="CancellationToken"/>. A call is canceled when either that token or the cancellation token of the underlying command is canceled, and one combined token covers opening the connection, looking up stored-procedure parameter names, executing the command and reading the rows. Cancellation surfaces as <see cref="OperationCanceledException"/>.
/// </para>
/// </remarks>
/// <seealso cref="MappedQuery{T}"/>
/// <seealso cref="MappedQueryExtensions"/>
public interface IMappedQuery<T>
{
	/// <summary>
	/// Executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-sync']/*" />
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
	/// buffer the rows and release the connection, and
	/// <see cref="MappedQuery{T}.ToBufferedAsyncStream(CancellationToken)"/>
	/// streams them to a consumer without holding it
	/// (the producer is unbounded: it reads without waiting for the
	/// consumer and frees the connection while the consumer takes its
	/// time, and the rows wait in memory until the consumer takes them;
	/// dispose the enumerator, as <see langword="await"/> <see langword="foreach"/> does).
	/// </note>
	/// <para>
	/// The connection is opened if it is not already open and is released when the handler returns, whether the handler completes or throws. Exceptions from the provider propagate unchanged.
	/// </para>
	/// </remarks>
	void Read(Action<IEnumerable<T>> handler);

	/// <summary>
	/// Executes the query, passes the mapped rows to
	/// <paramref name="handler"/> as they are read,
	/// and returns the handler's result;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-result']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-sync']/*" />
	/// <returns>The value returned by <paramref name="handler"/>. Materialize it before returning (for example into a list) rather than returning the lazy sequence.</returns>
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler);

	/// <summary>
	/// Asynchronously executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-async']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	ValueTask ReadAsync(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask> handler, CancellationToken cancellationToken);

	/// <summary>
	/// Asynchronously executes the query, passes the mapped rows to
	/// <paramref name="handler"/> as they are read,
	/// and returns the handler's result;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-result']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-async']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='returns-task-result']/*" />
	/// <inheritdoc cref="Read(Action{IEnumerable{T}})" path="/remarks"/>
	ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken);
}
