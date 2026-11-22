namespace Open.Database.Extensions;

/// <summary>
/// A deferred query whose rows are mapped to <typeparamref name="T"/>. Nothing runs until one of the terminal methods is called.
/// </summary>
/// <include file="Mapped.docs.xml" path="docs/members[@name='row-type']/*" />
/// <remarks>
/// <para>
/// The <c>Read</c> and <c>ReadAsync</c> methods are the primitive terminals: they execute the command, hand the handler a forward-only stream of mapped rows backed by the open data reader, and release the connection when the handler returns. The convenience terminals in <see cref="MappedQueryExtensions"/> (for example <see cref="Synchronous.SyncMappedQueryExtensions.ToList{T}(IMappedQuery{T})"/> and <see cref="Synchronous.SyncMappedQueryExtensions.FirstOrDefault{T}(IMappedQuery{T})"/>) are built on them.
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
	/// Asynchronously executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="Mapped.docs.xml" path="docs/members[@name='handler-async']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <include file="Mapped.docs.xml" path="docs/members[@name='returns-task']/*" />
	/// <inheritdoc cref="Synchronous.IMappedQuerySync{T}.Read(Action{IEnumerable{T}})" path="/remarks"/>
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
	/// <inheritdoc cref="Synchronous.IMappedQuerySync{T}.Read(Action{IEnumerable{T}})" path="/remarks"/>
	ValueTask<TResult> ReadAsync<TResult>(Func<IAsyncEnumerable<T>, CancellationToken, ValueTask<TResult>> handler, CancellationToken cancellationToken);
}
