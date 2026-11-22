namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// An <see cref="IExecuteReader"/> that can also run a reader on the calling thread.
/// </summary>
/// <remarks>
/// <para>
/// The sync calls (<c>ExecuteReader</c> with a handler or a transform) are extensions in
/// <c>Open.Database.Extensions.Synchronous</c>, and they block the calling thread until the reader is done.
/// A command of this library already implements this interface.
/// A type of your own that implements <see cref="IExecuteReader"/> adds this one to take part in them;
/// without it a sync call on that type throws <see cref="NotSupportedException"/>, whose message names this interface.
/// </para>
/// </remarks>
public interface IExecuteReaderSync : IExecuteReader
{
	/// <summary>
	/// Executes the command and passes the data reader to <paramref name="handler"/>.
	/// </summary>
	/// <param name="handler">The function that processes the data reader.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes the command, passes the data reader to <paramref name="transform"/> and returns its result.
	/// </summary>
	/// <typeparam name="T">The type the transform returns.</typeparam>
	/// <param name="transform">The function that processes the data reader and produces the result.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <returns>The result of <paramref name="transform"/>.</returns>
	T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes the command, passes the data reader and <paramref name="state"/> to <paramref name="transform"/>,
	/// and returns its result.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='reader-state-transform']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default);
}

/// <summary>
/// An <see cref="IExecuteReader{TReader}"/> that can also run a reader on the calling thread, with the typed reader.
/// </summary>
/// <typeparam name="TReader">The type of the reader the handler receives.</typeparam>
/// <seealso cref="IExecuteReaderSync"/>
public interface IExecuteReaderSync<out TReader> : IExecuteReaderSync, IExecuteReader<TReader>
	where TReader : IDataReader
{
	/// <summary>
	/// Executes the command and passes the typed data reader to <paramref name="handler"/>.
	/// </summary>
	/// <param name="handler">The function that processes the data reader.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	void ExecuteReader(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>
	/// Executes the command, passes the typed data reader to <paramref name="transform"/> and returns its result.
	/// </summary>
	/// <typeparam name="T">The type the transform returns.</typeparam>
	/// <param name="transform">The function that processes the data reader and produces the result.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <returns>The result of <paramref name="transform"/>.</returns>
	T ExecuteReader<T>(Func<TReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);
}

/// <summary>
/// An <see cref="IExecuteCommand"/> that can also run work on the calling thread.
/// </summary>
/// <remarks>
/// The sync calls (<c>Execute</c>, <c>ExecuteNonQuery</c>, <c>ExecuteScalar</c>, <c>ExecuteReturn</c>) are extensions in
/// <c>Open.Database.Extensions.Synchronous</c>, and they block the calling thread.
/// A command of this library already implements this interface. A type of your own that implements <see cref="IExecuteCommand"/>
/// adds it to take part; without it a sync call on that type throws <see cref="NotSupportedException"/>, whose message names this interface.
/// </remarks>
public interface IExecuteCommandSync : IExecuteCommand
{
	/// <summary>
	/// Creates a command and passes it to <paramref name="action"/>.
	/// </summary>
	/// <param name="action">The action that operates on the command.</param>
	void Execute(Action<IDbCommand> action);

	/// <summary>
	/// Creates a command and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="transform">The function that operates on the command.</param>
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	T Execute<T>(Func<IDbCommand, T> transform);
}

/// <summary>
/// An <see cref="IExecuteCommand{TCommand}"/> that can also run work on the calling thread, with the typed command.
/// </summary>
/// <typeparam name="TCommand">The type of the command the function receives.</typeparam>
/// <seealso cref="IExecuteCommandSync"/>
public interface IExecuteCommandSync<out TCommand> : IExecuteCommandSync, IExecuteCommand<TCommand>
	where TCommand : IDbCommand
{
	/// <summary>
	/// Creates a command and passes it to <paramref name="action"/>.
	/// </summary>
	/// <param name="action">The action that operates on the typed command.</param>
	void Execute(Action<TCommand> action);

	/// <summary>
	/// Creates a command and passes it to <paramref name="transform"/>, returning its result.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="transform">The function that operates on the typed command.</param>
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	T Execute<T>(Func<TCommand, T> transform);
}

/// <summary>
/// An <see cref="IMappedQuery{T}"/> that can also read its rows on the calling thread.
/// </summary>
/// <typeparam name="T">The type of a mapped row.</typeparam>
/// <remarks>
/// The sync terminals (<c>ToList</c>, <c>First</c> and the others) are extensions in <c>Open.Database.Extensions.Synchronous</c>,
/// and they block the calling thread. The queries of this library implement this interface. A type of your own that implements
/// <see cref="IMappedQuery{T}"/> adds it to take part; without it a sync terminal on that type throws
/// <see cref="NotSupportedException"/>, whose message names this interface.
/// </remarks>
public interface IMappedQuerySync<T> : IMappedQuery<T>
{
	/// <summary>
	/// Executes the query and passes the mapped rows to
	/// <paramref name="handler"/> as they are read;
	/// the connection is held while each row is processed.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-sync']/*" />
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
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-result']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='handler-sync']/*" />
	/// <returns>The value returned by <paramref name="handler"/>. Materialize it before returning (for example into a list) rather than returning the lazy sequence.</returns>
	/// <inheritdoc cref="IMappedQuerySync{T}.Read(Action{IEnumerable{T}})" path="/remarks"/>
	TResult Read<TResult>(Func<IEnumerable<T>, TResult> handler);
}
