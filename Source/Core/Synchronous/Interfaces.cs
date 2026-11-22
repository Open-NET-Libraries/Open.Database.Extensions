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
