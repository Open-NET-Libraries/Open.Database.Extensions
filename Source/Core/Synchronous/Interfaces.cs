namespace Open.Database.Extensions.Synchronous;

// The blocking calls of the library's own commands. They are internal: the public blocking calls are the extension
// methods in this namespace, which reach a command through SyncSupport.AsSync, so importing the namespace is the one
// opt-in and a type from outside the library cannot take part.

/// <summary>An <see cref="IExecuteReader"/> that can run a reader on the calling thread.</summary>
internal interface IExecuteReaderSync : IExecuteReader
{
	/// <summary>Executes a reader and passes it to the handler.</summary>
	void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>Executes a reader, passes it to the transform and returns the result.</summary>
	T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>Executes a reader, passes it and a state to the transform and returns the result.</summary>
	TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default);
}

/// <summary>An <see cref="IExecuteReader{TReader}"/> that can run a typed reader on the calling thread.</summary>
/// <typeparam name="TReader">The type of reader.</typeparam>
internal interface IExecuteReaderSync<out TReader> : IExecuteReaderSync, IExecuteReader<TReader>
	where TReader : IDataReader
{
	/// <summary>Executes a reader and passes it to the handler.</summary>
	void ExecuteReader(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default);

	/// <summary>Executes a reader, passes it to the transform and returns the result.</summary>
	T ExecuteReader<T>(Func<TReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);
}

/// <summary>An <see cref="IExecuteCommand"/> that can run work against its command on the calling thread.</summary>
internal interface IExecuteCommandSync : IExecuteCommand
{
	/// <summary>Executes the command and passes it to the handler.</summary>
	void Execute(Action<IDbCommand> action);

	/// <summary>Executes the command, passes it to the transform and returns the result.</summary>
	T Execute<T>(Func<IDbCommand, T> transform);
}

/// <summary>An <see cref="IExecuteCommand{TCommand}"/> that can run work against its typed command on the calling thread.</summary>
/// <typeparam name="TCommand">The type of command.</typeparam>
internal interface IExecuteCommandSync<out TCommand> : IExecuteCommandSync, IExecuteCommand<TCommand>
	where TCommand : IDbCommand
{
	/// <summary>Executes the command and passes it to the handler.</summary>
	void Execute(Action<TCommand> action);

	/// <summary>Executes the command, passes it to the transform and returns the result.</summary>
	T Execute<T>(Func<TCommand, T> transform);
}
