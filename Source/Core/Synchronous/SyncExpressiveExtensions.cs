namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking calls on a command or a query definition that has a command
/// (<c>Command</c>, <c>StoredProcedure</c>, <c>Using(...)</c>): they hold the calling thread until the work is done.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
public static class SyncExpressiveExtensions
{
	/// <summary>
	/// Executes the command and passes it to the handler.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <param name="action">The function that uses the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
	public static void Execute<TCommand>(
		this IExecuteCommand<TCommand> command,
		Action<TCommand> action)
		where TCommand : IDbCommand
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		command.AsSync().Execute(action);
	}

	/// <summary>
	/// Executes the command, passes it to the transform and returns the result.
	/// </summary>
	/// <typeparam name="TCommand">The type of command.</typeparam>
	/// <typeparam name="T">The type returned.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="transform">The function that uses the command.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	public static T Execute<TCommand, T>(
		this IExecuteCommand<TCommand> command,
		Func<TCommand, T> transform)
		where TCommand : IDbCommand
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().Execute(transform);
	}

	/// <summary>
	/// Executes the command with <see cref="IDbCommand.ExecuteNonQuery"/>.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <returns>The number of rows affected, as the provider reports it.</returns>
	public static int ExecuteNonQuery(this IExecuteCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().Execute(static cmd => cmd.ExecuteNonQuery());
	}

	/// <summary>
	/// Executes the command with <see cref="IDbCommand.ExecuteScalar"/>.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <returns>The raw value the provider returns: <see cref="DBNull.Value"/> when the column is NULL and <see langword="null"/> when there is no row, which is how to tell the two apart. Use <c>ExecuteScalar&lt;T&gt;()</c> to convert it.</returns>
	public static object? ExecuteScalar(this IExecuteCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().Execute(static cmd => cmd.ExecuteScalar());
	}

	/// <summary>
	/// Executes the command with <see cref="IDbCommand.ExecuteScalar"/> and converts the value to the expected type, as <c>Map&lt;T&gt;()</c> converts a column.
	/// </summary>
	/// <typeparam name="T">The type to convert the value to.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <returns>
	/// The value, converted to <typeparamref name="T"/>: a value that is already a <typeparamref name="T"/> is returned as is, an integer is converted to another integer type with a range check, and so on for the other conversions <c>Map&lt;T&gt;()</c> makes.
	/// <see langword="default"/> when the result is NULL or there was no row.
	/// </returns>
	/// <exception cref="ValueMappingException">The value does not convert to <typeparamref name="T"/>, or is out of its range; the conversion's own error is the inner exception.</exception>
	public static T ExecuteScalar<T>(this IExecuteCommand command)
		=> RowMap<T>.ConvertScalar(command.ExecuteScalar());

	/// <summary>
	/// Executes the command with <see cref="IDbCommand.ExecuteScalar"/> and
	/// passes the value to a transform.
	/// </summary>
	/// <typeparam name="T">The type expected.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="transform">The function that converts the value. It receives <see langword="null"/> when the column is NULL or there is no row, never <see cref="DBNull"/>; any other value is passed unchanged.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	public static T ExecuteScalar<T>(
		this IExecuteCommand command,
		Func<object?, T> transform)
	{
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return transform(ScalarValue(command.ExecuteScalar()));
	}

	/// <summary>
	/// Executes the command with a return parameter, as
	/// <see cref="IDbCommand.ExecuteNonQuery"/> does, and returns the value of
	/// that parameter.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <returns>The value from the return parameter.</returns>
	public static object? ExecuteReturn(this IExecuteCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().Execute(static cmd =>
		{
			IDbDataParameter returnParameter = cmd.AddReturnParameter();
			cmd.ExecuteNonQuery();
			return returnParameter.Value;
		});
	}

	/// <summary>
	/// Executes the command with a return parameter, as
	/// <see cref="IDbCommand.ExecuteNonQuery"/> does, and returns the value of
	/// that parameter converted to <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The type to convert the value to.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <returns>
	/// The value from the return parameter, converted to <typeparamref name="T"/> as <c>Map&lt;T&gt;()</c> converts a column: a value that is already a <typeparamref name="T"/> is returned as is, and <see langword="default"/> when it is NULL.
	/// </returns>
	/// <exception cref="ValueMappingException">The value does not convert to <typeparamref name="T"/>, or is out of its range.</exception>
	public static T ExecuteReturn<T>(this IExecuteCommand command)
		=> RowMap<T>.ConvertScalar(command.ExecuteReturn());

	/// <summary>
	/// Executes a reader and passes it to the handler.
	/// </summary>
	/// <typeparam name="TReader">The type of reader.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="handler">The function that uses the reader.</param>
	/// <param name="behavior">The command behavior.</param>
	/// <exception cref="ArgumentNullException"><paramref name="handler"/> is <see langword="null"/>.</exception>
	public static void ExecuteReader<TReader>(
		this IExecuteReader<TReader> command,
		Action<TReader> handler,
		CommandBehavior behavior = CommandBehavior.Default)
		where TReader : IDataReader
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		command.AsSync().ExecuteReader(handler, behavior);
	}

	/// <summary>
	/// Executes a reader, passes it to the transform and returns the result.
	/// </summary>
	/// <typeparam name="TReader">The type of reader.</typeparam>
	/// <typeparam name="T">The type returned.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="transform">The function that uses the reader.</param>
	/// <param name="behavior">The command behavior.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	public static T ExecuteReader<TReader, T>(
		this IExecuteReader<TReader> command,
		Func<TReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		where TReader : IDataReader
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().ExecuteReader(transform, behavior);
	}

	/// <summary>
	/// Executes a reader, passes it and a state to the transform and returns the result.
	/// </summary>
	/// <typeparam name="TState">The type of the state.</typeparam>
	/// <typeparam name="TResult">The type returned.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="state">The state to pass to the transform.</param>
	/// <param name="transform">The function that uses the reader.</param>
	/// <param name="behavior">The command behavior.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
	public static TResult ExecuteReader<TState, TResult>(
		this IExecuteReader command,
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().ExecuteReader(state, transform, behavior);
	}

	/// <summary>
	/// Executes the command and passes it to the handler.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <param name="action">The function that uses the command.</param>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="action"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls; use the async call.</exception>
	public static void Execute(
		this IExecuteCommand command,
		Action<IDbCommand> action)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		command.AsSync().Execute(action);
	}

	/// <summary>
	/// Executes the command, passes it to the transform and returns the result.
	/// </summary>
	/// <typeparam name="T">The type returned.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="transform">The function that uses the command.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls; use the async call.</exception>
	public static T Execute<T>(
		this IExecuteCommand command,
		Func<IDbCommand, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().Execute(transform);
	}

	/// <summary>
	/// Executes a reader and passes it to the handler.
	/// </summary>
	/// <param name="command">The command to execute.</param>
	/// <param name="handler">The function that uses the reader.</param>
	/// <param name="behavior">The command behavior.</param>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls; use the async call.</exception>
	public static void ExecuteReader(
		this IExecuteReader command,
		Action<IDataReader> handler,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		command.AsSync().ExecuteReader(handler, behavior);
	}

	/// <summary>
	/// Executes a reader, passes it to the transform and returns the result.
	/// </summary>
	/// <typeparam name="T">The type returned.</typeparam>
	/// <param name="command">The command to execute.</param>
	/// <param name="transform">The function that uses the reader.</param>
	/// <param name="behavior">The command behavior.</param>
	/// <returns>The value returned from the transform.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>.</exception>
	/// <exception cref="NotSupportedException">The command does not support blocking calls; use the async call.</exception>
	public static T ExecuteReader<T>(
		this IExecuteReader command,
		Func<IDataReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().ExecuteReader(transform, behavior);
	}

	private static object? ScalarValue(object? value)
		=> value is DBNull ? null : value;
}
