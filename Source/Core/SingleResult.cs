namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Opts an expressive command out of the read-through, before <c>Map</c>:
	/// <c>command.SingleResult().Map&lt;Post&gt;().ToListAsync(cancellationToken)</c>.
	/// The query reads only the first result set and stops,
	/// so a failure in a later statement is not seen.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A <c>Map</c> terminal returns the rows of the first result set
	/// (a <em>result</em> here is one whole result set, what <see cref="IDataReader.NextResult"/> moves between; never a row)
	/// and then reads through the later sets without reading their rows,
	/// so a failure in a later statement is thrown as the provider raised it.
	/// This is the one explicit opt-out of that.
	/// With it the reader is asked for <see cref="CommandBehavior.SingleResult"/>
	/// (and <see cref="CommandBehavior.SingleRow"/> too for <c>FirstOrDefault</c> when nothing is skipped),
	/// which lets the provider stop early.
	/// It asserts nothing: it does not check that there is only one result set.
	/// </para>
	/// <para>
	/// <c>command.SingleResult().Map&lt;T&gt;()</c> is exactly
	/// <c>command.Map&lt;T&gt;().SingleResult()</c>, which still works,
	/// and the five <c>Map</c> overloads that follow are the ones that
	/// <see cref="Map{T}(IExecuteReader, RowMap{T}?)"/> has.
	/// The command is not changed, so it can be used again without the opt-out.
	/// </para>
	/// <para>
	/// A later failure can follow a write that already committed,
	/// so a caller that retries can repeat it, and the opt-out hides that failure.
	/// </para>
	/// <para>
	/// The result is a view of the command for <c>Map</c>.
	/// Used as an <see cref="IExecuteReader"/> directly,
	/// it runs the command with <see cref="CommandBehavior.SingleResult"/> added to each call.
	/// A reader (<see cref="IDataReader"/>) and a <see cref="DataTable"/> have nothing to opt out of:
	/// their <c>Map</c> never reads through, so there is no <c>SingleResult</c> before it.
	/// </para>
	/// </remarks>
	/// <param name="command">The command to read from.</param>
	/// <returns>The command, to be followed by <c>Map</c>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IExecuteReader SingleResult(
		this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command is SingleResultReader
			? command
			: new SingleResultReader(command);
	}

	/// <summary>
	/// Opts a command out of the read-through, before <c>Map</c>:
	/// <c>command.SingleResult().Map&lt;Post&gt;().ToListAsync(cancellationToken)</c>.
	/// The query reads only the first result set and stops,
	/// so a failure in a later statement is not seen.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A <c>Map</c> terminal returns the rows of the first result set
	/// (a <em>result</em> here is one whole result set, what <see cref="IDataReader.NextResult"/> moves between; never a row)
	/// and then reads through the later sets without reading their rows,
	/// so a failure in a later statement is thrown as the provider raised it.
	/// This is the one explicit opt-out of that.
	/// It asserts nothing: it does not check that there is only one result set.
	/// </para>
	/// <para>
	/// <c>command.SingleResult().Map&lt;T&gt;()</c> is exactly
	/// <c>command.Map&lt;T&gt;().SingleResult()</c>, which still works,
	/// and the five <c>Map</c> overloads that follow are the ones that
	/// <see cref="Map{T}(IDbCommand, RowMap{T}?)"/> has.
	/// The command is not changed and belongs to the caller; the query never disposes it.
	/// </para>
	/// <para>
	/// A later failure can follow a write that already committed,
	/// so a caller that retries can repeat it, and the opt-out hides that failure.
	/// </para>
	/// <para>
	/// The result is a view of the command for <c>Map</c>:
	/// it is not a <see cref="DbCommand"/>, so pass the command itself to anything else.
	/// Used as an <see cref="IDbCommand"/> directly,
	/// it runs the command with <see cref="CommandBehavior.SingleResult"/> added to each reader it opens.
	/// </para>
	/// </remarks>
	/// <param name="command">The command to read from.</param>
	/// <returns>The command, to be followed by <c>Map</c>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static IDbCommand SingleResult(
		this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command is SingleResultDbCommand
			? command
			: new SingleResultDbCommand(command);
	}
}

// What `command.SingleResult()` returns for an expressive command:
// the flag, carried to the `Map` that follows.
// Map unwraps it and runs the query against the command itself
// with the core's singleResult flag set (see Views.QueryCore),
// so nothing here runs on that path.
// Used as an IExecuteReader on its own it is the command
// with SingleResult added to each behavior, so the name holds either way.
sealed class SingleResultReader(IExecuteReader command)
	: IExecuteReaderAsync
{
	internal IExecuteReader Command { get; } = command;

	public CancellationToken CancellationToken
		=> Command.CancellationToken;

	public bool UseAsyncRead
		=> Command is IExecuteReaderAsync { UseAsyncRead: true };

	public void ExecuteReader(
		Action<IDataReader> handler,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReader(
			handler,
			behavior | CommandBehavior.SingleResult);

	public T ExecuteReader<T>(
		Func<IDataReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReader(
			transform,
			behavior | CommandBehavior.SingleResult);

	public ValueTask ExecuteReaderAsync(
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReaderAsync(
			handler,
			behavior | CommandBehavior.SingleResult);

	public ValueTask<T> ExecuteReaderAsync<T>(
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReaderAsync(
			transform,
			behavior | CommandBehavior.SingleResult);

	public TResult ExecuteReader<TState, TResult>(
		TState state,
		Func<IDataReader, TState, TResult> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReader(
			state,
			transform,
			behavior | CommandBehavior.SingleResult);

	public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> Command.ExecuteReaderAsync(
			state,
			transform,
			behavior | CommandBehavior.SingleResult);

	public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(
		TState state,
		Func<IDataReader, TState, ValueTask<TResult>> transform,
		CommandBehavior behavior,
		CancellationToken cancellationToken)
		=> Command.ExecuteReaderAsync(
			state,
			transform,
			behavior | CommandBehavior.SingleResult,
			cancellationToken);
}

// What `command.SingleResult()` returns for a raw command:
// every member forwards to the caller's command,
// and Map unwraps it as it does the one above (see Views.CommandCore).
// The two ExecuteReader members add SingleResult to the behavior.
sealed class SingleResultDbCommand(IDbCommand command)
	: IDbCommand
{
	internal IDbCommand Command { get; } = command;

	public IDbConnection? Connection
	{
		get => Command.Connection;
		set => Command.Connection = value;
	}

	public IDbTransaction? Transaction
	{
		get => Command.Transaction;
		set => Command.Transaction = value;
	}

#if NET10_0_OR_GREATER
	[AllowNull]
#endif
	public string CommandText
	{
		get => Command.CommandText;
		set => Command.CommandText = value;
	}

	public int CommandTimeout
	{
		get => Command.CommandTimeout;
		set => Command.CommandTimeout = value;
	}

	public CommandType CommandType
	{
		get => Command.CommandType;
		set => Command.CommandType = value;
	}

	public IDataParameterCollection Parameters
		=> Command.Parameters;

	public UpdateRowSource UpdatedRowSource
	{
		get => Command.UpdatedRowSource;
		set => Command.UpdatedRowSource = value;
	}

	public void Cancel()
		=> Command.Cancel();

	public IDbDataParameter CreateParameter()
		=> Command.CreateParameter();

	public int ExecuteNonQuery()
		=> Command.ExecuteNonQuery();

	public IDataReader ExecuteReader()
		=> Command.ExecuteReader(CommandBehavior.SingleResult);

	public IDataReader ExecuteReader(CommandBehavior behavior)
		=> Command.ExecuteReader(behavior | CommandBehavior.SingleResult);

	public object? ExecuteScalar()
		=> Command.ExecuteScalar();

	public void Prepare()
		=> Command.Prepare();

	public void Dispose()
		=> Command.Dispose();
}
