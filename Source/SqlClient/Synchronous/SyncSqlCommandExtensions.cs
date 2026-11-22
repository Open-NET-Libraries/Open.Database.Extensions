namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking calls on a <see cref="SqlCommand"/>: they hold the calling thread until the work is done.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
[Obsolete(SqlClientObsolete._message)]
public static class SyncSqlCommandExtensions
{
	// Opens a closed connection and executes the reader in one step; Core's shared helpers close a connection opened here when the
	// provider fails to execute. The execute delegate is a static lambda, so there is no closure.
	static SqlDataReader OpenAndExecuteReader(this SqlCommand command, CommandBehavior behavior)
		=> CommandExtensions.OpenAndExecuteReader(command, behavior, static (c, b) => c.ExecuteReader(b));

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the handler. The reader is disposed afterwards.
	/// </summary>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="handler">The function that reads from the reader. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the handler throws.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="handler"/> is <see langword="null"/>.</exception>
	public static void ExecuteReader(this SqlCommand command, Action<SqlDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		using SqlDataReader reader = command.OpenAndExecuteReader(behavior);
		handler(reader);
	}

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result. The reader is disposed afterwards.
	/// </summary>
	/// <typeparam name="T">The type returned by the transform function.</typeparam>
	/// <param name="command">The command to execute. Its connection is opened if needed and closed again if it was opened here; neither the command nor the connection is disposed.</param>
	/// <param name="transform">The function that reads from the reader and produces the result. Do not let the reader escape it.</param>
	/// <param name="behavior">The behavior to use with the data reader. <see cref="CommandBehavior.CloseConnection"/> is added when the command opens a closed connection.</param>
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	/// <remarks>
	/// Exceptions from the provider (for example <see cref="SqlException"/>) propagate unchanged. The reader is disposed, and with it a connection that this method opened, even when the transform function throws.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="transform"/> is <see langword="null"/>.</exception>
	public static T ExecuteReader<T>(this SqlCommand command, Func<SqlDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		using SqlDataReader reader = command.OpenAndExecuteReader(behavior);
		return transform(reader);
	}
}
