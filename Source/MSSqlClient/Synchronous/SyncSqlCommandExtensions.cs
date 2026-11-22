namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking calls on a <see cref="SqlCommand"/>: they hold the calling thread until the work is done.
/// Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
public static class SyncSqlCommandExtensions
{
	// Opens a closed connection and executes the reader in one step; Core's shared helpers close a connection opened here when the
	// provider fails to execute. The execute delegate is a static lambda, so there is no closure.
	static SqlDataReader OpenAndExecuteReader(this SqlCommand command, CommandBehavior behavior)
		=> CommandExtensions.OpenAndExecuteReader(command, behavior, static (c, b) => c.ExecuteReader(b));

	/// <summary>
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the handler.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
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
	/// Executes the command and passes the <see cref="SqlDataReader"/> to the transform function, returning its result.
	/// The reader is disposed afterwards.
	/// </summary>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-command']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-behavior']/*" />
	/// <returns>The value returned by <paramref name="transform"/>.</returns>
	/// <include file="../Extensions/MSSqlClient.docs.xml" path="docs/members[@name='reader-remarks']/*" />
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
