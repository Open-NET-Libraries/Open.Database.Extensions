using static Open.Database.Extensions.CommandExtensions;

namespace Open.Database.Extensions.Synchronous;

/// <summary>
/// Blocking extensions on an <see cref="IDbCommand"/>: they hold the calling thread until the work is done. Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.
/// </summary>
public static class SyncCommandExtensions
{
	/// <summary>
	/// Executes a reader and loads its first result set into a <see cref="DataTable"/>.
	/// </summary>
	/// <remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="behavior">
	/// The behavior to use with the data reader.
	/// The default is <see cref="CommandBehavior.SequentialAccess"/>.
	/// </param>
	/// <returns>A <see cref="DataTable"/> that holds the result set.</returns>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		using IDataReader reader = command.OpenAndExecuteReader(behavior);
		return reader.ToDataTable();
	}

	/// <summary>
	/// Executes a reader and loads each result set into its own <see cref="DataTable"/>.
	/// </summary>
	/// <remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// <para>
	/// It calls <see cref="IDataReader.NextResult"/> to check for more results.
	/// </para>
	/// </remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="behavior">
	/// The behavior to use with the data reader.
	/// The default is <see cref="CommandBehavior.SequentialAccess"/>.
	/// </param>
	/// <returns>A list of tables, one for each result set.</returns>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static List<DataTable> ToDataTables(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		using IDataReader reader = command.OpenAndExecuteReader(behavior);
		return reader.ToDataTables();
	}

	/// <summary>
	/// Executes a reader and passes it to a handler.
	/// </summary>
	/// <remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='reader-handler']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='null-command-handler']/*" />
	public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		using IDataReader reader = command.OpenAndExecuteReader(behavior);
		handler(reader);
	}

	/// <summary>
	/// Executes a reader and returns the result of a transform function applied to it.
	/// </summary>
	/// <remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='reader-transform']/*" />
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='behavior']/*" />
	/// <returns>The result of the transform.</returns>
	/// <include file="../Extensions/Readers.docs.xml" path="docs/members[@name='null-command-transform']/*" />
	public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		using IDataReader reader = command.OpenAndExecuteReader(behavior);
		return transform(reader);
	}
}
