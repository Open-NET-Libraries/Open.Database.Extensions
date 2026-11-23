namespace Open.Database.Extensions.Synchronous;

/// <summary>Blocking extensions on an <see cref="IExecuteReader"/>, which hold the calling thread until the work is done.</summary>
/// <remarks>Add <c>using Open.Database.Extensions.Synchronous;</c> to use them.</remarks>
public static class SyncExecuteReaderExtensions
{
	/// <summary>
	/// Loads the records of the first result set into a <see cref="DataTable"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The resultant <see cref="DataTable"/>.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static DataTable LoadTable(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().ExecuteReader(
			reader => reader.ToDataTable(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Loads each result set of the command into its own <see cref="DataTable"/>.
	/// </summary>
	/// <remarks>
	/// It calls <see cref="IDataReader.NextResult"/> to check for more results.
	/// </remarks>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The resultant list of tables.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static List<DataTable> LoadTables(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.AsSync().ExecuteReader(
			reader => reader.ToDataTables(),
			CommandBehavior.SequentialAccess);
	}
}
