namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Reads each row of a query without a model, as an array of its values.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Everything after it is the mapped view: the terminals, <c>Skip</c> and <c>Take</c>, <c>SingleResult()</c>,
	/// <c>AsyncReads()</c> and <c>UsePool</c> are the ones <c>Map&lt;T&gt;()</c> has, and a later result set is read through
	/// the same way.
	/// Each row is one new <c>object?[]</c> with a slot for each column (a database NULL is <see langword="null"/>).
	/// The row arrays belong to the caller; only the buffers that hold the rows are pooled, and those are cleared when they go back.
	/// The columns are read once, in order, so a provider may stream a row.
	/// The result carries no column names: use <see cref="AsDictionaryRows(IExecuteReader)"/> when you need them.
	/// </para>
	/// </remarks>
	/// <param name="command">The command to read from.</param>
	/// <returns>The deferred query; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static MappedQuery<object?[]> AsObjectArrays(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return new(Views.QueryCore(command, ObjectArraySource.Instance));
	}

	/// <inheritdoc cref="AsObjectArrays(IExecuteReader)"/>
	public static MappedCommand<object?[]> AsObjectArrays(this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return new(Views.CommandCore(command, ObjectArraySource.Instance));
	}

	/// <summary>Reads each row of a reader without a model, as an array of its values.</summary>
	/// <remarks>
	/// <para>
	/// Each row is one new <c>object?[]</c> with a slot for each column (a database NULL is <see langword="null"/>), and belongs to the caller.
	/// The result carries no column names: use <see cref="AsDictionaryRows(IDataReader)"/> when you need them.
	/// </para>
	/// </remarks>
	/// <param name="reader">The reader the caller owns, positioned on the result set to read; it is never read through.</param>
	/// <returns>The deferred view; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static MappedReader<object?[]> AsObjectArrays(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return new(Views.ReaderCore(reader, ObjectArraySource.Instance));
	}

	/// <summary>Reads each row of a table without a model, as an array of its values.</summary>
	/// <remarks>
	/// <para>
	/// Each row is one new <c>object?[]</c> with a slot for each column (a database NULL is <see langword="null"/>), and belongs to the caller.
	/// The result carries no column names: use <see cref="AsDictionaryRows(DataTable)"/> when you need them.
	/// </para>
	/// </remarks>
	/// <param name="table">The table to read.</param>
	/// <returns>The deferred view; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="table"/> is <see langword="null"/>.</exception>
	public static MappedTable<object?[]> AsObjectArrays(this DataTable table)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		Contract.EndContractBlock();

		return new(Views.TableCore(table, ObjectArraySource.Instance));
	}

	/// <summary>
	/// Reads each row of a query without a model, as a <see cref="DictionaryRow"/> (the values and the column names).
	/// </summary>
	/// <remarks>
	/// <para>
	/// The same as <see cref="AsObjectArrays(IExecuteReader)"/>, with the names.
	/// A <see cref="DictionaryRow"/> holds the row's values; the column names are shared by every row of the result set.
	/// Use <see cref="DictionaryRow.ToDictionary"/> for a copy the caller owns.
	/// A result set with no rows gives no rows, so no names.
	/// </para>
	/// </remarks>
	/// <param name="command">The command to read from.</param>
	/// <returns>The deferred query; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="command"/> is <see langword="null"/>.</exception>
	public static MappedQuery<DictionaryRow> AsDictionaryRows(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return new(Views.QueryCore(command, DictionaryRowSource.Instance));
	}

	/// <inheritdoc cref="AsDictionaryRows(IExecuteReader)"/>
	public static MappedCommand<DictionaryRow> AsDictionaryRows(this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return new(Views.CommandCore(command, DictionaryRowSource.Instance));
	}

	/// <summary>Reads each row of a reader without a model, as a <see cref="DictionaryRow"/> (the values and the column names).</summary>
	/// <remarks>
	/// <para>
	/// The same as <see cref="AsObjectArrays(IDataReader)"/>, with the names.
	/// A <see cref="DictionaryRow"/> holds the row's values; the column names are shared by every row of the result set.
	/// Use <see cref="DictionaryRow.ToDictionary"/> for a copy the caller owns.
	/// </para>
	/// </remarks>
	/// <param name="reader">The reader the caller owns, positioned on the result set to read; it is never read through.</param>
	/// <returns>The deferred view; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="reader"/> is <see langword="null"/>.</exception>
	public static MappedReader<DictionaryRow> AsDictionaryRows(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return new(Views.ReaderCore(reader, DictionaryRowSource.Instance));
	}

	/// <summary>Reads each row of a table without a model, as a <see cref="DictionaryRow"/> (the values and the column names).</summary>
	/// <remarks>
	/// <para>
	/// The same as <see cref="AsObjectArrays(DataTable)"/>, with the names.
	/// A <see cref="DictionaryRow"/> holds the row's values; the column names are shared by every row of the result set.
	/// Use <see cref="DictionaryRow.ToDictionary"/> for a copy the caller owns.
	/// </para>
	/// </remarks>
	/// <param name="table">The table to read.</param>
	/// <returns>The deferred view; nothing runs until a terminal does.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="table"/> is <see langword="null"/>.</exception>
	public static MappedTable<DictionaryRow> AsDictionaryRows(this DataTable table)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		Contract.EndContractBlock();

		return new(Views.TableCore(table, DictionaryRowSource.Instance));
	}
}
