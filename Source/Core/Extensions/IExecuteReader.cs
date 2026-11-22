namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that read data from an
/// <see cref="IExecuteReader"/>.
/// </summary>
public static class IExecuteReaderExtensions
{
	const string LaterSets = "Map also reads the later result sets, so a failure in one is thrown; to read only the first, as this method does, call .SingleResult() before Map (cmd.SingleResult().Map(...); .Map(...).SingleResult() still works).";
	const string ResultsObsolete = "Use .Map<T>().ToList() instead (.Map(map).ToList() when you pass a RowMap; Map does not accept a null map). " + LaterSets + " Map returns a List<T>. Removed in 12.";

	#region Async terminals that take the type: Map<T>() then the terminal

	// Only the async forms are on the command:
	// a synchronous caller writes command.Map<T>().ToList()
	// (the reader-level synchronous helpers are unchanged).
	// Each is one expression: no await, no state machine of its own.

	/// <summary>
	/// Reads every row into a list, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToListAsync(cancellationToken)</c>:
	/// a single value such as <see cref="int"/> reads the first column;
	/// any other <typeparamref name="T"/> maps by column name.
	/// </summary>
	/// <remarks>
	/// A single value (a primitive, an enum, <see cref="string"/>,
	/// <see cref="decimal"/>, <see cref="DateTime"/>,
	/// <see cref="DateTimeOffset"/>, <see cref="TimeSpan"/>,
	/// <see cref="Guid"/>, <c>byte[]</c>, <c>DateOnly</c>,
	/// <c>TimeOnly</c> or a <see cref="Nullable{T}"/> of one)
	/// is read from the first column of each row, whatever it is named;
	/// the other columns are ignored, so select only the one you need.
	/// The column is converted as the mapper converts a property of that type:
	/// its typed getter when the column type matches (no boxing),
	/// a range-checked integer conversion, else a general conversion.
	/// A NULL is <see langword="default"/> for a non-nullable value type
	/// (<c>0</c> for <see cref="int"/>)
	/// and <see langword="null"/> for <see cref="Nullable{T}"/>,
	/// <see cref="string"/> and <c>byte[]</c>;
	/// declare <c>int?</c> to tell NULL from 0.
	/// A value that cannot be converted throws <see cref="ValueMappingException"/>.
	/// Only the asynchronous terminals are on the command;
	/// for a synchronous read use <c>command.Map&lt;T&gt;().ToList()</c>.
	/// Pass the token or nothing:
	/// <c>ToListAsync&lt;int&gt;(default)</c> binds to the obsolete instance method
	/// <c>ToListAsync(transform, behavior)</c> of an expressive command,
	/// which is the one member that still wins over these extensions.
	/// </remarks>
	/// <typeparam name="T">The model type to map the rows to, or the single value to read from the first column.</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as a list.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped
	/// (an interface, an abstract class, ...):
	/// thrown at the call, before anything runs.
	/// </exception>
	/// <seealso cref="CoreExtensions.Map{T}(IExecuteReader)"/>
	public static ValueTask<List<T>> ToListAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToListAsync(cancellationToken);

	/// <summary>
	/// Reads every row into an array, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToArrayAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as an array.</returns>
	public static ValueTask<T[]> ToArrayAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToArrayAsync(cancellationToken);

	/// <summary>
	/// Reads every row into an immutable array, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToImmutableArrayAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as an immutable array.</returns>
	public static ValueTask<ImmutableArray<T>> ToImmutableArrayAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToImmutableArrayAsync(cancellationToken);

	/// <summary>
	/// Reads the first row, or <see langword="default"/> when there is none,
	/// asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().FirstOrDefaultAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The first row, or <see langword="default"/>.</returns>
	public static ValueTask<T?> FirstOrDefaultAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().FirstOrDefaultAsync(cancellationToken);

	/// <summary>
	/// Reads the only row, or <see langword="default"/> when there is none,
	/// asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().SingleOrDefaultAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The only row, or <see langword="default"/>.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is more than one row.
	/// </exception>
	public static ValueTask<T?> SingleOrDefaultAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().SingleOrDefaultAsync(cancellationToken);

	/// <summary>
	/// Reads the first row, asynchronously; throws when there is none.
	/// Equivalent to <c>command.Map&lt;T&gt;().FirstAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The first row.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is no row.
	/// </exception>
	public static ValueTask<T> FirstAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().FirstAsync(cancellationToken);

	/// <summary>
	/// Reads the only row, asynchronously; throws when there is none
	/// or more than one.
	/// Equivalent to <c>command.Map&lt;T&gt;().SingleAsync(cancellationToken)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The only row.</returns>
	/// <exception cref="InvalidOperationException">
	/// There is no row, or more than one.
	/// </exception>
	public static ValueTask<T> SingleAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().SingleAsync(cancellationToken);

	/// <summary>
	/// Reads every row into a hash set, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToHashSetAsync(comparer, cancellationToken)</c>.
	/// The comparer comes first, as on <see cref="MappedQuery{T}"/>:
	/// pass the token by name,
	/// <c>ToHashSetAsync&lt;int&gt;(cancellationToken: ct)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="comparer">The comparer of the set, or <see langword="null"/> for the default.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as a hash set.</returns>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(
		this IExecuteReader command,
		IEqualityComparer<T>? comparer = null,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToHashSetAsync(comparer, cancellationToken);

	#endregion

	/// <summary>
	/// Reads every record of the first result set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the columns at the given ordinals from every record of the result
	/// set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<int> ordinals)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(ordinals),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the columns at the given ordinals from every record of the result
	/// set and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, int n, params IEnumerable<int> others)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(n, others),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the named columns from every record of the first result set and
	/// returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="columnNames">The column names to select.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <exception cref="ArgumentNullException"><paramref name="command"/> or <paramref name="columnNames"/> is <see langword="null"/>.</exception>
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<string> columnNames)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(columnNames),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the named columns from every record of the result set and returns
	/// the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="c">The first column name to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining column names to request from the reader for each record.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='returns-queue']/*" />
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, string c, params IEnumerable<string> others)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		//if (c is null) throw new ArgumentNullException(nameof(c));
		//if (others.Any(e => e is null)) throw new ArgumentNullException(nameof(c));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(c, others),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record and returns the results as a
	/// list.
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The list of transformed records.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static IEnumerable<object?> FirstOrdinalResults(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record and returns the values cast to
	/// <typeparamref name="T0"/>.
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </summary>
	/// <typeparam name="T0">The type the values are cast to.</typeparam>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The enumerable of cast values.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults<T0>(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

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

		return command.ExecuteReader(
			reader => reader.ToDataTable(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Loads each result set of the command into its own <see cref="DataTable"/>.
	/// Calls <see cref="IDataReader.NextResult"/> to check for more results.
	/// </summary>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <returns>The resultant list of tables.</returns>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='null-command']/*" />
	public static List<DataTable> LoadTables(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.ToDataTables(),
			CommandBehavior.SequentialAccess);
	}
}
