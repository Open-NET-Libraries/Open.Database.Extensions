namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for retrieving data from a command using best practices.
/// </summary>
public static class IExecuteReaderExtensions
{
	const string LaterSets = "Map also reads the later result sets, so a failure in one is thrown; add .SingleResult() to read only the first, as this method does.";
	const string SkipDifference = "Map does not run the selector on skipped rows, so a selector that throws on one no longer fails the call.";
	const string ResultsObsolete = "Use .Map<T>().ToList() instead (.Map(map).ToList() when you pass a RowMap; Map does not accept a null map). " + LaterSets + " Map returns a List<T>. Removed in 12.";
	const string FirstOrDefaultObsolete = "Use .Map(selector).FirstOrDefault() instead. " + LaterSets + " Removed in 12.";
	const string SingleOrDefaultObsolete = "Use .Map(selector).SingleOrDefault() instead. " + LaterSets + " Removed in 12.";
	const string TakeObsolete = "Use .Map(selector).Take(count).ToList() instead. " + LaterSets + " Take(0) now runs the command; this method did not. Removed in 12.";
	const string SkipObsolete = "Use .Map(selector).Skip(count).ToList() instead. " + LaterSets + " " + SkipDifference + " Removed in 12.";
	const string SkipThenTakeObsolete = "Use .Map(selector).Skip(skip).Take(take).ToList() instead. " + LaterSets + " " + SkipDifference + " Take(0) now runs the command; this method did not. Removed in 12.";

	/// <summary>
	/// Iterates a reader on a command with a handler function.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="handler">The handler function for each IDataRecord.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	public static void IterateReader(this IExecuteReader command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		command.ExecuteReader(
			reader => reader.ForEach(handler),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates a reader on a command while the handler function returns true.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="handler">The handler function for each IDataRecord.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	public static void IterateReaderWhile(this IExecuteReader command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		command.ExecuteReader(
			reader => reader.IterateWhile(handler),
			behavior | CommandBehavior.SingleResult);
	}

	/// <inheritdoc cref="IterateReader(IExecuteReader, Action{IDataRecord}, CommandBehavior)"/>
	public static ValueTask IterateReaderAsync(this IExecuteReaderAsync command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return command.ExecuteReaderAsync(
			reader =>
			{
				if (reader is DbDataReader r)
					return r.ForEachAsync(handler, command.UseAsyncRead, command.CancellationToken);

				reader.ForEach(handler, true, command.CancellationToken);
				return new ValueTask();
			},
			behavior | CommandBehavior.SingleResult);
	}

	/// <inheritdoc cref="IterateReaderWhile(IExecuteReader, Func{IDataRecord, bool}, CommandBehavior)"/>
	public static ValueTask IterateReaderWhileAsync(this IExecuteReaderAsync command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return command.ExecuteReaderAsync(
			reader =>
			{
				if (reader is DbDataReader r)
					return r.IterateWhileAsync(handler, command.UseAsyncRead, command.CancellationToken);

				reader.IterateWhile(handler, true, command.CancellationToken);
				return new ValueTask();
			},
			behavior | CommandBehavior.SingleResult);
	}

	/// <inheritdoc cref="IterateReaderWhile(IExecuteReader, Func{IDataRecord, bool}, CommandBehavior)"/>
	public static ValueTask IterateReaderWhileAsync(this IExecuteReaderAsync command, Func<IDataRecord, ValueTask<bool>> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		return command.ExecuteReaderAsync(
			reader => reader.IterateWhileAsync(handler, command.CancellationToken),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Executes a reader on a command with a transform function.
	/// </summary>
	/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
	/// <typeparam name="TResult">The type returned by the selector.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function for each IDataRecord.</param>
	/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The result of the transform.</returns>
	public static TResult IterateReader<TEntity, TResult>(
		this IExecuteReader command,
		Func<IDataRecord, TEntity> transform,
		Func<IEnumerable<TEntity>, TResult> selector, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => selector(reader.Select(transform)),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates an IDataReader and returns the first result through a transform function.  Throws if none.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The value from the transform.</returns>
	public static T First<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).First(),
			CommandBehavior.SingleRow | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/> then <see cref="MappedQuery{T}.FirstOrDefault"/>.
	/// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The value from the transform.</returns>
	[Obsolete(FirstOrDefaultObsolete)]
	public static T? FirstOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).FirstOrDefault(),
			CommandBehavior.SingleRow | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates a IDataReader and returns the first result through a transform function.
	/// Throws if none or more than one entry.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The value from the transform.</returns>
	public static T Single<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).Single(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/> then <see cref="MappedQuery{T}.SingleOrDefault"/>.
	/// Iterates an IDataReader and returns the first result through a transform function.
	/// Returns default(T) if none.
	/// Throws if more than one entry.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The value from the transform.</returns>
	[Obsolete(SingleOrDefaultObsolete)]
	public static T? SingleOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).SingleOrDefault(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Take(int)"/>, then <see cref="MappedQueryWithTake{T}.ToList"/>.
	/// Iterates an IDataReader and returns the first number of results defined by the count.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="count">The maximum number of records to return.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The results from the transform limited by the take count.</returns>
	[Obsolete(TakeObsolete)]
	public static List<T> Take<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
		Contract.EndContractBlock();

		return count == 0 ? [] : command.ExecuteReader(
			reader => reader.Select(transform).Take(count).ToList(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Skip(int)"/>, then <see cref="MappedQueryWithSkip{T}.ToList"/>.
	/// Iterates an IDataReader and skips the first number of results defined by the count.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="count">The number of records to skip.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The results from the transform after the skip count.</returns>
	[Obsolete(SkipObsolete)]
	public static List<T> Skip<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => count == 0
				? reader.ToList(transform)
				: reader.Select(transform).Skip(count).ToList(),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader, Func{IDataRecord, T})"/>, then <see cref="MappedQuery{T}.Skip(int)"/>, <see cref="MappedQueryWithSkip{T}.Take(int)"/> and <see cref="MappedQueryWithTake{T}.ToList"/>.
	/// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="skip">The number of entries to skip before starting to take results.</param>
	/// <param name="take">The maximum number of records to return.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The results from the skip, transform and take operation.</returns>
	[Obsolete(SkipThenTakeObsolete)]
	public static List<T> SkipThenTake<T>(this IExecuteReader command, int skip, int take, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (skip < 0) throw new ArgumentOutOfRangeException(nameof(skip), skip, "Cannot be negative.");
		if (take < 0) throw new ArgumentOutOfRangeException(nameof(take), take, "Cannot be negative.");
		Contract.EndContractBlock();

		return take == 0
			? []
			: command.ExecuteReader(
				reader => reader.Select(transform).Skip(skip).Take(take).ToList(),
				CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Converts all IDataRecords into a list using a transform function.
	/// </summary>
	/// <typeparam name="T">The expected return type.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <returns>The list of transformed records.</returns>
	public static List<T> ToList<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToList(),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Converts all IDataRecords into an array using a transform function.
	/// </summary>
	/// <typeparam name="T">The expected return type.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <returns>The array of transformed records.</returns>
	public static T[] ToArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToArray(),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Converts all IDataRecords into an immutable array using a transform function.
	/// </summary>
	/// <typeparam name="T">The expected return type.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function.</param>
	/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
	/// <returns>The array of transformed records.</returns>
	public static ImmutableArray<T> ToImmutableArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Select(transform).ToImmutableArray(),
			behavior | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates all records within the first result set using an IDataReader and returns the results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates all records within the current result set using an IDataReader and returns the desired results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
	/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<int> ordinals)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(ordinals),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates all records within the current result set using an IDataReader and returns the desired results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
	public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, int n, params IEnumerable<int> others)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.Retrieve(n, others),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="columnNames">The column names to select.</param>
	/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
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
	/// Iterates all records within the current result set using an IDataReader and returns the desired results.
	/// <see cref="DBNull"/> values are left unchanged (retained).
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="c">The first column name to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining column names to request from the reader for each record.</param>
	/// <returns>The QueryResult that contains all the results and the column mappings.</returns>
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
	/// Obsolete: use <see cref="CoreExtensions.Map{T}(IExecuteReader)"/> then <see cref="MappedQuery{T}.ToList"/>.
	/// Iterates each record and attempts to map the fields to type T.
	/// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
	/// </summary>
	/// <remarks>Obsolete in 11.0, removed in 12. See <see href="https://open-net-libraries.github.io/Open.Database.Extensions/articles/whats-new-11.html#deprecated-in-110">Deprecated in 11.0</see> for the difference and a before/after example.</remarks>
	/// <remarks>Declare the <see cref="RowMap{T}"/> once and pass it: a map built in the call (a <c>new RowMap&lt;T&gt;(...)</c> or a collection expression in the argument) is rebuilt, and its row reader compiled, on every call.</remarks>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsObsolete)]
	public static IEnumerable<T> Results<T>(this IExecuteReader command, RowMap<T>? map = null)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(
			reader => reader.ResultsBuffered(map),
			CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record and returns the values in a queue.
	/// <see cref="DBNull"/> values are converted to null.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <returns>The queue of first-column values; <see cref="DBNull"/> is already null. It is the queue the method built: enumerate it as often as you like, or call <c>Dequeue</c> to release each row as you go. Rows stay in memory until the queue is dropped.</returns>
	public static Queue<object?> FirstOrdinalResults(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Reads the first column from every record..
	/// <see cref="DBNull"/> values are converted to null.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <returns>The queue of values cast to T0. It is the queue the method built: enumerate it as often as you like, or call <c>Dequeue</c> to release each row as you go. Rows stay in memory until the queue is dropped.</returns>
	public static Queue<T0> FirstOrdinalResults<T0>(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.FirstOrdinalResults<T0>(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Imports all data using an IDataReader into a DataTable.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <returns>The resultant DataTable.</returns>
	public static DataTable LoadTable(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.ToDataTable(),
			CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
	}

	/// <summary>
	/// Loads all data from a command through an IDataReader into a DataTables.
	/// Each result set is loaded into its own table. Calls .NextResult() to check for more results.
	/// </summary>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <returns>The resultant list of DataTables.</returns>
	public static List<DataTable> LoadTables(this IExecuteReader command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command.ExecuteReader(
			reader => reader.ToDataTables(),
			CommandBehavior.SequentialAccess);
	}
}
