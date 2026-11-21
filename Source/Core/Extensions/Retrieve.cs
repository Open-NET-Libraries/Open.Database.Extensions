
namespace Open.Database.Extensions;

[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Internal function that requires a cancellation token.")]
public static partial class CoreExtensions
{
	internal static QueryResultQueue<object[]> RetrieveInternal(
		IDataReader reader,
		IEnumerable<int> ordinals,
		IEnumerable<string>? columnNames = null,
		bool readStarted = false)
	{
		IList<int> o = ordinals is IList<int> i ? i : ordinals.ToImmutableArray();
		return new QueryResultQueue<object[]>(
			o, columnNames ?? reader.GetNames(o),
			reader.AsEnumerableInternal(o, readStarted));
	}

	internal static QueryResultQueue<object[]> RetrieveInternal(
		ArrayPool<object> arrayPool,
		IDataReader reader,
		IEnumerable<int> ordinals,
		IEnumerable<string>? columnNames = null,
		bool readStarted = false)
	{
		IList<int> o = ordinals is IList<int> i ? i : ordinals.ToImmutableArray();
		return new QueryResultQueue<object[]>(
			o, columnNames ?? reader.GetNames(o),
			reader.AsEnumerableInternal(o, readStarted, arrayPool));
	}

	/// <inheritdoc cref="Retrieve(IDataReader, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IDataReader reader)
	{
		// GetNames guards reader.
		ImmutableArray<string> names = reader.GetNames();
		return new QueryResultQueue<object[]>(
			IdentityOrdinals(names.Length), names,
			reader.AsEnumerable());
	}

	/// <summary>
	/// Builds the ordinals <c>[0, 1, ... count - 1]</c> directly as an <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <remarks>
	/// The array has the exact capacity and is reused zero-copy by <c>QueryResult</c>,
	/// instead of an <c>Enumerable.Range</c> that a growing <c>ToImmutableArray</c> would then copy.
	/// </remarks>
	/// <param name="count">The number of ordinals.</param>
	/// <returns>The ordinals.</returns>
	static ImmutableArray<int> IdentityOrdinals(int count)
	{
		var b = ImmutableArray.CreateBuilder<int>(count);
		for (int i = 0; i < count; i++) b.Add(i);
		return b.MoveToImmutable();
	}

	/// <summary>
	/// Reads the records of the current result set of an <see cref="IDataReader"/> and returns them with their column mappings.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are left unchanged (retained).</remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
	/// <returns>The query result that contains all the results and the column mappings.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="ordinals"/> is <see langword="null"/>.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDataReader reader, IEnumerable<int> ordinals)
		=> RetrieveInternal(reader, ordinals); // GetNames guards reader.

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="Retrieve(IDataReader, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDataReader reader, int n, params IEnumerable<int> others)
		=> RetrieveInternal(reader, others.Prepend(n));

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='columnNames-select']/*" />
	/// <inheritdoc cref="Retrieve(IDataReader, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="columnNames"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="IndexOutOfRangeException">
	/// A requested column does not exist. The message lists all the invalid names.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDataReader reader, IEnumerable<string> columnNames)
	{
		// GetOrdinalMapping returns early for an empty list without touching reader.
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		(string Name, int Ordinal)[] columns = reader.GetOrdinalMapping(columnNames);
		return RetrieveInternal(reader,
			columns.Select(c => c.Ordinal),
			columns.Select(c => c.Name));
	}

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='c-others']/*" />
	/// <inheritdoc cref="Retrieve(IDataReader, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="IndexOutOfRangeException">
	/// A requested column does not exist. The message lists all the invalid names.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDataReader reader, string c, params IEnumerable<string> others)
		=> Retrieve(reader, others.Prepend(c));

	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="behavior">
	/// The behavior to use with the data reader.
	/// <see cref="CommandBehavior.SequentialAccess"/> is added to it.
	/// The default is <see cref="CommandBehavior.Default"/>.
	/// </param>
	/// <inheritdoc cref="Retrieve(IDbCommand, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static QueryResultQueue<object[]> Retrieve(this IDbCommand command, CommandBehavior behavior = CommandBehavior.Default)
		=> command.ExecuteReader(reader => reader.Retrieve(), behavior | CommandBehavior.SequentialAccess);

	/// <summary>
	/// Executes a reader and returns the records of its remaining result set with their column mappings.
	/// </summary>
	/// <remarks>
	/// <para><see cref="DBNull"/> values are left unchanged (retained).</para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="ordinals">The ordinals to request from the reader for each record.</param>
	/// <returns>The query result that contains all the results and the column mappings.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="ordinals"/> is <see langword="null"/>.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDbCommand command, IEnumerable<int> ordinals)
		=> command.ExecuteReader(reader => reader.Retrieve(ordinals));

	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="Retrieve(IDbCommand, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDbCommand command, int n, params IEnumerable<int> others)
		=> command.ExecuteReader(reader => RetrieveInternal(reader, others.Prepend(n)));

	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='columnNames-select']/*" />
	/// <inheritdoc cref="Retrieve(IDbCommand, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="columnNames"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="IndexOutOfRangeException">
	/// A requested column does not exist. The message lists all the invalid names.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDbCommand command, IEnumerable<string> columnNames)
		=> command.ExecuteReader(reader => reader.Retrieve(columnNames));

	/// <include file="Readers.docs.xml" path="docs/members[@name='command']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='c-others']/*" />
	/// <inheritdoc cref="Retrieve(IDbCommand, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="IndexOutOfRangeException">
	/// A requested column does not exist. The message lists all the invalid names.
	/// </exception>
	public static QueryResultQueue<object[]> Retrieve(this IDbCommand command, string c, params IEnumerable<string> others)
		=> command.ExecuteReader(reader => Retrieve(reader, others.Prepend(c)));

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static async ValueTask<QueryResultQueue<object[]>> RetrieveAsync(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		int fieldCount = reader.FieldCount;
		ImmutableArray<string> names = reader.GetNames(); // pull before first read.
		var buffer = new Queue<object[]>();

		while (useReadAsync ? await reader.ReadAsync(cancellationToken).ConfigureAwait(false) : (!cancellationToken.IsCancellationRequested && reader.Read()))
		{
			object[] row = new object[fieldCount];
			reader.GetValues(row);
			buffer.Enqueue(row);
		}

		if (!useReadAsync) cancellationToken.ThrowIfCancellationRequested();

		return new QueryResultQueue<object[]>(
			IdentityOrdinals(names.Length),
			names,
			buffer);
	}

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		CancellationToken cancellationToken)
		=> RetrieveAsync(reader, true, cancellationToken);

	static ValueTask<QueryResultQueue<object[]>> RetrieveAsyncInternal(DbDataReader reader, CancellationToken cancellationToken, IEnumerable<int> ordinals, IEnumerable<string>? columnNames = null, bool readStarted = false, bool useReadAsync = true)
		=> RetrieveAsyncInternal(null, reader, cancellationToken, ordinals, columnNames, readStarted, useReadAsync);

	static async ValueTask<QueryResultQueue<object[]>> RetrieveAsyncInternal(ArrayPool<object>? arrayPool, DbDataReader reader, CancellationToken cancellationToken, IEnumerable<int> ordinals, IEnumerable<string>? columnNames = null, bool readStarted = false, bool useReadAsync = true)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var buffer
			= new Queue<object[]>();

		var result
			= new QueryResultQueue<object[]>(
			   ordinals,
			   columnNames ?? ordinals.Select(reader.GetName),
			   buffer);

		int fieldCount = result.ColumnCount;
		ImmutableArray<int> o = result.Ordinals;

		Func<IDataRecord, object[]> handler = GetHandler(arrayPool, fieldCount, o);

		if (readStarted)
			buffer.Enqueue(handler(reader));

		while (useReadAsync
			? await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
			: (!cancellationToken.IsCancellationRequested && reader.Read()))
		{
			buffer.Enqueue(handler(reader));
		}

		if (!useReadAsync)
			cancellationToken.ThrowIfCancellationRequested();

		return result;

		static Func<IDataRecord, object[]> GetHandler(ArrayPool<object>? arrayPool, int fieldCount, ImmutableArray<int> o)
		{
			if (arrayPool != null)
			{
				return record =>
				{
					object[] row = arrayPool.Rent(fieldCount);
					for (int i = 0; i < fieldCount; i++)
						row[i] = record.GetValue(o[i]);
					return row;
				};
			}

			return fieldCount == 0
				? (_ => Array.Empty<object>())
				: (record =>
				{
					object[] row = new object[fieldCount];
					for (int i = 0; i < fieldCount; i++)
						row[i] = record.GetValue(o[i]);
					return row;
				});
		}
	}

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		IEnumerable<int> ordinals,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> RetrieveAsyncInternal(reader, cancellationToken, ordinals, useReadAsync: useReadAsync);

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{int}, bool, CancellationToken)" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		IEnumerable<int> ordinals,
		CancellationToken cancellationToken)
		=> RetrieveAsyncInternal(reader, cancellationToken, ordinals);

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, CancellationToken, int, IEnumerable{int})" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		int n,
		params IEnumerable<int> others)
		=> RetrieveAsync(reader, others.Prepend(n));

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		CancellationToken cancellationToken,
		int n,
		params IEnumerable<int> others)
		=> RetrieveAsync(reader, others.Prepend(n), cancellationToken);

	/// <summary>
	/// Asynchronously reads the records of the current result set of a <see cref="DbDataReader"/> and returns them with their column mappings.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are left unchanged (retained).</remarks>
	/// <param name="reader">The <see cref="DbDataReader"/> to read results from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='columnNames-select']/*" />
	/// <param name="normalizeColumnOrder">
	/// <see langword="true"/> to order the columns by ordinal;
	/// <see langword="false"/> to keep the requested order.
	/// The default is <see langword="false"/>.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the query result that contains all the results and the column mappings.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		IEnumerable<string> columnNames,
		bool normalizeColumnOrder = false,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		// Validate columns first.
		(string Name, int Ordinal)[] columns = reader.GetOrdinalMapping(columnNames, normalizeColumnOrder);
		return RetrieveAsyncInternal(reader, cancellationToken,
			columns.Select(c => c.Ordinal),
			columns.Select(c => c.Name), useReadAsync: useReadAsync);
	}

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		IEnumerable<string> columnNames,
		bool normalizeColumnOrder,
		CancellationToken cancellationToken)
		=> RetrieveAsync(reader, columnNames, normalizeColumnOrder, true, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		IEnumerable<string> columnNames,
		CancellationToken cancellationToken)
		=> RetrieveAsync(reader, columnNames, false, true, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbDataReader, CancellationToken, string, IEnumerable{string})"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		string c,
		params IEnumerable<string> others)
		=> RetrieveAsync(reader, others.Prepend(c));

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='c-others']/*" />
	/// <inheritdoc cref="RetrieveAsync(DbDataReader, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbDataReader reader,
		CancellationToken cancellationToken,
		string c,
		params IEnumerable<string> others)
		=> RetrieveAsync(reader, others.Prepend(c), cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and returns the records of its remaining result set with their column mappings.
	/// </summary>
	/// <remarks>
	/// <para><see cref="DBNull"/> values are left unchanged (retained).</para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the query result that contains all the results and the column mappings.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> command.ExecuteReaderAsync(reader => RetrieveAsync(reader, useReadAsync, cancellationToken), CommandBehavior.SequentialAccess, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{int}, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		CancellationToken cancellationToken)
		=> RetrieveAsync(command, true, cancellationToken);

	static ValueTask<QueryResultQueue<object[]>> RetrieveAsyncInternal(
		DbCommand command,
		CancellationToken cancellationToken,
		IEnumerable<int> ordinals,
		IEnumerable<string>? columnNames = null,
		bool useReadAsync = true)
		=> command.ExecuteReaderAsync(reader => RetrieveAsyncInternal(reader, cancellationToken, ordinals, columnNames, useReadAsync: useReadAsync), cancellationToken: cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-command']/*" />
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		IEnumerable<int> ordinals,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> RetrieveAsyncInternal(command, cancellationToken, ordinals, useReadAsync: useReadAsync);

	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{int}, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		IEnumerable<int> ordinals,
		CancellationToken cancellationToken)
		=> RetrieveAsyncInternal(command, cancellationToken, ordinals);
	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)"/>
	/// <inheritdoc cref="RetrieveAsync(DbCommand, CancellationToken, int, IEnumerable{int})" path="/param[@name='n' or @name='others']"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		int n,
		params IEnumerable<int> others)
		=> RetrieveAsync(command, others.Prepend(n));

	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		CancellationToken cancellationToken,
		int n,
		params IEnumerable<int> others)
		=> RetrieveAsync(command, others.Prepend(n), cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and returns the records of its single result set with their column mappings.
	/// </summary>
	/// <remarks>
	/// <para><see cref="DBNull"/> values are left unchanged (retained).</para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='columnNames-select']/*" />
	/// <param name="normalizeColumnOrder">
	/// <see langword="true"/> to order the columns by ordinal;
	/// <see langword="false"/> to keep the requested order.
	/// The default is <see langword="false"/>.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the query result that contains all the results and the column mappings.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="columnNames"/> is <see langword="null"/>.
	/// </exception>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		IEnumerable<string> columnNames,
		bool normalizeColumnOrder = false,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> command.ExecuteReaderAsync(reader => RetrieveAsync(reader, columnNames, normalizeColumnOrder, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		IEnumerable<string> columnNames,
		bool normalizeColumnOrder,
		CancellationToken cancellationToken)
		=> RetrieveAsync(command, columnNames, normalizeColumnOrder, true, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		IEnumerable<string> columnNames,
		CancellationToken cancellationToken)
		=> RetrieveAsync(command, columnNames, false, true, cancellationToken);

	/// <inheritdoc cref="RetrieveAsync(DbCommand, CancellationToken, string, IEnumerable{string})"/>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		string columnName,
		params IEnumerable<string> otherColumnNames)
		=> RetrieveAsync(command, otherColumnNames.Prepend(columnName));

	/// <include file="Readers.docs.xml" path="docs/members[@name='dbCommand']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <param name="columnName">The first column name to include in the request to the reader for each record.</param>
	/// <param name="otherColumnNames">The remaining column names to request from the reader for each record.</param>
	/// <inheritdoc cref="RetrieveAsync(DbCommand, IEnumerable{string}, bool, bool, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> or <paramref name="otherColumnNames"/> is <see langword="null"/>.
	/// </exception>
	public static ValueTask<QueryResultQueue<object[]>> RetrieveAsync(
		this DbCommand command,
		CancellationToken cancellationToken,
		string columnName,
		params IEnumerable<string> otherColumnNames)
		=> RetrieveAsync(command, otherColumnNames.Prepend(columnName), false, cancellationToken);
}
