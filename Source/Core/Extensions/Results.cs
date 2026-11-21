namespace Open.Database.Extensions;

public static partial class CoreExtensions
{
	/// <summary>
	/// Iterates each remaining record of an <see cref="IDataReader"/> and maps its fields to a <typeparamref name="T"/>.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> is <see langword="null"/>.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> Results<T>(
	this IDataReader reader, RowMap<T>? map = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(map ?? RowMap<T>.Default, reader, 0, -1);
	}

	/// <inheritdoc cref="Results{T}(IDataReader, RowMap{T}?)"/>
	public static IEnumerable<T> Results<T>(
		this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.Stream(RowMap<T>.Default, reader, 0, -1);
	}

	/// <summary>
	/// Maps each remaining record of an <see cref="IDataReader"/> to a <typeparamref name="T"/>,
	/// buffering all the results in a queue before returning.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>The enumerable to pull the mapped results from.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> is <see langword="null"/>.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader, RowMap<T>? map = null)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return Views.ReaderCore(reader, map ?? RowMap<T>.Default)
			.Collect<QueueBuild<T>, Queue<T>>(default)
			.DequeueEach();
	}

	/// <inheritdoc cref="ResultsBuffered{T}(IDataReader, RowMap{T}?)" />
	public static IEnumerable<T> ResultsBuffered<T>(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return Views.ReaderCore(reader, RowMap<T>.Default)
			.Collect<QueueBuild<T>, Queue<T>>(default)
			.DequeueEach();
	}

	/// <summary>
	/// Executes a reader and maps each record of its result to a <typeparamref name="T"/>,
	/// buffering all the results in a queue before returning.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>The enumerable to pull the mapped results from.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>). It is thrown at the call, before the command runs.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static IEnumerable<T> Results<T>(this IDbCommand command, RowMap<T>? map = null)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(reader => reader.ResultsBuffered(map));
	}

	/// <inheritdoc cref="Results{T}(IDbCommand, RowMap{T}?)" />
	public static IEnumerable<T> Results<T>(this IDbCommand command)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReader(reader => reader.ResultsBuffered<T>());
	}

	/// <summary>
	/// Asynchronously iterates each remaining record of a <see cref="DbDataReader"/> and maps its fields to a <typeparamref name="T"/>.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <param name="reader">The <see cref="DbDataReader"/> to read results from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>The asynchronous enumerable to pull the mapped results from.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> is <see langword="null"/>.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		RowMap<T>? map = null,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return RowReader.StreamAsync(map ?? RowMap<T>.Default, reader, 0, -1, useReadAsync: true, readThrough: false, cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbDataReader, RowMap{T}?, CancellationToken)" />
	public static IAsyncEnumerable<T> ResultsAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken)
		=> ResultsAsync<T>(reader, null, cancellationToken);

	/// <summary>
	/// Asynchronously maps each remaining record of a <see cref="DbDataReader"/> to a <typeparamref name="T"/>,
	/// buffering all the results in a queue before returning.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <param name="reader">The <see cref="DbDataReader"/> to read results from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <param name="useReadAsync">
	/// <see langword="true"/> to read the rows with <c>ReadAsync</c>;
	/// <see langword="false"/> to read them with <c>Read</c>.
	/// The default is <see langword="true"/>.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the enumerable to pull the mapped results from.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> is <see langword="null"/>.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static async ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		RowMap<T>? map = null,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		MappedQueryCore<T> core = Views.ReaderCore(reader, map ?? RowMap<T>.Default);
		if (useReadAsync) core = core.WithAsyncReads();

		return (await core
			.CollectAsync<QueueBuild<T>, Queue<T>>(default, cancellationToken)
			.ConfigureAwait(false))
			.DequeueEach();
	}

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
	this DbDataReader reader,
	RowMap<T>? map,
	CancellationToken cancellationToken)
	=> ResultsBufferedAsync(reader, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsBufferedAsync{T}(DbDataReader, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsBufferedAsync<T>(
		this DbDataReader reader,
		CancellationToken cancellationToken)
		=> ResultsBufferedAsync<T>(reader, null, true, cancellationToken);

	/// <summary>
	/// Asynchronously executes a reader and maps each record of its result to a <typeparamref name="T"/>,
	/// buffering all the results in a queue before returning.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='connection']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <param name="useReadAsync">
	/// <see langword="true"/> to read the rows with <c>ReadAsync</c>;
	/// <see langword="false"/> to read them with <c>Read</c>.
	/// The default is <see langword="true"/>.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is the enumerable to pull the mapped results from.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="command"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>). It is thrown at the call, before the command runs.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		RowMap<T>? map = null,
		bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		return command.ExecuteReaderAsync(reader => reader.ResultsBufferedAsync(map, useReadAsync, cancellationToken), CommandBehavior.SingleResult, cancellationToken);
	}

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	[OverloadResolutionPriority(-1)]
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		RowMap<T>? map,
		CancellationToken cancellationToken = default)
		=> ResultsAsync(command, map, true, cancellationToken);

	/// <inheritdoc cref="ResultsAsync{T}(DbCommand, RowMap{T}?, bool, CancellationToken)" />
	public static ValueTask<IEnumerable<T>> ResultsAsync<T>(
		this DbCommand command,
		CancellationToken cancellationToken)
		=> ResultsAsync<T>(command, null, true, cancellationToken);

	const string ResultsFromDataTableObsolete = "Use .ToDataTable() then table.Map(map).ToList() instead, for example: using var table = command.ToDataTable(); var rows = table.Map(map).ToList(). This method cleared the table as the rows were read; Map does not, so dispose or clear the table yourself. This method threw for a type that cannot be mapped before it ran; Map<T> still throws, but after the data is loaded: read RowMap<T>.Default (or build your RowMap) first to keep the early failure. Removed in 12.";

	/// <summary>
	/// Loads all the data of an <see cref="IDataReader"/> into a <see cref="DataTable"/>, then maps its rows to a <typeparamref name="T"/>.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='obsolete']/*" />
	/// <para>
	/// Use <see cref="DataReaderExtensions.ToDataTable(IDataReader)"/> and then <c>table.Map(map)</c> instead.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-results']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>). It is thrown at the call, before the data is read.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsFromDataTableObsolete)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDataReader reader, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = reader.ToDataTable();
		return table.To(map, true);
	}

	/// <summary>
	/// Executes a reader, loads all its data into a <see cref="DataTable"/>, then maps its rows to a <typeparamref name="T"/>.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='obsolete']/*" />
	/// <para>
	/// Use <see cref="CommandExtensions.ToDataTable(IDbCommand, CommandBehavior)"/> and then <c>table.Map(map)</c> instead.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='rowmap-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map-T']/*" />
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>The enumerable to pull the transformed results from.</returns>
	/// <exception cref="InvalidOperationException">
	/// <typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>). It is thrown at the call, before the command runs.
	/// </exception>
	/// <seealso cref="RowMap{T}"/>
	[Obsolete(ResultsFromDataTableObsolete)]
	public static IEnumerable<T> ResultsFromDataTable<T>(this IDbCommand command, RowMap<T>? map = null)
	{
		// An unmappable T fails here, before the command runs.
		RowMap<T>.ThrowIfUnmappable();

		using var table = command.ToDataTable();
		return table.To(map, true);
	}
}
