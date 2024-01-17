namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for retrieving data from a command using best practices.
/// </summary>
public static partial class CommandExtensions
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ConnectionState EnsureOpen(this IDbCommand command)
	{
#if DEBUG
		if (command.Connection is null) throw new InvalidOperationException("Cannot execute a command with a null connection.");
#endif
		return command.Connection!.EnsureOpen();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ValueTask<ConnectionState> EnsureOpenAsync(this IDbCommand command, CancellationToken cancellationToken)
	{
#if DEBUG
		if (command.Connection is null) throw new ArgumentException("Cannot execute a command with a null connection.");
#endif
		return command.Connection!.EnsureOpenAsync(cancellationToken);
	}

	/// <summary>
	/// Iterates all records using an <see cref="IDataReader"/> and returns the desired results as a list.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	public static List<T> ToList<T>(
		this IDbCommand command,
		CommandBehavior behavior,
		Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).ToList();
	}

	/// <inheritdoc cref="ToList{T}(IDbCommand, CommandBehavior, Func{IDataRecord, T})"/>
	public static List<T> ToList<T>(this IDbCommand command,
		Func<IDataRecord, T> transform,
		CommandBehavior behavior = CommandBehavior.Default)
		=> ToList(command, behavior, transform);

	/// <inheritdoc cref="ToListAsync{T}(DbCommand, CommandBehavior, Func{IDataRecord, T}, bool, CancellationToken)"/>
	public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
		CommandBehavior behavior,
		Func<IDataRecord, ValueTask<T>> transform,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

		return await reader.ToListAsync(transform, cancellationToken).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ToListAsync{T}(DbCommand, CommandBehavior, Func{IDataRecord, T}, bool, CancellationToken)"/>
	public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
		Func<IDataRecord, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
		=> ToListAsync(command, behavior, transform, cancellationToken);

	/// <summary>
	/// Asynchronously iterates all records using an <see cref="IDataReader"/> and returns the desired results as a list.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>A task containing a list of all results.</returns>
	public static async ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
		CommandBehavior behavior,
		Func<IDataRecord, T> transform,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;

#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

		if (useReadAsync) return await reader.ToListAsync(transform, cancellationToken).ConfigureAwait(false);

		var r = reader.ToList(transform, cancellationToken);
		return r;
	}

	/// <inheritdoc cref="ToListAsync{T}(DbCommand, CommandBehavior, Func{IDataRecord, T}, bool, CancellationToken)"/>
	public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
		CommandBehavior behavior,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
		=> ToListAsync(command, behavior, transform, true, cancellationToken);

	/// <inheritdoc cref="ToListAsync{T}(DbCommand, CommandBehavior, Func{IDataRecord, T}, bool, CancellationToken)"/>
	public static ValueTask<List<T>> ToListAsync<T>(this DbCommand command,
		Func<IDataRecord, T> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> ToListAsync(command, behavior, transform, useReadAsync, cancellationToken);

	/// <summary>
	/// Iterates all records using an <see cref="IDataReader"/> and returns the desired results as an array.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	public static T[] ToArray<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).ToArray();
	}

	/// <inheritdoc cref="ToArray{T}(IDbCommand, CommandBehavior, Func{IDataRecord, T})" />
	public static T[] ToArray<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		=> ToArray(command, behavior, transform);

	/// <inheritdoc cref="ToArray{T}(IDbCommand, CommandBehavior, Func{IDataRecord, T})" />
	public static ImmutableArray<T> ToImmutableArray<T>(this IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).ToImmutableArray();
	}

	/// <inheritdoc cref="ToArray{T}(IDbCommand, CommandBehavior, Func{IDataRecord, T})" />
	public static ImmutableArray<T> ToImmutableArray<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
		=> ToImmutableArray(command, behavior, transform);

	/// <summary>
	/// Loads all data from a command through an <see cref="IDataReader"/> into a DataTable.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The resultant DataTable.</returns>
	public static DataTable ToDataTable(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.ToDataTable();
	}

	/// <summary>
	/// Loads all data from a command through an <see cref="IDataReader"/> into a DataTables.
	/// Calls .NextResult() to check for more results.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The resultant list of DataTables.</returns>
	public static List<DataTable> ToDataTables(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.ToDataTables();
	}

	/// <summary>
	/// Executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	public static void ExecuteReader(this IDbCommand command, Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		handler(reader);
	}

	/// <summary>
	/// Executes a reader on a command with a transform function.
	/// </summary>
	/// <remarks>The default behavior will open a connection, execute the reader and close the connection it if was not already open.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="transform">The transform function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The result of the transform.</returns>
	public static T ExecuteReader<T>(this IDbCommand command, Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return transform(reader);
	}

	/// <summary>
	/// If the <paramref name="command"/> is derived from <see cref="DbCommand"/>, this will call <see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>;
	/// otherwise it will call <see cref="IDbCommand.ExecuteReader(CommandBehavior)"/>.
	/// </summary>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	public static async ValueTask<IDataReader> ExecuteReaderAsync(this IDbCommand command,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		return command is DbCommand c
			? await c.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false)
			: command.ExecuteReader(behavior);
	}

	/// <summary>
	/// Asynchronously executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	public static async ValueTask ExecuteReaderAsync(this DbCommand command,
		Action<DbDataReader> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		handler(reader);
	}

	/// <summary>
	/// Asynchronously executes a reader on a command with a handler function.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="ExecuteReaderAsync(DbCommand, Action{DbDataReader}, CommandBehavior, CancellationToken)"/>
	public static async ValueTask ExecuteReaderAsync(this IDbCommand command,
		Func<IDataReader, ValueTask> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		if (command is DbCommand c)
		{
			await c.ExecuteReaderAsync(reader => handler(reader), behavior, cancellationToken).ConfigureAwait(false);
			return;
		}

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		await handler(reader).ConfigureAwait(false);
	}

	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="ExecuteReaderAsync(DbCommand, Action{DbDataReader}, CommandBehavior, CancellationToken)"/>
	public static async ValueTask ExecuteReaderAsync(this DbCommand command,
		Func<DbDataReader, ValueTask> handler,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await handler(reader).ConfigureAwait(false);
	}

	/// <summary>
	/// Asynchronously executes a reader on a command with a transform function.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="transform">The transform function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The result of the transform.</returns>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, T> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		return transform(reader);
	}

	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="transform">The transform function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, T}, CommandBehavior, CancellationToken)"/>
	public static ValueTask<T> ExecuteReaderAsync<T>(this IDbCommand command,
		Func<IDataReader, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		return command is DbCommand c
			? ExecuteReaderAsync(c, reader => transform(reader), behavior, cancellationToken)
			: ExecuteReaderAsyncCore(command, transform, behavior);

		static async ValueTask<T> ExecuteReaderAsyncCore(IDbCommand command, Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior)
		{
			var state = command.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			return await transform(reader).ConfigureAwait(false);
		}
	}

	/// <inheritdoc cref="ExecuteReaderAsync{T}(DbCommand, Func{DbDataReader, T}, CommandBehavior, CancellationToken)"/>
	public static async ValueTask<T> ExecuteReaderAsync<T>(this DbCommand command,
		Func<DbDataReader, ValueTask<T>> transform,
		CommandBehavior behavior = CommandBehavior.Default,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		return await transform(reader).ConfigureAwait(false);
	}

	/// <inheritdoc cref="IterateReader{TEntity, TResult}(IDbCommand, CommandBehavior, Func{IDataRecord, TEntity}, Func{IEnumerable{TEntity}, TResult})"/>
	public static TResult IterateReader<TEntity, TResult>(
		this IDbCommand command,
		Func<IDataRecord, TEntity> transform,
		Func<IEnumerable<TEntity>, TResult> selector,
		CommandBehavior behavior = CommandBehavior.Default)
		=> IterateReader(command, behavior, transform, selector);

	/// <summary>
	/// Executes a reader on a command with a transform function.
	/// </summary>
	/// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
	/// <typeparam name="TResult">The type returned by the selector.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="transform">The transform function for each <see cref="IDataRecord"/>.</param>
	/// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
	/// <inheritdoc cref="ExecuteReader{T}(IDbCommand, Func{IDataReader, T}, CommandBehavior)"/>
	public static TResult IterateReader<TEntity, TResult>(
		this IDbCommand command,
		CommandBehavior behavior,
		Func<IDataRecord, TEntity> transform,
		Func<IEnumerable<TEntity>, TResult> selector)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		if (selector is null) throw new ArgumentNullException(nameof(selector));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return selector(reader.Select(transform));
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> on a command with a handler function.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	public static void IterateReader(
		this IDbCommand command,
		CommandBehavior behavior,
		Action<IDataRecord> handler)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		reader.ForEach(handler);
	}

	/// <inheritdoc cref="IterateReader(IDbCommand, CommandBehavior, Action{IDataRecord})"/>
	public static void IterateReader(
		this IDbCommand command,
		Action<IDataRecord> handler,
		CommandBehavior behavior = CommandBehavior.Default)
		=> IterateReader(command, behavior, handler);

	internal static IEnumerable<T> IterateReaderInternal<T>(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
	{
		return command is null
			? throw new ArgumentNullException(nameof(command))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: IterateReaderInternalCore(command, behavior, transform);

		static IEnumerable<T> IterateReaderInternalCore(IDbCommand command, CommandBehavior behavior, Func<IDataRecord, T> transform)
		{
			var state = command.EnsureOpen();
			if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
			using var reader = command.ExecuteReader(behavior);
			while (reader.Read())
				yield return transform(reader);
		}
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> on a command while the predicate returns true.
	/// </summary>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="predicate">The handler function that processes each <see cref="IDataRecord"/> and decides if iteration should continue.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	public static void IterateReaderWhile(this IDbCommand command,
		Func<IDataRecord, bool> predicate,
		CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		reader.IterateWhile(predicate);
	}

	/// <summary>
	/// Asynchronously iterates all records from an <see cref="IDataReader"/>.
	/// </summary>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	public static async ValueTask ForEachAsync(this DbCommand command,
		Action<IDataRecord> handler,
		CommandBehavior behavior,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await reader.ForEachAsync(handler, useReadAsync, cancellationToken).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ForEachAsync(DbCommand, Action{IDataRecord}, CommandBehavior, bool, CancellationToken)"/>
	public static ValueTask ForEachAsync(this DbCommand command,
		Action<IDataRecord> handler,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> ForEachAsync(command, handler, CommandBehavior.Default, useReadAsync, cancellationToken);

	/// <inheritdoc cref="ForEachAsync(DbCommand, Action{IDataRecord}, CommandBehavior, bool, CancellationToken)"/>
	public static async ValueTask ForEachAsync(this DbCommand command,
		Func<IDataRecord, ValueTask> handler, CommandBehavior behavior,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		cancellationToken.ThrowIfCancellationRequested();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await reader.ForEachAsync(handler, useReadAsync, cancellationToken).ConfigureAwait(false);
	}

	/// <inheritdoc cref="ForEachAsync(DbCommand, Action{IDataRecord}, CommandBehavior, bool, CancellationToken)"/>
	public static ValueTask ForEachAsync(this DbCommand command,
		Func<IDataRecord, ValueTask> handler,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
		=> ForEachAsync(command, handler, CommandBehavior.Default, useReadAsync, cancellationToken);

	/// <summary>
	/// Asynchronously iterates an <see cref="IDataReader"/> on a command while the predicate returns true.
	/// </summary>
	/// <param name="command">The <see cref="DbCommand"/> to generate a reader from.</param>
	/// <param name="predicate">The handler function that processes each <see cref="IDataRecord"/> and decides if iteration should continue.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	public static async ValueTask IterateReaderWhileAsync(this DbCommand command, Func<IDataRecord, ValueTask<bool>> predicate, CommandBehavior behavior = CommandBehavior.Default, bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		var state = await command.EnsureOpenAsync(cancellationToken).ConfigureAwait(false);
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
#if NETSTANDARD2_0
#else
		await
#endif
		using var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
		await reader.IterateWhileAsync(predicate, useReadAsync, cancellationToken).ConfigureAwait(false);
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and returns the first result through a transform function.
	/// </summary>
	/// <remarks>Throws an <see cref="InvalidOperationException"/> if there are no results.</remarks>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The value from the transform.</returns>
	public static T First<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
		return reader.Select(transform).First();
	}

	/// <remarks>Returns <c>default(T)</c> if thre are no results.</remarks>
	/// <inheritdoc cref="First{T}(IDbCommand, Func{IDataRecord, T}, CommandBehavior)"/>
	public static T? FirstOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleRow);
		return reader.Select(transform).FirstOrDefault();
	}

	/// <remarks>Throws an <see cref="InvalidOperationException"/> if there is anything other than a single result.</remarks>
	/// <inheritdoc cref="First{T}(IDbCommand, Func{IDataRecord, T}, CommandBehavior)"/>
	public static T Single<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).Single();
	}

	/// <remarks>
	/// Returns <c>default(T)</c> if thre are no results.
	/// Throws an <see cref="InvalidOperationException"/> if there is more than one result.
	/// </remarks>
	/// <inheritdoc cref="FirstOrDefault{T}(IDbCommand, Func{IDataRecord, T}, CommandBehavior)"/>
	public static T? SingleOrDefault<T>(this IDbCommand command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).SingleOrDefault();
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and returns the first number of results defined by the count.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="count">The maximum number of records to return.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The results from the transform limited by the take count.</returns>
	public static List<T> Take<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		return reader.Select(transform).Take(count).ToList();
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and skips the first number of results defined by the count.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="count">The number of records to skip.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The results from the transform after the skip count.</returns>
	public static List<T> Skip<T>(this IDbCommand command, int count, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		while (0 < count--) reader.Read();
		return reader.Select(transform).ToList();
	}

	/// <summary>
	/// Iterates an <see cref="IDataReader"/> and skips by the skip parameter returns the maximum remaining defined by the take parameter.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="skip">The number of entries to skip before starting to take results.</param>
	/// <param name="take">The maximum number of records to return.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <returns>The results from the skip, transform and take operation.</returns>
	public static List<T> SkipThenTake<T>(this IDbCommand command, int skip, int take, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior);
		while (0 < skip--) reader.Read();
		return reader.Select(transform).Take(take).ToList();
	}

	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <inheritdoc cref="FirstOrdinalResultsAsync{T0}(DbCommand, CommandBehavior, bool, CancellationToken)"/>
	public static IEnumerable<object?> FirstOrdinalResults(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		var results = new Queue<object>(IterateReaderInternal(command, behavior | CommandBehavior.SingleResult, r => r.GetValue(0)));
		return results.DequeueEach().DBNullToNull();
	}

	/// <inheritdoc cref="FirstOrdinalResultsAsync{T0}(DbCommand, CommandBehavior, bool, CancellationToken)"/>
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
		=> command is DbCommand dbc
		? dbc.FirstOrdinalResults<T0>()
		: command.FirstOrdinalResults(behavior | CommandBehavior.SingleResult).Cast<T0>();

	/// <typeparam name="T0">The expected type of the first ordinal.</typeparam>
	/// <inheritdoc cref="FirstOrdinalResultsAsync{T0}(DbCommand, CommandBehavior, bool, CancellationToken)"/>
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess)
	{
		if (command is null) throw new ArgumentNullException(nameof(command));
		Contract.EndContractBlock();

		var state = command.EnsureOpen();
		if (state == ConnectionState.Closed) behavior |= CommandBehavior.CloseConnection;
		using var reader = command.ExecuteReader(behavior | CommandBehavior.SingleResult);
		return reader.FirstOrdinalResults<T0>();
	}

	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <inheritdoc cref="FirstOrdinalResultsAsync{T0}(DbCommand, CommandBehavior, bool, CancellationToken)"/>
	public static ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, bool useReadAsync = true, CancellationToken cancellationToken = default)
		=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync(useReadAsync, cancellationToken), behavior | CommandBehavior.SingleResult, cancellationToken);

	/// <summary>
	/// Reads the first column from every record.
	/// </summary>
	/// <typeparam name="T0">The expected type of the first ordinal.</typeparam>
	/// <remarks>Any <see cref="DBNull"/> values are then converted to null and casted to type <typeparamref name="T0"/>.</remarks>
	/// <param name="command">The <see cref="IDbCommand"/> to generate a reader from.</param>
	/// <param name="behavior">The behavior to use with the data reader.</param>
	/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>The enumerable of casted values.</returns>
	public static ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbCommand command, CommandBehavior behavior = CommandBehavior.SequentialAccess, bool useReadAsync = true, CancellationToken cancellationToken = default)
		=> command.ExecuteReaderAsync(reader => reader.FirstOrdinalResultsAsync<T0>(useReadAsync, cancellationToken), behavior | CommandBehavior.SingleResult, cancellationToken);
}
