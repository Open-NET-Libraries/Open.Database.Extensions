namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that read data from an
/// <see cref="IExecuteReader"/>.
/// </summary>
public static class IExecuteReaderExtensions
{

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

	/// <inheritdoc cref="ToHashSetAsync{T}(IExecuteReader, IEqualityComparer{T}, CancellationToken)" path="/*[not(self::param[@name='comparer'])]"/>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(
		this IExecuteReader command,
		CancellationToken cancellationToken = default)
		=> ToHashSetAsync<T>(command, null, cancellationToken);

	/// <summary>
	/// Reads every row into a hash set, asynchronously.
	/// Equivalent to <c>command.Map&lt;T&gt;().ToHashSetAsync(comparer, cancellationToken)</c>.
	/// The token goes straight after the comparer, or first when there is no comparer:
	/// <c>ToHashSetAsync&lt;int&gt;(ct)</c>.
	/// </summary>
	/// <inheritdoc cref="ToListAsync{T}(IExecuteReader, CancellationToken)" path="/*[not(self::summary) and not(self::returns) and not(self::param)]"/>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='command']/*" />
	/// <param name="comparer">The comparer of the set, or <see langword="null"/> for the default.</param>
	/// <include file="../Mapped.docs.xml" path="docs/members[@name='token']/*" />
	/// <returns>The rows as a hash set.</returns>
	public static ValueTask<HashSet<T>> ToHashSetAsync<T>(
		this IExecuteReader command,
		IEqualityComparer<T>? comparer,
		CancellationToken cancellationToken = default)
		=> command.Map<T>().ToHashSetAsync(comparer, cancellationToken);

	#endregion

}
