namespace Open.Database.Extensions;

/// <summary>
/// The early-stopping terminals that the types of this library implement as members,
/// so that the extensions on <see cref="IMappedQuery{T}"/> can call them
/// instead of running LINQ over the rows.
/// </summary>
/// <typeparam name="T">The type each row is mapped to.</typeparam>
/// <remarks>
/// Internal on purpose: it is not part of the public API.
/// A type of this library is told apart from another implementation of
/// <see cref="IMappedQuery{T}"/> by this interface, and only the library's own types have it.
/// Through <see cref="IMappedQuery{T}"/> they do what the members do:
/// the library's no-rows message, and the read-through of the later result sets.
/// </remarks>
internal interface IMappedTerminals<T>
{
	T First();

	T Single();

	T? FirstOrDefault();

	T? SingleOrDefault();

	ValueTask<T> FirstAsync(
		CancellationToken cancellationToken = default);

	ValueTask<T> SingleAsync(
		CancellationToken cancellationToken = default);

	ValueTask<T?> FirstOrDefaultAsync(
		CancellationToken cancellationToken = default);

	ValueTask<T?> SingleOrDefaultAsync(
		CancellationToken cancellationToken = default);
}
