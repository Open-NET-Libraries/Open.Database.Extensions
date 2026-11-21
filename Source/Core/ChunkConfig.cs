namespace Open.Database.Extensions.Core;

/// <summary>
/// Everything a call needs to rent, seal and release pooled chunks: the
/// pool, whether the rows are cleared on return, the chunk sizes and the
/// recycle bin.
/// </summary>
/// <remarks>
/// <para>
/// One immutable object per query shape, not per call: a query is given one
/// when it is made (the default of the entry point, see Views) or by
/// <c>UsePool</c>, so no call allocates it.
/// Every part is provided by whoever builds it: nothing here defaults to a
/// shared pool, and no terminal chooses one.
/// A lease holds a reference to it, which is how a release knows its pool.
/// </para>
/// <para>
/// Clearing is ours, and only of the rows that were written: when the
/// whole array was used the pool does it (<c>Return(clearArray: true)</c>
/// clears the same slots), otherwise the used part is cleared with a span
/// and the array is returned without the flag.
/// Whether that happens where the release happens or in the recycle bin is
/// decided per chunk, by its size (<see cref="DeferFromRows"/>).
/// </para>
/// </remarks>
/// <typeparam name="T">The type of each row.</typeparam>
/// <param name="pool">The pool the arrays come from and go back to.</param>
/// <param name="clearOnReturn">
/// What was asked: <see langword="null"/> for the app-wide default,
/// <see langword="true"/> or <see langword="false"/> for the caller's choice
/// (see <see cref="ClearOnReturn"/>).
/// </param>
/// <param name="sizes">The chunk sizes of the collectors, buffers and streams.</param>
/// <param name="bin">The recycle bin that clears deferred chunks.</param>
/// <param name="deferFromRows">
/// The chunk size from which a chunk is cleared in the recycle bin (see
/// <see cref="DeferFromRows"/>).
/// </param>
internal sealed class ChunkConfig<T>(
	ArrayPool<T> pool,
	bool? clearOnReturn,
	ChunkSizes sizes,
	RecycleBin bin,
	int deferFromRows)
{
	/// <summary>
	/// The chunk size (rows written) from which the rows of a chunk that holds
	/// references are cleared in the recycle bin instead of where the chunk is
	/// released. Chunks of rows with no references are always cleared where
	/// they are released.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Measured (BenchmarkDotNet, in process, .NET 10).
	/// Clearing the rows where the chunk is released adds 3 to 5 ns up to 256
	/// rows, and about 100 / 425 / 850 ns at 1,024 / 4,096 / 8,192 class rows;
	/// for <c>int</c> rows it is about 200 ns even at 4,096 and 8,192.
	/// </para>
	/// <para>
	/// Handing a chunk to the bin costs the releasing thread about 130 to 150
	/// ns whatever the size (the queue write and the wake-up), and the array is
	/// not back in the pool until the loop gets to it: a tight loop that rents
	/// again right after posting misses the pool, so the whole cycle (fill,
	/// read, release) was slower through the bin in every measured row (19.5
	/// against 32.0 microseconds for a 10,000 row buffer of
	/// <c>int</c> rows).
	/// The bin pays only for a large clear of rows that hold references, where
	/// it takes more than a post off the consumer's thread: from 4,096 rows.
	/// </para>
	/// </remarks>
	public const int DefaultDeferFromRows = 4_096;

	/// <summary>Gets the pool the arrays come from and go back to.</summary>
	public ArrayPool<T> Pool { get; } = pool;

	/// <summary>Gets whether the rows are cleared before an array goes back.</summary>
	/// <remarks>
	/// Resolved once, here, from what was asked (the constructor takes the raw
	/// intent: <see langword="null"/> is the app-wide default,
	/// <see langword="true"/> and <see langword="false"/> are what the caller
	/// chose), and the floor is enforced here too, not trusted to the caller:
	/// a row type that holds references is cleared whatever was asked
	/// (<see cref="PoolClearing"/>), so such rows are never given
	/// <see cref="ClearMode.None"/>.
	/// </remarks>
	public bool ClearOnReturn { get; } = PoolClearing.Resolve<T>(clearOnReturn);

	/// <summary>Gets the chunk sizes of the collectors, buffers and streams.</summary>
	public ChunkSizes Sizes { get; } = sizes;

	/// <summary>Gets the recycle bin that clears deferred chunks.</summary>
	public RecycleBin Bin { get; } = bin;

	/// <summary>
	/// Gets the number of rows from which a chunk is cleared in the recycle
	/// bin; a smaller one is cleared where it is released.
	/// </summary>
	public int DeferFromRows { get; } = deferFromRows;

	/// <summary>
	/// Rents an array of at least <paramref name="size"/> slots.
	/// </summary>
	/// <param name="size">The least number of slots.</param>
	/// <returns>An array of at least <paramref name="size"/> slots.</returns>
	/// <exception cref="InvalidOperationException">
	/// The pool returned a shorter array than asked for (it is given back
	/// first).
	/// </exception>
	public T[] Rent(int size)
	{
		T[] array = Pool.Rent(size);
		if (array.Length < size)
		{
			Pool.Return(array, clearArray: false);
			throw new InvalidOperationException(
				"The pool returned less memory than asked for.");
		}

		return array;
	}

	/// <summary>
	/// Makes the lease of an array that holds <paramref name="used"/> rows,
	/// with the clear mode that suits that many rows.
	/// </summary>
	/// <param name="array">The array, rented from <see cref="Pool"/>.</param>
	/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
	/// <returns>The lease, to be released once.</returns>
	public ArrayLease<T> Seal(T[] array, int used)
		=> new(array, used, this, ModeFor(used));

	/// <summary>Whether a row of <typeparamref name="T"/> is, or contains, a reference.</summary>
	static readonly bool HoldsReferences = PoolClearing.HoldsReferences<T>();

	/// <summary>Chooses what happens to the rows of a chunk on release.</summary>
	/// <remarks>
	/// <see cref="ClearMode.None"/>: only when nothing is to be cleared (no
	/// clearing asked and no references to keep alive, see the constructor), or
	/// no rows were written.
	/// <see cref="ClearMode.Deferred"/>: only a large chunk of rows that hold
	/// references (see <see cref="DefaultDeferFromRows"/>); everything else that
	/// clears does it right away.
	/// (On netstandard2.0 every type counts as holding references, so a large
	/// chunk of value rows is deferred there too: still correct.)
	/// </remarks>
	/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
	ClearMode ModeFor(int used)
		=> !ClearOnReturn || used == 0
			? ClearMode.None
			: HoldsReferences && used >= DeferFromRows
				? ClearMode.Deferred
				: ClearMode.Immediate;

	/// <summary>
	/// Clears the first <paramref name="used"/> slots and gives the array
	/// back: with the pool's own clear when the whole array was used,
	/// otherwise with a span clear and a plain return.
	/// </summary>
	/// <param name="array">The array to clear and return.</param>
	/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
	public void ClearAndReturn(T[] array, int used)
	{
		if (used == array.Length)
		{
			Pool.Return(array, clearArray: true);
			return;
		}

		array.AsSpan(0, used).Clear();
		Pool.Return(array, clearArray: false);
	}
}
