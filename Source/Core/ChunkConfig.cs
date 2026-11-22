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
/// decided per chunk, by the bytes it holds (<see cref="DeferFromBytes"/>).
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
/// <param name="deferFromBytes">
/// The bytes of rows (rows written times the size of a row) from which a chunk is
/// cleared in the recycle bin (see <see cref="DeferFromBytes"/>).
/// </param>
internal sealed class ChunkConfig<T>(
	ArrayPool<T> pool,
	bool? clearOnReturn,
	ChunkSizes sizes,
	RecycleBin bin,
	int deferFromBytes)
{
	/// <summary>
	/// The bytes of rows (rows written times the size of a row: a reference is 8)
	/// from which the rows of a chunk are cleared in the recycle bin instead of
	/// where the chunk is released, for every kind of row.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Measured (the recycle bin proof, paced so the bin never fills, .NET 10).
	/// Clearing where the chunk is released costs the releasing thread about 8 to
	/// 20 ns per KB (<c>int</c> rows: 154 ns at 16 KB, 526 ns at 64 KB, 1.8 microseconds at 256 KB;
	/// rows of 80 bytes without references: 21 microseconds at 1.3 MB, 123 at 5.2 MB;
	/// references: 508 ns at 32 KB, 2.0 microseconds at 128 KB, 9.9 at 512 KB).
	/// Handing a chunk to the bin costs the releasing thread a flat 0.8 to 3 microseconds
	/// whatever the size, and the bin's thread then pays the clear: the total CPU is higher
	/// than clearing in place for a small chunk and equal or lower from about 256 KB.
	/// </para>
	/// <para>
	/// The two release costs meet at about 128 to 256 KB.
	/// The default is the lower end: the rule is to hand off unless clearing in place is
	/// certainly cheaper, and at 128 KB of references it is a tie.
	/// Rows that hold no references are included, which the earlier rule (4,096 rows, references
	/// only) was not: a large chunk of value rows is cleared in the bin too.
	/// </para>
	/// </remarks>
	public const int DefaultDeferFromBytes = 131_072;

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
	public int DeferFromBytes { get; } = deferFromBytes;

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

	/// <summary>The size of a row in the array (a reference is a pointer).</summary>
	static readonly int RowBytes = Unsafe.SizeOf<T>();

	/// <summary>Chooses what happens to the rows of a chunk on release.</summary>
	/// <remarks>
	/// <see cref="ClearMode.None"/>: only when nothing is to be cleared (no
	/// clearing asked and no references to keep alive, see the constructor), or
	/// no rows were written.
	/// <see cref="ClearMode.Deferred"/>: a chunk of (rows written times the size of a
	/// row) <see cref="DefaultDeferFromBytes"/> or more, of any kind of row; everything
	/// else that clears does it right away.
	/// </remarks>
	/// <include file="Internals.docs.xml" path="docs/members[@name='used']/*" />
	ClearMode ModeFor(int used)
		=> !ClearOnReturn || used == 0
			? ClearMode.None
			: (long)used * RowBytes >= DeferFromBytes
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
