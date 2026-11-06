
namespace Open.Database.Extensions.Core;

/// <summary>
/// Utility of transforming retrieved data into models matching the type parameter provided.
/// </summary>
/// <remarks>
/// The mapping is a compiled, cached <see cref="RowMap{T}"/>: constructing a transformer and applying it to
/// a result no longer rebuilds lookups or property setters per query.
/// </remarks>
/// <typeparam name="T">The type of the model to create from the data.</typeparam>
public class Transformer<T>
	where T : new()
{
	/// <summary>
	/// Maximum number of arrays to hold in the local array pool per bucket.
	/// May also define how many records are pre buffered before transforming.
	/// </summary>
	protected const int MaxArrayBuffer = 1024;

	/// <summary>
	/// Buffers for transforming.
	/// </summary>
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter.", Justification = "<Pending>")]
	protected internal static readonly ArrayPool<object> LocalPool = ArrayPool<object>.Create(1024, MaxArrayBuffer);

	/// <summary>
	/// The type of <typeparamref name="T"/>.
	/// </summary>
	public Type Type => typeof(T);

	readonly RowMap<T> _plan;

	/// <summary>
	/// The property names.
	/// </summary>
	public IEnumerable<string> PropertyNames => _plan.PropertyNames;

	/// <summary>
	/// The column names.
	/// </summary>
	public IEnumerable<string> ColumnNames => _plan.ColumnNames;

	/// <summary>
	/// Constructs a transformer using the optional map.
	/// </summary>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	protected internal Transformer(RowMap<T>? map = null)
		=> _plan = map ?? RowMap<T>.Default;

	/// <inheritdoc cref="Create(RowMap{T}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(1)]
	public static Transformer<T> Create() => new();

	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	/// <param name="map">An optional prebuilt <see cref="RowMap{T}"/>; <see langword="null"/> reads by column name.</param>
	/// <remarks>Declare a <see cref="RowMap{T}"/> once and pass it here; a <c>new RowMap&lt;T&gt;([...])</c> or an <see cref="ImmutableArray{T}"/> passed to the constructor at the call site builds a new map, and compiles its row reader, on every call.</remarks>
	/// <seealso cref="RowMap{T}"/>
	public static Transformer<T> Create(RowMap<T>? map)
		=> new(map);

	/// <inheritdoc cref="Create(RowMap{T}?)"/>
	/// <remarks>Convenience overload. It builds a new <see cref="RowMap{T}"/> and compiles its row reader on every call; declare a <see cref="RowMap{T}"/> once and pass it for anything that runs more than occasionally. The tuples are validated strictly: an unknown or repeated property, an empty column name, or two properties on one column throw <see cref="ArgumentException"/>.</remarks>
	/// <seealso cref="RowMap{T}"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(-1)]
	public static Transformer<T> Create(params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		=> new(RowMap<T>.FromTuples(fieldMappingOverrides, nameof(fieldMappingOverrides)));

	/// <summary>
	/// A sub class for processing the transformer results.
	/// </summary>
	/// <remarks>
	/// Constructs a processor.
	/// </remarks>
	/// <param name="transformer">The transformer to use.</param>
	protected class Processor(Transformer<T> transformer)
	{
		/// <summary>
		/// Constructs a processor.
		/// </summary>
		/// <param name="transformer">The transformer to use.</param>
		/// <param name="names">The names of columns/properties to acquire.</param>
		public Processor(Transformer<T> transformer, ImmutableArray<string> names)
			: this(transformer) => SetNames(names);

		/// <summary>
		/// The transformer being used.
		/// </summary>
		public Transformer<T> Transformer { get; } = transformer;

		/// <summary>
		/// The resultant transform function.
		/// </summary>
		public Func<object?[], T> Transform { get; private set; }
			= static _ => new T(); // Using a Func<object?[],T> for better type inference.

		/// <summary>
		/// Allows for deferred initialization.
		/// </summary>
		/// <param name="names">The column/property names to process.</param>
		public void SetNames(ImmutableArray<string> names)
			=> Transform = Transformer._plan.ArrayMapper(names);
	}

	/// <inheritdoc cref="AsDequeueingEnumerable(QueryResult{Queue{object?[]}}, ArrayPool{object?}, bool)" />
	public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object?[]>> results)
	{
		if (results is null) throw new ArgumentNullException(nameof(results));
		Contract.EndContractBlock();

		return Drain(results.Result, _plan.ArrayMapper(results.Names));
	}

	/// <summary>
	/// Dequeues the results and transforms each one by one during enumeration.
	/// </summary>
	/// <param name="results">The results to process.</param>
	/// <param name="arrayPool">The array pool to return the buffers to.</param>
	/// <param name="clearArrays">Indicates whether the contents of the buffers should be cleared before reuse.</param>
	/// <returns>A dequeuing enumerable of the transformed results.</returns>
	public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results, ArrayPool<object> arrayPool, bool clearArrays = false)
	{
		if (results is null) throw new ArgumentNullException(nameof(results));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		return Drain(results.Result, _plan.ArrayMapper(results.Names), arrayPool, clearArrays);
	}

	// Each row leaves the queue as it is mapped, so it can be collected while the rest are still pending.
	static IEnumerable<T> Drain(Queue<object?[]> queue, Func<object?[], T> map)
	{
		while (queue.Count > 0)
			yield return map(queue.Dequeue());
	}

	static IEnumerable<T> Drain(Queue<object[]> queue, Func<object?[], T> map, ArrayPool<object> pool, bool clear)
	{
		while (queue.Count > 0)
		{
			object[] row = queue.Dequeue();
			T item;
			try
			{
				item = map(row);
			}
			finally
			{
				pool.Return(row, clear);
			}

			yield return item;
		}
	}

	/// <summary>
	/// Processes the data from the data table into a queue. Then dequeues the results and transforms each one by one during enumeration.
	/// </summary>
	/// <param name="table">The data to process.</param>
	/// <param name="clearTable">If true, will clear the table after buffering the data.</param>
	/// <returns>A dequeuing enumerable of the transformed results.</returns>
	public IEnumerable<T> Results(DataTable table, bool clearTable)
	{
		if (table is null) throw new ArgumentNullException(nameof(table));
		Contract.EndContractBlock();

		DataColumnCollection cols = table.Columns;
		int columnCount = cols.Count;

		// Ordinals + names in a single pass (no double Select iterators).
		int[] ordinals = new int[columnCount];
		string[] names = new string[columnCount];
		for (int i = 0; i < columnCount; i++)
		{
			DataColumn c = cols[i];
			ordinals[i] = c.Ordinal;
			names[i] = c.ColumnName;
		}

		// The row count is known, so pre-size the queue and fill it directly.
		DataRowCollection rows = table.Rows;
		var buffer = new Queue<object[]>(rows.Count);
		foreach (DataRow r in rows)
		{
			object[] a = LocalPool.Rent(columnCount);
			for (int i = 0; i < columnCount; i++) a[i] = r[i];
			buffer.Enqueue(a);
		}

		var results = new QueryResult<Queue<object[]>>(ordinals, names, buffer);

		if (clearTable) table.Rows.Clear();
		return AsDequeueingEnumerable(results, LocalPool);
	}
}
