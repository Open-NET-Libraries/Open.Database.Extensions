
namespace Open.Database.Extensions.Core;

/// <summary>
/// Transforms retrieved data into models of type <typeparamref name="T"/>.
/// </summary>
/// <remarks>
/// <para>
/// The mapping is a compiled, cached <see cref="RowMap{T}"/>:
/// constructing a transformer and applying it to a result does not rebuild lookups or property setters per query.
/// </para>
/// <para>
/// A model is built as <c>Map&lt;T&gt;()</c> builds it:
/// a type with a public parameterless constructor with <c>new T()</c>, then its properties are set;
/// any other type through its one public constructor, then the properties it does not cover.
/// A type that cannot be mapped throws <see cref="InvalidOperationException"/> when the transformer is created (see <see cref="RowMap{T}"/>).
/// </para>
/// </remarks>
/// <typeparam name="T">The type of the model to create from the data.</typeparam>
public class Transformer<T>
{
	const string ResultsObsolete = "Use table.Map(map).ToList() instead, where map is the RowMap<T> you gave Transformer<T>.Create. It returns a List<T>, not a lazy enumerable, and has no clearTable argument: clear the table yourself after reading it. Removed in 12.";

	/// <summary>
	/// The maximum number of arrays to hold in the local array pool per bucket.
	/// May also define how many records are pre-buffered before transforming.
	/// </summary>
	protected const int MaxArrayBuffer = 1024;

	/// <summary>
	/// The pool of buffers for transforming.
	/// </summary>
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter.", Justification = "<Pending>")]
	protected internal static readonly ArrayPool<object> LocalPool = ArrayPool<object>.Create(1024, MaxArrayBuffer);

	/// <summary>
	/// Gets the <see cref="System.Type"/> of <typeparamref name="T"/>.
	/// </summary>
	/// <value>The type of the model that the transformer creates.</value>
	public Type Type => typeof(T);

	readonly RowMap<T> _plan;

	/// <summary>
	/// Gets the property names.
	/// </summary>
	/// <value>The names of the properties of <typeparamref name="T"/> that the map reads a column into.</value>
	public IEnumerable<string> PropertyNames => _plan.PropertyNames;

	/// <summary>
	/// Gets the column names.
	/// </summary>
	/// <value>The names of the columns that the mapped properties read.</value>
	public IEnumerable<string> ColumnNames => _plan.ColumnNames;

	/// <summary>
	/// Finds the columns of the record that feed a property.
	/// </summary>
	/// <remarks>
	/// The columns are returned as their ordinals in ascending order and, aligned with them, their names,
	/// matched exactly as a row read matches them (exactly, ignoring case, then ignoring underscores).
	/// </remarks>
	/// <param name="record">The record whose columns are matched.</param>
	/// <param name="ordinals">The ordinals of the columns that feed a property, in ascending order.</param>
	/// <param name="names">The names of those columns, aligned with <paramref name="ordinals"/>.</param>
	internal void MatchingColumns(IDataRecord record, out int[] ordinals, out ImmutableArray<string> names) => _plan.MatchingColumns(record, out ordinals, out names);

	/// <summary>
	/// Initializes a new instance of the <see cref="Transformer{T}"/> class using the optional map.
	/// </summary>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	protected internal Transformer(RowMap<T>? map = null)
		=> _plan = map ?? RowMap<T>.Default;

	/// <summary>
	/// Creates a transformer that reads by column name.
	/// </summary>
	/// <returns>A new <see cref="Transformer{T}"/>.</returns>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(1)]
	public static Transformer<T> Create() => new();

	/// <summary>
	/// Creates a <see cref="Transformer{T}"/>.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map-declare-once']/*" />
	/// </remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	/// <returns>A new transformer that uses <paramref name="map"/>.</returns>
	/// <seealso cref="RowMap{T}"/>
	public static Transformer<T> Create(RowMap<T>? map)
		=> new(map);

	/// <summary>
	/// Processes the results of the transformer.
	/// </summary>
	/// <param name="transformer">The transformer to use.</param>
	protected class Processor(Transformer<T> transformer)
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="Processor"/> class for the named columns.
		/// </summary>
		/// <param name="transformer">The transformer to use.</param>
		/// <param name="names">The names of columns/properties to acquire.</param>
		public Processor(Transformer<T> transformer, ImmutableArray<string> names)
			: this(transformer) => SetNames(names);

		/// <summary>
		/// Gets the transformer being used.
		/// </summary>
		/// <value>The transformer the processor was created with.</value>
		public Transformer<T> Transformer { get; } = transformer;

		/// <summary>
		/// Gets the resultant transform function.
		/// </summary>
		/// <value>A function that creates a <typeparamref name="T"/> from an array of column values.</value>
		/// <remarks>
		/// Until <see cref="SetNames"/> gives it columns, it builds <typeparamref name="T"/> from no columns:
		/// its properties keep their initializers and its constructor parameters take their declared defaults.
		/// It is built from the map on first use, and only when no names were set;
		/// a static default could not reach the instance's map, and <c>new T()</c> no longer compiles.
		/// </remarks>
		public Func<object?[], T> Transform
		{
			get => field ??= Transformer._plan.ArrayMapper([]);
			private set;
		}

		/// <summary>
		/// Sets the columns to process, which allows for deferred initialization.
		/// </summary>
		/// <param name="names">The column/property names to process.</param>
		public void SetNames(ImmutableArray<string> names)
			=> Transform = Transformer._plan.ArrayMapper(names);
	}

	/// <summary>
	/// Dequeues the results and transforms each one by one during enumeration.
	/// </summary>
	/// <param name="results">The results to process.</param>
	/// <returns>A dequeuing enumerable of the transformed results.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="results"/> is <see langword="null"/>.</exception>
	/// <exception cref="ValueMappingException">A required property has no column in the results.</exception>
	public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results)
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
	/// <param name="clearArrays"><see langword="true"/> to clear the columns written to the buffers before they go back to the pool; otherwise, <see langword="false"/>. The default is <see langword="false"/>.</param>
	/// <returns>A dequeuing enumerable of the transformed results.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="results"/> or <paramref name="arrayPool"/> is <see langword="null"/>.</exception>
	/// <exception cref="ValueMappingException">A required property has no column in the results.</exception>
	public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results, ArrayPool<object> arrayPool, bool clearArrays = false)
	{
		if (results is null) throw new ArgumentNullException(nameof(results));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));
		Contract.EndContractBlock();

		return Drain(results.Result, _plan.ArrayMapper(results.Names), arrayPool, clearArrays, results.ColumnCount);
	}

	/// <summary>
	/// Dequeues the rows and maps each one during enumeration.
	/// </summary>
	/// <remarks>
	/// Each row leaves the queue as it is mapped, so it can be collected while the rest are still pending.
	/// </remarks>
	static IEnumerable<T> Drain(Queue<object[]> queue, Func<object?[], T> map)
	{
		while (queue.Count > 0)
			yield return map(queue.Dequeue());
	}

	/// <summary>
	/// Dequeues the rows and maps each one during enumeration, then returns its array to the pool.
	/// </summary>
	/// <remarks>
	/// Only the columns were written, so only they are cleared (when asked): the rest of a rented array was never used.
	/// </remarks>
	static IEnumerable<T> Drain(Queue<object[]> queue, Func<object?[], T> map, ArrayPool<object> pool, bool clear, int columnCount)
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
				// Only the columns were written, so only they are cleared: the rest of a rented array was never used.
				if (clear) Array.Clear(row, 0, Math.Min(columnCount, row.Length));
				pool.Return(row, false);
			}

			yield return item;
		}
	}

	/// <summary>
	/// Obsolete: use <c>table.Map(map).ToList()</c>.
	/// Processes the data from the data table into a queue, then dequeues the results and transforms each one by one during enumeration.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='obsolete-12']/*" />
	/// <para>
	/// That page has the difference and a before/after example.
	/// </para>
	/// </remarks>
	/// <param name="table">The data to process.</param>
	/// <param name="clearTable"><see langword="true"/> to clear the table after buffering the data; otherwise, <see langword="false"/>.</param>
	/// <returns>A dequeuing enumerable of the transformed results.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="table"/> is <see langword="null"/>.</exception>
	/// <exception cref="ValueMappingException">A required property has no column in the table; the table is not cleared then.</exception>
	[Obsolete(ResultsObsolete)]
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

		// The plan for these columns is built here, and a required property with no column makes that throw: the table is cleared only once it has not.
		IEnumerable<T> mapped = AsDequeueingEnumerable(results, LocalPool);
		if (clearTable) table.Rows.Clear();
		return mapped;
	}
}
