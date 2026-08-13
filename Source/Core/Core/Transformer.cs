
#if NET10_0_OR_GREATER
using System.Collections.Frozen;
#endif

namespace Open.Database.Extensions.Core;

/// <summary>
/// Utility of transforming retrieved data into models matching the type parameter provided.
/// </summary>
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

	// Reflection is invariant per T; a static on this generic type is per-T and initialized once
	// (thread-safe), so GetProperties() and the name->PropertyInfo map aren't rebuilt per query.
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter.", Justification = "Per-T reflection cache is intentional.")]
	static readonly Dictionary<string, PropertyInfo> PropertiesByName
		= typeof(T).GetProperties().ToDictionary(p => p.Name);

	// Allow mapping key = object property, value = column name.
	readonly Dictionary<string, string> _propertyMap;

	// Column-name -> property lookup, matched case-insensitively (OrdinalIgnoreCase) so no
	// upper-cased key strings are allocated. Built once and never mutated after construction,
	// so it is frozen on the modern target for faster reads.
#if NET10_0_OR_GREATER
	readonly FrozenDictionary<string, PropertyInfo> _columnToPropertyMap;
#else
	readonly Dictionary<string, PropertyInfo> _columnToPropertyMap;
#endif

	/// <summary>
	/// The property names.
	/// </summary>
	public IEnumerable<string> PropertyNames => _propertyMap.Keys;

	/// <summary>
	/// The column names.
	/// </summary>
	public IEnumerable<string> ColumnNames => _propertyMap.Values;

	/// <summary>
	/// Constructs a transformer using the optional field overrides.
	/// </summary>
	protected internal Transformer(IEnumerable<(string Field, string? Column)>? overrides = null)
	{
		// Per-instance copy of the default (property-name -> same column-name) map, sized exactly,
		// so the optional overrides can mutate it without touching the shared reflection cache.
		_propertyMap = new Dictionary<string, string>(PropertiesByName.Count);
		foreach (string name in PropertiesByName.Keys)
			_propertyMap[name] = name;

		if (overrides != null)
		{
			foreach ((string Field, string? Column) in overrides)
			{
				if (Column == null) _propertyMap.Remove(Field); // Null column indicates 'ignore this field'.
				else _propertyMap[Field] = Column;
			}
		}

		// Project column-name -> property straight into the target map with a case-insensitive
		// comparer: no upper-cased key strings, and no intermediate Dictionary on the frozen path.
#if NET10_0_OR_GREATER
		_columnToPropertyMap = _propertyMap.ToFrozenDictionary(
			kvp => kvp.Value, kvp => PropertiesByName[kvp.Key], StringComparer.OrdinalIgnoreCase);
#else
		_columnToPropertyMap = _propertyMap.ToDictionary(
			kvp => kvp.Value, kvp => PropertiesByName[kvp.Key], StringComparer.OrdinalIgnoreCase);
#endif
	}

	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	/// <param name="overrides"></param>
	public static Transformer<T> Create(IEnumerable<(string Field, string? Column)>? overrides = null)
		=> new(overrides);

	/// <summary>
	/// A sub class for processing the transformer results.
	/// </summary>
	protected class Processor
	{
		/// <summary>
		/// Constructs a processor.
		/// </summary>
		/// <param name="transformer">The transformer to use.</param>
		public Processor(Transformer<T> transformer)
		{
			Transformer = transformer;
			Transform = record =>
			{
				var model = new T();
				int count = _names.Length;
				for (int i = 0; i < count; i++)
				{
					Action<T, object?>? p = _propertySetters[i];
					if (p != null)
					{
						object? value = record[i];
						if (value == DBNull.Value) value = null;
						try
						{
							p(model, value);
						}
						catch (Exception ex)
						{
							throw new InvalidOperationException($"Unable to set value of property '{_names[i]}'.", ex);
						}
					}
				}

				return model;
			};
		}

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
		public Transformer<T> Transformer { get; }

		ImmutableArray<string> _names = [];
		Action<T, object?>?[] _propertySetters = [];

		/// <summary>
		/// The resultant transform function.
		/// </summary>
		public Func<object?[], T> Transform { get; } // Using a Func<object?[],T> for better type inference.

		/// <summary>
		/// Allows for deferred initialization.
		/// </summary>
		/// <param name="names">The column/property names to process.</param>
		public void SetNames(ImmutableArray<string> names)
		{
			var map = Transformer._columnToPropertyMap;
			_names = names;
			_propertySetters = names
				.Select(n => map.TryGetValue(n, out PropertyInfo? p) ? p.BuildUntypedSetter<T>() : null)
				.ToArray();
		}
	}

	/// <inheritdoc cref="AsDequeueingEnumerable(QueryResult{Queue{object?[]}}, ArrayPool{object?}, bool)" />
	public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object?[]>> results)
	{
		if (results is null) throw new ArgumentNullException(nameof(results));
		Contract.EndContractBlock();

		var processor = new Processor(this, results.Names);
		Queue<object?[]> q = results.Result;

		return q.DequeueEach().Select(processor.Transform);
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

		var processor = new Processor(this, results.Names);
		Func<object?[], T> transform = processor.Transform;
		Queue<object[]> q = results.Result;

		return q.DequeueEach().Select(a =>
		{
			try
			{
				return transform(a);
			}
			finally
			{
				arrayPool.Return(a, clearArrays);
			}
		});
	}

	// Splits the matched (Name, Ordinal) columns into an ordinal array (which hits the IList<int>
	// fast path in the reader enumeration) and an exactly-sized immutable name array, in one pass —
	// avoiding two lazy Select iterators and a growing ImmutableArray builder per query.
	static (int[] Ordinals, ImmutableArray<string> Names) SplitColumns((string Name, int Ordinal)[] columns)
	{
		int n = columns.Length;
		var ordinals = new int[n];
		var names = ImmutableArray.CreateBuilder<string>(n);
		names.Count = n;
		for (int i = 0; i < n; i++)
		{
			ordinals[i] = columns[i].Ordinal;
			names[i] = columns[i].Name;
		}

		return (ordinals, names.MoveToImmutable());
	}

	/// <inheritdoc cref="ResultsBuffered(IDataReader, bool)"/>
	internal IEnumerable<T> Results(IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		// Ignore missing columns.
		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(_propertyMap.Values, true);
		(int[] ordinals, ImmutableArray<string> names) = SplitColumns(columns);
		var processor = new Processor(this, names);
		Func<object?[], T> transform = processor.Transform;

		return reader
			.AsEnumerable(ordinals, LocalPool)
			.Select(a =>
			{
				try
				{
					return transform(a);
				}
				finally
				{
					LocalPool.Return(a);
				}
			});
	}

	/// <summary>
	/// Transforms the results
	/// </summary>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="readStarted"></param>
	/// <returns>An enumerable that transforms the results.</returns>
	internal IEnumerable<T> ResultsBuffered(IDataReader reader, bool readStarted)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		if (!readStarted && !reader.Read())
			return Enumerable.Empty<T>();

		// Ignore missing columns.
		(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(_propertyMap.Values, true);

		// NOTE: pass the lazy projections straight through — RetrieveInternal materializes the
		// ordinals into an ImmutableArray that QueryResult then reuses zero-copy (Immute fast path);
		// pre-materializing here would add a redundant array.
		return AsDequeueingEnumerable(
			CoreExtensions.RetrieveInternal(
				LocalPool,
				reader,
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.Name),
				readStarted: readStarted),
			LocalPool);
	}

#if NETSTANDARD2_0
#else
	/// <param name="reader">The reader to read from.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <inheritdoc cref="ResultsBuffered(IDataReader, bool)"/>
	internal IAsyncEnumerable<T> ResultsAsync(DbDataReader reader, CancellationToken cancellationToken)
	{
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ResultsAsyncCore(reader, cancellationToken);

		async IAsyncEnumerable<T> ResultsAsyncCore(DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			// Ignore missing columns.
			(string Name, int Ordinal)[] columns = reader.GetMatchingOrdinals(_propertyMap.Values, true);
			(int[] ordinals, ImmutableArray<string> names) = SplitColumns(columns);
			var processor = new Processor(this, names);

			await foreach (object[] a in reader.AsAsyncEnumerable(ordinals, LocalPool, cancellationToken))
			{
				try
				{
					yield return processor.Transform(a);
				}
				finally
				{
					LocalPool.Return(a);
				}
			}
		}
	}
#endif

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
		var ordinals = new int[columnCount];
		var names = new string[columnCount];
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
