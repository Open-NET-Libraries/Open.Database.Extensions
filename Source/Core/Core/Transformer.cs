
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

	// Compiling a setter is expensive; one per property, for the life of the process.
	static readonly System.Collections.Concurrent.ConcurrentDictionary<PropertyInfo, Action<T, object?>> Setters = new();

	/// <summary>
	/// The type of <typeparamref name="T"/>.
	/// </summary>
	public Type Type => typeof(T);

	// Reflection is invariant per T; a static on this generic type is per-T and initialized once
	// (thread-safe), so GetProperties() and the name->PropertyInfo map aren't rebuilt per query.
	// Built once, read on every query (property lookups + Keys) -> frozen on the modern target.
#if NET10_0_OR_GREATER
	static readonly FrozenDictionary<string, PropertyInfo> PropertiesByName
		= typeof(T).GetProperties().ToFrozenDictionary(p => p.Name);
#else
	static readonly Dictionary<string, PropertyInfo> PropertiesByName
		= typeof(T).GetProperties().ToDictionary(p => p.Name);
#endif

	// Allow mapping key = object property, value = column name.
	readonly Dictionary<string, string> _propertyMap;

	// Column-name -> property lookup, matched case-insensitively (OrdinalIgnoreCase) so no
	// upper-cased key strings are allocated. This map is per-query (a fresh Transformer is built
	// per operation) and read only a handful of times, so a plain Dictionary is correct here —
	// freezing a short-lived map costs more to build than the few lookups would ever recover.
	readonly Dictionary<string, PropertyInfo> _columnToPropertyMap;

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
	/// <remarks>
	/// The "key" is the field name, and the "value" is the column name.
	/// </remarks>
	protected internal Transformer(IEnumerable<KeyValuePair<string, string?>>? overrides = null)
	{
		// Per-instance copy of the default (property-name -> same column-name) map, sized exactly,
		// so the optional overrides can mutate it without touching the shared reflection cache.
		_propertyMap = [with(PropertiesByName.Count)];
		foreach (string name in PropertiesByName.Keys)
			_propertyMap[name] = name;

		if (overrides is not null)
		{
			foreach (var kvp in overrides)
			{
				if (kvp.Value == null) _propertyMap.Remove(kvp.Key); // Null column indicates 'ignore this field'.
				else _propertyMap[kvp.Key] = kvp.Value;
			}
		}

		// Project column-name -> property directly with a case-insensitive comparer (no upper-cased
		// key strings). A plain Dictionary — this map lives only for the duration of one query.
		_columnToPropertyMap = _propertyMap.ToDictionary(
			kvp => kvp.Value, kvp => PropertiesByName[kvp.Key], StringComparer.OrdinalIgnoreCase);
	}

	/// <inheritdoc cref="Create(IEnumerable{KeyValuePair{string, string?}}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(1)]
	public static Transformer<T> Create() => new();

	/// <summary>
	/// Static utility for creating a Transformer <typeparamref name="T"/>.
	/// </summary>
	public static Transformer<T> Create(params IEnumerable<KeyValuePair<string, string?>>? overrides)
		=> new(overrides);

	/// <inheritdoc cref="Create(IEnumerable{KeyValuePair{string, string?}}?)"/>
	[ExcludeFromCodeCoverage]
	[OverloadResolutionPriority(-1)]
	public static Transformer<T> Create(params IEnumerable<(string Field, string? Column)>? fieldMappingOverrides)
		=> new(fieldMappingOverrides?.Select(f => new KeyValuePair<string, string?>(f.Field, f.Column)));

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
				.Select(n => map.TryGetValue(n, out PropertyInfo? p) ? Setters.GetOrAdd(p, static p => p.BuildUntypedSetter<T>()) : null)
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
