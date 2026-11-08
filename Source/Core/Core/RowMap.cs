using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Globalization;
using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// A validated, read-only mapping of the properties of <typeparamref name="T"/> to the columns that feed them, for when the columns are named differently from the properties.
/// Declare it once and pass it as the <c>map</c> argument of the query methods (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …).
/// </summary>
/// <remarks>
/// Properties the map does not mention still map by name: a column feeds the property of that name exactly, else ignoring case, else ignoring case and underscores (<c>q_type</c> feeds <c>QType</c>).
/// A property with a pair is only matched by its column, exactly or ignoring case; a <see langword="null"/> column means "do not map this property".
/// The pairs are validated when the map is constructed: an unknown or unsettable property, a property named twice, an empty column name,
/// or two properties reading one column throw <see cref="ArgumentException"/>.
/// A type that cannot be mapped throws <see cref="InvalidOperationException"/> when the map is constructed or <see cref="Default"/> is read, which every query method does at the call, even when the result is empty:
/// an interface, an abstract class, a delegate, a single value such as <see cref="int"/> or <see cref="string"/> and, for now, a type with no public parameterless constructor or with <c>required</c> members.
/// A NULL follows the property's declared type: a nullable property (<see cref="Nullable{T}"/> or a reference type) receives <see langword="null"/>; a non-nullable value-type property (an enum counts) keeps the value <c>new T()</c> gave it, its initializer else its default.
/// A map is immutable and thread-safe, and caches its compiled row readers. One built at a call site is rebuilt on every call, so declare it once.
/// Declare it with the collection syntax, <c>RowMap&lt;Post&gt; map = [new(nameof(Post.Id), "post_id")];</c>, or with the constructor, from one pair or several; a map enumerates the pairs it was declared with.
/// </remarks>
/// <example>
/// <code>
/// public static RowMap&lt;Post&gt; PostMap =&gt; field ??= [new(nameof(Post.Id), "post_id"), new(nameof(Post.Title), "post_title")];
///
/// var posts = connection.Command("select * from posts").Map&lt;Post&gt;(PostMap).ToList();
/// </code>
/// </example>
/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
[CollectionBuilder(typeof(RowMap), nameof(RowMap.Create))]
public sealed class RowMap<T> : IEnumerable<KeyValuePair<string, string?>>, IRowSource<T>
{
	/// <summary>Creates and validates the map from one (property name, column name) pair, so <c>new RowMap&lt;T&gt;(new("A", "b"))</c> compiles. Declare it once: a map built in a call is rebuilt on every call.</summary>
	/// <param name="mapping">The (property name, column name) pair; a <see langword="null"/> column means "do not map this property".</param>
	/// <exception cref="ArgumentException">The pair is invalid (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	[OverloadResolutionPriority(1)]
	public RowMap(KeyValuePair<string, string?> mapping) : this([mapping]) { }

	/// <summary>Creates and validates the map from (property name, column name) pairs. Declare it once: a map built in a call is rebuilt on every call.</summary>
	/// <param name="mappings">The (property name, column name) pairs; a <see langword="null"/> column means "do not map this property".</param>
	/// <exception cref="ArgumentException">A pair is invalid: an unknown or unsettable property, a property named twice, an empty column name, or two properties reading one column.</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	public RowMap(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
	{
		// A model type that cannot be mapped fails here, at the call, not when the first row is read.
		TypeShape.ThrowIfUnmappable();

		// property → column, in declaration order; a property reads the column of its own name unless a pair says otherwise.
		string?[] columns = new string?[Slots.Length];
		for (int i = 0; i < columns.Length; i++) columns[i] = Slots[i].Name;

		bool[] named = Declare(mappings, columns, nameof(mappings));
		RejectSharedColumns(columns, named, nameof(mappings));

		_columns = columns;
		_named = named;
	}

	/// <summary>Creates and validates the map from (property name, column name) pairs held in an <see cref="ImmutableArray{T}"/> (a default array is no mappings). Declare it once: a map built in a call is rebuilt on every call.</summary>
	/// <param name="mappings">The (property name, column name) pairs; a <see langword="null"/> column means "do not map this property".</param>
	/// <exception cref="ArgumentException">A pair is invalid (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	[OverloadResolutionPriority(-1)]
	public RowMap(ImmutableArray<KeyValuePair<string, string?>> mappings) : this(mappings.AsSpan()) { }

	/// <inheritdoc cref="RowMap{T}.RowMap(ReadOnlySpan{KeyValuePair{string, string?}})"/>
	[OverloadResolutionPriority(-2)]
	public RowMap(IEnumerable<KeyValuePair<string, string?>>? mappings) : this(GetMappingSpan(mappings)) { }

	private static ReadOnlySpan<KeyValuePair<string, string?>> GetMappingSpan(IEnumerable<KeyValuePair<string, string?>>? mappings) => mappings switch
	{
		null => default,
		KeyValuePair<string, string?>[] array => array,
		ImmutableArray<KeyValuePair<string, string?>> ia => ia.AsSpan(),
		_ => mappings.ToArray()
	};

	/// <summary>Returns <see cref="Default"/> when <paramref name="mappings"/> is empty; otherwise creates a new map from them, exactly as the constructor does.</summary>
	/// <param name="mappings">The (property name, column name) pairs; a <see langword="null"/> column means "do not map this property".</param>
	/// <returns>The default map, or a new map built from <paramref name="mappings"/>.</returns>
	/// <exception cref="ArgumentException">A pair is invalid (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	public static RowMap<T> From(ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> mappings.Length == 0 ? Default : new RowMap<T>(mappings);

	// A map enumerates the pairs it was declared with, which also makes it a collection-expression target: each property a pair named, in declaration order,
	// with the column the pair gave it (null: an ignored property). A property no pair named is not yielded, so new RowMap<T>(map) builds an equivalent map.
	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
	{
		for (int i = 0; i < _columns.Length; i++)
			if (_named[i]) yield return new(Slots[i].Name, _columns[i]);
	}

	IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<string, string?>>)this).GetEnumerator();

	// The validated column table and the compiled, cached, typed row readers (one per result-set layout: column names + column types),
	// so after the first query of a given shape no reflection, expression building or compilation happens again.
	// Values are read with the provider's typed getters whenever the column type matches the property type (or converts numerically), so nothing is boxed.
	// The property's declared type decides what a NULL does: a reference type or Nullable<T> receives null; a non-nullable value type (an enum counts) is skipped,
	// so it keeps what new T() gave it (its initializer, else its default): one IsDBNull per such cell on the typed path, a single read of the cell on the general path.

	// The per-layout reader cache is bounded so dynamically generated column lists cannot grow memory forever.
	// Real applications use a handful of layouts; when the bound is hit the cache is simply cleared and refills.
	const int MaxLayouts = 128;

	// The per-model-type statics below (TypeShape, Slots, SlotIndex, Default) are one immutable copy per T, built once. Any cache of compiled readers keyed by model type
	// lives as long and costs a lookup per call; a static on the generic type is the cheapest place for it. Nothing that does not depend on T belongs here.

	// Whether T can be mapped at all, judged from its shape; never throws (an unmappable T holds its error), so no static initializer here can fail with it.
	static readonly Shape TypeShape = Shape.Of(typeof(T));

	// What a column can feed: one slot per settable public instance property, in declaration order (a property hidden by name keeps the first). Reflection runs once per T.
	static readonly ImmutableArray<Slot> Slots = BuildSlots();

	static ImmutableArray<Slot> BuildSlots()
	{
		if (TypeShape.Error is not null) return [];

		var seen = new HashSet<string>(StringComparer.Ordinal);
		var list = new List<Slot>();
		foreach (PropertyInfo p in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			if (p.GetIndexParameters().Length == 0 && p.GetSetMethod() is { } setter && seen.Add(p.Name))
				list.Add(new Slot(p.Name, p.PropertyType, setter));
		}

		return [.. list];
	}

	// NOTE: static initializers run in textual order; each must follow what it uses.
	static readonly FrozenDictionary<string, int> SlotIndex = BuildSlotIndex().ToFrozenDictionary();

	static Dictionary<string, int> BuildSlotIndex()
	{
		var index = new Dictionary<string, int>(Slots.Length, StringComparer.Ordinal);
		for (int i = 0; i < Slots.Length; i++) index.Add(Slots[i].Name, i);
		return index;
	}

	/// <summary>The map for the model's default mapping (property name == column name), used when no map is passed.</summary>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped: it is an interface, an abstract class, a delegate or a single value such as <see cref="int"/> or <see cref="string"/>, or it is a type that cannot be mapped yet (see <see cref="RowMap{T}"/>).</exception>
	public static RowMap<T> Default
	{
		// Null when T cannot be mapped, so reading Default throws instead of a static initializer failing.
		get => field ?? throw new InvalidOperationException(TypeShape.Error);
	} = TypeShape.Error is null ? new() : null!; // Declared after TypeShape, Slots and SlotIndex: its constructor reads them.

	// The column each property reads, aligned with Slots; null means the property is not mapped (ignored).
	// With _named, all the map keeps of the pairs.
	readonly string?[] _columns;

	// Aligned with Slots: true when a pair named the property, so what the pair says is exact and final (a pair whose column equals the property's own name included);
	// only a property no pair named is eligible for the underscore tier.
	readonly bool[] _named;

	/// <summary>The names of the properties that are mapped.</summary>
	internal IEnumerable<string> PropertyNames
	{
		get
		{
			for (int i = 0; i < _columns.Length; i++)
				if (_columns[i] is not null) yield return Slots[i].Name;
		}
	}

	/// <summary>The names of the columns the mapped properties read.</summary>
	internal IEnumerable<string> ColumnNames
	{
		get
		{
			foreach (string? column in _columns)
				if (column is not null) yield return column;
		}
	}

	/// <summary>
	/// The columns of the record that feed a property, as their ordinals in ascending order and, aligned with them, their actual names in the record: the same match (<see cref="Match"/>) a row read uses,
	/// so the pipelines that pick columns before mapping them agree with <c>Map&lt;T&gt;()</c> on which column feeds which property.
	/// </summary>
	internal void MatchingColumns(IDataRecord record, out int[] ordinals, out ImmutableArray<string> names)
	{
		string[] all = new string[record.FieldCount];
		for (int i = 0; i < all.Length; i++) all[i] = record.GetName(i);

		Slot?[] feeds = Match(all);
		int count = 0;
		foreach (Slot? feed in feeds)
			if (feed is not null) count++;

		ordinals = new int[count];
		var matched = ImmutableArray.CreateBuilder<string>(count);
		for (int i = 0, next = 0; next < count; i++)
		{
			if (feeds[i] is null) continue;

			ordinals[next++] = i;
			matched.Add(all[i]);
		}

		names = matched.MoveToImmutable();
	}

	// The property each result column feeds (null: the column is ignored). Never runs per row; linear in the columns plus the properties.
	// Three tiers run over the whole result set, in order, so a lower tier never takes a property a higher one has filled: the column a property reads exactly, the same ignoring case,
	// then, only for a property no pair named, a column equal to the property's name ignoring case and underscores. Within a tier the first matching column wins, and a column feeds one property.
	Slot?[] Match(string[] names)
	{
		var feeds = new Slot?[names.Length];
		bool[] filled = new bool[_columns.Length];

		int unmatched = names.Length - Feed(ColumnIndex(StringComparer.Ordinal), names, feeds, filled);
		if (unmatched > 0) unmatched -= Feed(ColumnIndex(StringComparer.OrdinalIgnoreCase), names, feeds, filled);
		if (unmatched > 0 && NameIndex(filled) is { } byName) Feed(byName, names, feeds, filled);

		return feeds;
	}

	// One pass over the columns that no earlier tier matched: each looks up its property, which it takes unless an earlier column already did. Returns how many it fed.
	static int Feed(Dictionary<string, int> index, string[] names, Slot?[] feeds, bool[] filled)
	{
		int fed = 0;
		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not null || names[i] is null || !index.TryGetValue(names[i], out int p) || filled[p]) continue;

			filled[p] = true;
			feeds[i] = Slots[p];
			fed++;
		}

		return fed;
	}

	// Tiers 1 and 2: column name → the first property, in declaration order, that reads it (compared as the tier says).
	Dictionary<string, int> ColumnIndex(StringComparer comparer)
	{
		var index = new Dictionary<string, int>(_columns.Length, comparer);
		for (int p = 0; p < _columns.Length; p++)
			if (_columns[p] is { } column && !index.ContainsKey(column)) index.Add(column, p);

		return index;
	}

	// Tier 3: property name → the first property, in declaration order, of the unfilled properties no pair named (every named property is left out, whatever its column), compared
	// ignoring case and underscores. Null when there is none, so nothing is built for a result set whose properties are all settled.
	Dictionary<string, int>? NameIndex(bool[] filled)
	{
		Dictionary<string, int>? index = null;
		for (int p = 0; p < _columns.Length; p++)
		{
			string name = Slots[p].Name;
			if (filled[p] || _named[p]) continue;

			index ??= new(_columns.Length, new IgnoreCaseAndUnderscores());
#if NETSTANDARD2_0
			if (!index.ContainsKey(name)) index.Add(name, p);
#else
			index.TryAdd(name, p);
#endif
		}

		return index;
	}

	// Equal ignoring case and ignoring every '_' in either name, so no stripped copy of a name is ever made: the hash and the comparison walk the characters.
	sealed class IgnoreCaseAndUnderscores : IEqualityComparer<string>
	{
		public bool Equals(string? x, string? y)
		{
			if (ReferenceEquals(x, y)) return true;
			if (x is null || y is null) return false;

			int i = 0, j = 0;
			while (true)
			{
				while (i < x.Length && x[i] == '_') i++;
				while (j < y.Length && y[j] == '_') j++;
				if (i == x.Length || j == y.Length) return i == x.Length && j == y.Length;
				if (char.ToUpperInvariant(x[i++]) != char.ToUpperInvariant(y[j++])) return false;
			}
		}

		public int GetHashCode(string name)
		{
			int hash = 17;
			foreach (char c in name)
				if (c != '_') hash = unchecked(hash * 31 + char.ToUpperInvariant(c));

			return hash;
		}
	}

	// Most recently used layout; the overwhelmingly common case is the same shape every time.
	Layout? _last;
	// Created with the first layout: a map that is never read holds no dictionary.
	ConcurrentDictionary<int, Layout[]>? _layouts;

	// Applies the pairs and reports which properties they named.
	static bool[] Declare(ReadOnlySpan<KeyValuePair<string, string?>> mappings, string?[] columns, string paramName)
	{
		bool[] named = new bool[columns.Length];
		foreach (KeyValuePair<string, string?> pair in mappings)
		{
			if (pair.Key is null) throw new ArgumentException("A mapping has no property name.", paramName);

			bool known = SlotIndex.TryGetValue(pair.Key, out int index);
			if (known)
			{
				if (named[index]) throw new ArgumentException($"The property '{pair.Key}' is mapped more than once.", paramName);
				named[index] = true;
			}

			if (pair.Value is { Length: 0 }) throw new ArgumentException($"The property '{pair.Key}' is mapped to an empty column name. Use null to ignore it.", paramName);
			if (!known) throw new ArgumentException($"'{typeof(T)}' has no settable public property named '{pair.Key}'.", paramName);

			columns[index] = pair.Value; // A null column means 'do not map this property'.
		}

		return named;
	}

	// Each mapped property is compared with the properties before it (the first reader of a column is the one that gets it).
	// Only what the map names is held to account: the model's own by-name twins are Map<T>()'s business.
	// The check is quadratic in the property count and runs once per map.
	static void RejectSharedColumns(string?[] columns, bool[] named, string paramName)
	{
		for (int i = 1; i < columns.Length; i++)
		{
			if (columns[i] is not { } column) continue;

			for (int j = 0; j < i; j++)
			{
				if (!string.Equals(columns[j], column, StringComparison.OrdinalIgnoreCase)) continue;

				if (named[i] || named[j])
					throw new ArgumentException($"The properties '{Slots[j].Name}' and '{Slots[i].Name}' both read column '{column}'. Map one of them to another column, or to null to ignore it.", paramName);

				break;
			}
		}
	}

	Func<DbDataReader, T> IRowSource<T>.Mapper(DbDataReader reader)
	{
		Layout? l = _last;
		if (l?.Matches(reader) != true) l = Resolve(reader);
		return l.Db ??= Build<DbDataReader>(l);
	}

	Func<IDataRecord, T> IRowSource<T>.Mapper(IDataRecord reader)
	{
		Layout? l = _last;
		if (l?.Matches(reader) != true) l = Resolve(reader);
		return l.Rec ??= Build<IDataRecord>(l);
	}

	Layout Resolve(IDataRecord reader)
	{
		ConcurrentDictionary<int, Layout[]> layouts = Cache(ref _layouts);
		int hash = Layout.Hash(reader);
		return _last = Find(layouts, hash, l => l.Matches(reader))
			?? Remember(layouts, hash, new Layout(reader));
	}

	// Shared by both layout caches: the dictionary is created on first use (a lost race just drops an empty one).
	static ConcurrentDictionary<int, TLayout[]> Cache<TLayout>(ref ConcurrentDictionary<int, TLayout[]>? field)
		where TLayout : class
		=> Volatile.Read(ref field) ?? Interlocked.CompareExchange(ref field, new(), null) ?? field!;

	// Shared by both layout caches: find a layout by hash and match, or remember a new one.
	static TLayout? Find<TLayout>(ConcurrentDictionary<int, TLayout[]> cache, int hash, Func<TLayout, bool> matches)
		where TLayout : class
	{
		if (cache.TryGetValue(hash, out TLayout[]? candidates))
		{
			foreach (TLayout c in candidates)
				if (matches(c)) return c;
		}

		return null;
	}

	static TLayout Remember<TLayout>(ConcurrentDictionary<int, TLayout[]> cache, int hash, TLayout layout)
		where TLayout : class
	{
		if (cache.Count >= MaxLayouts) cache.Clear();
		cache.AddOrUpdate(hash, _ => [layout], (_, existing) => [.. existing, layout]);
		return layout;
	}

	sealed class Layout
	{
		public readonly string[] Names;
		public readonly Type[] Types;
		public Func<DbDataReader, T>? Db;
		public Func<IDataRecord, T>? Rec;

		public Layout(IDataRecord r)
		{
			int n = r.FieldCount;
			Names = new string[n];
			Types = new Type[n];
			for (int i = 0; i < n; i++)
			{
				Names[i] = r.GetName(i);
				Types[i] = r.GetFieldType(i) ?? typeof(object); // Be tolerant of providers that do not report a type.
			}
		}

		public static int Hash(IDataRecord r)
		{
			int n = r.FieldCount;
			int h = n;
			for (int i = 0; i < n; i++)
			{
				h = unchecked(h * 31 + (r.GetName(i)?.GetHashCode() ?? 0));
				h = unchecked(h * 31 + (r.GetFieldType(i) ?? typeof(object)).GetHashCode());
			}

			return h;
		}

		// The same check twice on purpose: this runs on every query, and virtual calls on DbDataReader beat
		// the interface calls the IDataRecord overload must make.
		public bool Matches(DbDataReader r)
		{
			string[] names = Names;
			Type[] types = Types;
			if (r.FieldCount != names.Length) return false;
			for (int i = 0; i < names.Length; i++)
			{
				if (!string.Equals(r.GetName(i), names[i], StringComparison.Ordinal) || (r.GetFieldType(i) ?? typeof(object)) != types[i])
					return false;
			}

			return true;
		}

		public bool Matches(IDataRecord r)
		{
			string[] names = Names;
			Type[] types = Types;
			if (r.FieldCount != names.Length) return false;
			for (int i = 0; i < names.Length; i++)
			{
				if (!string.Equals(r.GetName(i), names[i], StringComparison.Ordinal) || (r.GetFieldType(i) ?? typeof(object)) != types[i])
					return false;
			}

			return true;
		}
	}

	#region Array rows (the buffered Retrieve/QueryResult route)

	// Rows that were already copied out of a reader as object[] (boxed values) are mapped by a compiled,
	// cached per-column-list delegate too, rather than rebuilding lookups and property setters per query.
	ArrayLayout? _lastArray;
	ConcurrentDictionary<int, ArrayLayout[]>? _arrayLayouts;

	sealed class ArrayLayout(string[] names)
	{
		public readonly string[] Names = names;
		public Func<object?[], T>? Map;

		public bool Matches(ImmutableArray<string> candidate)
		{
			string[] mine = Names;
			if (candidate.Length != mine.Length) return false;
			for (int i = 0; i < mine.Length; i++)
				if (!string.Equals(candidate[i], mine[i], StringComparison.Ordinal)) return false;
			return true;
		}
	}

	/// <summary>Gets the mapper from <c>object[]</c> rows whose columns are, in order, <paramref name="names"/>.</summary>
	internal Func<object?[], T> ArrayMapper(ImmutableArray<string> names)
	{
		ArrayLayout? l = _lastArray;
		if (l?.Matches(names) != true) l = ResolveArray(names);
		return l.Map ??= BuildArray(l);
	}

	ArrayLayout ResolveArray(ImmutableArray<string> names)
	{
		int hash = names.Length;
		foreach (string n in names) hash = unchecked(hash * 31 + (n?.GetHashCode() ?? 0));

		ConcurrentDictionary<int, ArrayLayout[]> layouts = Cache(ref _arrayLayouts);
		return _lastArray = Find(layouts, hash, l => l.Matches(names))
			?? Remember(layouts, hash, new ArrayLayout([.. names]));
	}

	Func<object?[], T> BuildArray(ArrayLayout layout)
	{
		ParameterExpression row = Expression.Parameter(typeof(object?[]), "row");
		MethodInfo convert = typeof(RowMap<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!;

		MethodInfo isNullCell = typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!;

		// A non-nullable value-type property is skipped when its cell is NULL.
		return Compile<object?[]>(row, layout.Names, (i, slot)
			=> Expression.Call(convert.MakeGenericMethod(slot.Type), Expression.ArrayIndex(row, Expression.Constant(i))),
			(i, slot) => KeepsValueOnNull(slot.Type) ? Expression.Call(isNullCell, Expression.ArrayIndex(row, Expression.Constant(i))) : null);
	}

	#endregion

	#region Compilation

	sealed class Binding(string property, string column, Type type)
	{
		public readonly string Property = property;
		public readonly string Column = column;
		public readonly Type Type = type;
	}

	Func<TReader, T> Build<TReader>(Layout layout)
		where TReader : class, IDataRecord
	{
		Type readerType = typeof(TReader);
		MethodInfo isDbNull = readerType.GetMethod(nameof(IDataRecord.IsDBNull), [typeof(int)])!;
		MethodInfo getValue = readerType.GetMethod(nameof(IDataRecord.GetValue), [typeof(int)])!;

		ParameterExpression r = Expression.Parameter(readerType, "r");

		// The general path reads a cell once into `cell`, tests it for NULL, and converts that same value.
		ParameterExpression cell = Expression.Variable(typeof(object), "cell");

		return Compile<TReader>(r, layout.Names, (i, slot)
			=> ReadValue(r, Expression.Constant(i), layout.Types[i], slot.Type, readerType, isDbNull, getValue, cell),
			(i, slot) => NullTest(r, Expression.Constant(i), layout.Types[i], slot.Type, readerType, isDbNull, getValue, cell),
			cell);
	}

	// The skeleton shared by both mappers: `new T()`, then for each column that maps to a property,
	// `item.Property = <cell>`. Only how a cell is read differs (a live reader vs buffered object[] rows).
	// `isNull` gives, for a property that must keep its value on NULL, the test that the cell is NULL, which wraps the assignment as
	// `if (!isNull) item.Property = <cell>`; it may leave the raw cell in `cell`, which readCell then converts.
	Func<TArg, T> Compile<TArg>(ParameterExpression arg, string[] names, Func<int, Slot, Expression> readCell,
		Func<int, Slot, Expression?> isNull, ParameterExpression? cell = null)
	{
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[names.Length];
		var statements = new List<Expression>(names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		Slot?[] feeds = Match(names);
		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			bindings[i] = new Binding(slot.Name, names[i], slot.Type);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));
			Expression assign = Expression.Call(item, slot.Setter, readCell(i, slot));
			statements.Add(isNull(i, slot) is { } skip ? Expression.IfThen(Expression.Not(skip), assign) : assign);
		}

		statements.Add(item);
		return Expression.Lambda<Func<TArg, T>>(Guard(statements, item, index, cell, bindings), arg).Compile();
	}

	// `index` tracks the column being read so a failure can name the offending property and column.
	static BlockExpression Guard(List<Expression> statements, ParameterExpression item, ParameterExpression index, ParameterExpression? cell, Binding?[] bindings)
	{
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		ParameterExpression[] variables = cell is null ? [item, index] : [item, index, cell];
		return Expression.Block(
			typeof(T),
			variables,
			Expression.Assign(index, Expression.Constant(-1)),
			Expression.TryCatch(
				Expression.Block(typeof(T), statements),
				Expression.Catch(
					ex,
					// The provider's own failures and cancellation propagate untouched; everything else is wrapped with
					// the property and column that were being read.
					Expression.Condition(
						Expression.OrElse(
							Expression.TypeIs(ex, typeof(DbException)),
							Expression.TypeIs(ex, typeof(OperationCanceledException))),
						Expression.Rethrow(typeof(T)),
						Expression.Throw(
							Expression.Call(
								typeof(RowMap<T>).GetMethod(nameof(MapError), BindingFlags.Static | BindingFlags.NonPublic)!,
								ex, index, Expression.Constant(bindings)),
							typeof(T))))));
	}

	// The typed (unboxed) read of a cell as the target type, or null when the column type has no typed getter or does not convert directly.
	static Expression? TypedRead(ParameterExpression r, Expression ordinal, Type columnType, Type target, Type readerType)
	{
		if (!TypedGetters.ByType.TryGetValue(columnType, out string? getterName)) return null;

		MethodInfo getter = readerType.GetMethod(getterName, [typeof(int)])!;
		return TryConvert(Expression.Call(r, getter, ordinal), target);
	}

	// A NULL leaves the current value for a non-nullable value type (an enum counts): Nullable<T> and reference types receive null.
	static bool KeepsValueOnNull(Type propertyType)
		=> propertyType.IsValueType && Nullable.GetUnderlyingType(propertyType) is null;

	// The expression that is true when the cell of a property that keeps its value is NULL, or null for any other property.
	// On the typed path the reader answers; on the general path the cell is read once into `cell` and ReadValue converts that same value.
	static MethodCallExpression? NullTest(
		ParameterExpression r, Expression ordinal,
		Type columnType, Type propertyType,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue, ParameterExpression cell)
	{
		if (!KeepsValueOnNull(propertyType)) return null;

		if (TypedRead(r, ordinal, columnType, propertyType, readerType) is not null)
			return Expression.Call(r, isDbNull, ordinal);

		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!,
			Expression.Assign(cell, Expression.Call(r, getValue, ordinal)));
	}

	static Expression ReadValue(
		ParameterExpression r, Expression ordinal,
		Type columnType, Type propertyType,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue,
		ParameterExpression? cell = null)
	{
		Type? underlying = Nullable.GetUnderlyingType(propertyType);
		Type target = underlying ?? propertyType;
		bool nullable = underlying is not null || !propertyType.IsValueType;

		if (TypedRead(r, ordinal, columnType, target, readerType) is { } typed)
		{
			if (!nullable) return typed;

			// NULL → null/default; otherwise the typed value (lifted to Nullable<> when needed).
			Expression present = underlying is null ? typed : Expression.Convert(typed, propertyType);
			return Expression.Condition(
				Expression.Call(r, isDbNull, ordinal),
				Expression.Default(propertyType),
				present);
		}

		// General path: the boxed value, converted. Handles enums, strings → Guid, mismatched types, unknown column types.
		// NullTest has already read the cell of a non-nullable value-type property into `cell` to test it.
		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(propertyType),
			cell is not null && !nullable ? cell : Expression.Call(r, getValue, ordinal));
	}

	// Returns the expression converted to the target type, or null when the types have no direct conversion.
	static bool IsIntegral(Type t)
		=> t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort)
			|| t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong);

	static bool IsFractional(Type t) => t == typeof(float) || t == typeof(double) || t == typeof(decimal);

	// Direct typed conversions only where they cannot silently change the value: integer to integer/enum is
	// range-checked (throws on overflow), integers and floating point widen to a fractional type. Anything
	// else (fractional to integer, which must round, text, etc.) takes the general path.
	static Expression? TryConvert(Expression value, Type target)
	{
		Type source = value.Type;
		if (source == target) return value;

		Type t = target.IsEnum ? Enum.GetUnderlyingType(target) : target;
		try
		{
			if (IsIntegral(source) && IsIntegral(t)) return Expression.ConvertChecked(value, target);
			if ((IsIntegral(source) || IsFractional(source)) && IsFractional(t)) return Expression.Convert(value, target);
		}
		catch (InvalidOperationException)
		{
			// No such conversion: fall through to the general path.
		}

		return null;
	}

	#endregion

	#region Runtime helpers called from compiled code

	static bool IsNullCell(object? value) => value is null or DBNull;

	static TTarget ConvertValue<TTarget>(object? value)
	{
		// A NULL never reaches here for a non-nullable value type: the compiled readers skip that assignment.
		if (value is null or DBNull) return default!;

		if (value is TTarget exact) return exact;

		Type target = typeof(TTarget);
		Type u = Nullable.GetUnderlyingType(target) ?? target;
		return (TTarget)GetConverted(value, u);

		static object GetConverted(object value, Type u)
		{
			if (u.IsEnum)
			{
				return value is string s
					? Enum.Parse(u, s, true)
					: Enum.ToObject(u, value);
			}

			if (u == typeof(Guid))
			{
				if (value is string gs)
					return Guid.Parse(gs);

				if (value is byte[] gb)
					return new Guid(gb);

				throw new InvalidCastException($"Cannot convert '{value.GetType()}' to Guid.");
			}

			if (u == typeof(string))
			{
				if (value is byte[])
					throw new InvalidCastException("A byte array cannot be assigned to a string; map it to byte[] or convert it explicitly.");

				return Convert.ToString(value, CultureInfo.InvariantCulture)!;
			}

			return Convert.ChangeType(value, u, CultureInfo.InvariantCulture);
		}
	}

	static Exception MapError(Exception ex, int index, Binding?[] bindings)
	{
		if (index >= 0 && index < bindings.Length && bindings[index] is { } b)
			return new ValueMappingException($"Unable to set value of property '{b.Property}' ({Shape.TypeName(b.Type)}) from column '{b.Column}'.", b.Property, b.Column, b.Type, ex);

		return ex is InvalidOperationException ? ex : new InvalidOperationException($"Unable to create '{typeof(T)}' from the current row.", ex);
	}

	#endregion
}

// The typed getter of each type a column can be read as without boxing. It does not depend on the model type, so it lives here, not in RowMap<T>,
// where a static field is created once per model type.
static class TypedGetters
{
	internal static readonly FrozenDictionary<Type, string> ByType = new Dictionary<Type, string>()
	{
		[typeof(bool)] = nameof(IDataRecord.GetBoolean),
		[typeof(byte)] = nameof(IDataRecord.GetByte),
		[typeof(char)] = nameof(IDataRecord.GetChar),
		[typeof(short)] = nameof(IDataRecord.GetInt16),
		[typeof(int)] = nameof(IDataRecord.GetInt32),
		[typeof(long)] = nameof(IDataRecord.GetInt64),
		[typeof(float)] = nameof(IDataRecord.GetFloat),
		[typeof(double)] = nameof(IDataRecord.GetDouble),
		[typeof(decimal)] = nameof(IDataRecord.GetDecimal),
		[typeof(DateTime)] = nameof(IDataRecord.GetDateTime),
		[typeof(Guid)] = nameof(IDataRecord.GetGuid),
		[typeof(string)] = nameof(IDataRecord.GetString),
	}.ToFrozenDictionary();
}

// What one column can feed: today, a settable public instance property of the model, held once per model type in an ImmutableArray<Slot> in place of PropertyInfo,
// so the matcher and the compiled readers see only a name, the type a value must become and the method that stores it. It does not depend on the model type either.
sealed class Slot(string name, Type type, MethodInfo setter)
{
	public readonly string Name = name;
	public readonly Type Type = type;
	public readonly MethodInfo Setter = setter;
}

// Whether a model type can be mapped at all, decided once per model type from its shape, so that a type that cannot be mapped fails at the call, not at the first row.
// Reflection runs once and never throws: the reason is held as the message of the InvalidOperationException that RowMap<T> raises where every entry point passes
// (its constructor and RowMap<T>.Default). It does not depend on the model type, so it is not generic.
// Only the shape is judged so far. A type that needs constructor binding or `required` enforcement is rejected as not mappable yet, never mapped half-way.
sealed class Shape
{
	const string RequiredMemberAttributeName = "System.Runtime.CompilerServices.RequiredMemberAttribute";

	Shape(string? error) => Error = error;

	// Why the type cannot be mapped (the message), or null when it can.
	public readonly string? Error;

	public static Shape Of(Type type) => new(Judge(type));

	public void ThrowIfUnmappable()
	{
		if (Error is not null) throw new InvalidOperationException(Error);
	}

	// The type as the messages name it: the underlying name and a ? for a Nullable<T> (Int32?), else the short name (Int32, String).
	public static string TypeName(Type type) => Nullable.GetUnderlyingType(type) is { } underlying ? underlying.Name + "?" : type.Name;

	static string? Judge(Type type)
	{
		string name = TypeName(type);

		if (IsScalar(type)) return $"'{name}' is a single value, not a row model. Use ExecuteScalar<T>() or FirstOrdinalResults<T>().";
		if (NotConcrete(type) is { } kind) return $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
		if (HasRequiredMembers(type)) return $"'{name}' cannot be mapped yet: it has required members (required-member support arrives in a later phase).";
		if (!IsConstructible(type)) return $"'{name}' cannot be mapped yet: it has no public parameterless constructor (constructor binding arrives in a later phase).";

		return null;
	}

	// A single value, not a row: a column of it, not a model with properties. Nullable<T> of these is one too.
	static bool IsScalar(Type type)
	{
		type = Nullable.GetUnderlyingType(type) ?? type;
		return type.IsPrimitive || type.IsEnum
			|| type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset)
			|| type == typeof(TimeSpan) || type == typeof(Guid) || type == typeof(byte[]);
	}

	// What it is when it cannot be created at all, else null.
	static string? NotConcrete(Type type)
		=> type.IsInterface ? "an interface"
		: type.ContainsGenericParameters ? "an open generic type"
		: typeof(Delegate).IsAssignableFrom(type) ? "a delegate"
		: type.IsAbstract ? "an abstract class"
		: null;

	// A class needs a public parameterless constructor. A struct always has one, which reflection does not list, unless it declares public constructors of its own and none of them is parameterless.
	static bool IsConstructible(Type type)
		=> type.GetConstructor(Type.EmptyTypes) is not null || (type.IsValueType && type.GetConstructors().Length == 0);

	// The compiler marks each type that declares a required member, so a base type's marker finds the members a derived type inherits. Matched by name: a consumer's own polyfill counts.
	static bool HasRequiredMembers(Type type)
	{
		for (Type? t = type; t is not null && t != typeof(object); t = t.BaseType)
		{
			foreach (CustomAttributeData attribute in CustomAttributeData.GetCustomAttributes(t))
				if (attribute.AttributeType.FullName == RequiredMemberAttributeName) return true;
		}

		return false;
	}
}

/// <summary>The builder behind the collection syntax of <see cref="RowMap{T}"/>: <c>RowMap&lt;Post&gt; map = [new(nameof(Post.Id), "post_id")];</c>.</summary>
public static class RowMap
{
	/// <summary>Creates and validates a <see cref="RowMap{T}"/> from (property name, column name) pairs, exactly as the constructor does. This is what a collection expression of pairs calls; an empty one maps by name.</summary>
	/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
	/// <param name="mappings">The (property name, column name) pairs; a <see langword="null"/> column means "do not map this property".</param>
	/// <returns>The new map.</returns>
	/// <exception cref="ArgumentException">A pair is invalid (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	public static RowMap<T> Create<T>(ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> new(mappings);
}
