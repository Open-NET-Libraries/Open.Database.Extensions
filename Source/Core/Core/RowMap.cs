using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// A validated, read-only mapping of the properties and constructor parameters of <typeparamref name="T"/> to the columns that feed them, for when the columns are named differently from the members.
/// Declare it once and pass it as the <c>map</c> argument of the query methods (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …).
/// </summary>
/// <remarks>
/// Properties the map does not mention still map by name: a column feeds the property of that name exactly, else ignoring case, else ignoring case and underscores (<c>q_type</c> feeds <c>QType</c>).
/// A property with a pair is only matched by its column, exactly or ignoring case; a <see langword="null"/> column means "do not map this property".
/// A type with no public parameterless constructor is built through its one public constructor (a positional record, a record struct, a class with one constructor), and each parameter is fed like a property.
/// A parameter and a public property whose names differ only in case are one member, fed once, through the constructor, and a pair names it by the property's name or by the parameter's, ignoring case.
/// A parameter no column feeds, or a <see langword="null"/> column ignores, receives its declared default value, else <see langword="default"/>; so does a parameter the type declares non-nullable when the cell is NULL.
/// A <c>required</c> property must be fed by a column in every result shape, and a constructor parameter's default never feeds it: when none does, building the row reader throws <see cref="ValueMappingException"/> (<see cref="ValueMappingException.ColumnName"/> is the column looked for),
/// which happens when the first row of that shape is mapped, so a query with no rows never reports it (the buffered routes and the Channel and Dataflow methods know the columns before any row, and report it as soon as they have them). A constructor marked <c>SetsRequiredMembers</c> promises to set them: nothing is enforced then, and they map like any other property.
/// The pairs are validated when the map is constructed: an unknown or unsettable property or constructor parameter, a member named twice, an empty column name, a <see langword="null"/> column for a required property,
/// or two members reading one column throw <see cref="ArgumentException"/>.
/// A type that cannot be mapped throws <see cref="InvalidOperationException"/> when the map is constructed or <see cref="Default"/> is read, which every query method does at the call, even when the result is empty:
/// an interface, an abstract class, a delegate, a single value such as <see cref="int"/> or <see cref="string"/>, a type with two or more public constructors and none without parameters, a class with no public constructor and a type with a <c>required</c> public field (a field is never mapped, so it cannot be set).
/// A NULL never overwrites a member the C# type declares non-nullable: a value type other than <see cref="Nullable{T}"/> (an enum counts), or a reference type with nullable annotations on (<c>string</c>, not <c>string?</c>).
/// A property of such a type keeps the value its constructor or initializer gave it, else its default; a constructor parameter receives its declared default value, else <see langword="default"/>.
/// Anything else receives <see langword="null"/>: <see cref="Nullable{T}"/>, a reference type declared nullable (<c>string?</c>, or <c>[AllowNull]</c> on a property), a reference type in code with nullable annotations off, and a member declared as a type parameter.
/// The annotations are read from the assembly the model is declared in, once per model type.
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
/// <typeparam name="T">The model type whose properties and constructor parameters are mapped.</typeparam>
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
	/// <exception cref="ArgumentException">A pair is invalid: an unknown or unsettable property or constructor parameter, a member named twice, an empty column name, or two members reading one column.</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	public RowMap(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
	{
		// A model type that cannot be mapped fails here, at the call, not when the first row is read.
		TypeShape.ThrowIfUnmappable();

		// slot → column, in slot order; a slot reads the column of its own name unless a pair says otherwise.
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

	// A map enumerates the pairs it was declared with, which also makes it a collection-expression target: each slot a pair named, in slot order,
	// with the column the pair gave it (null: an ignored slot). A slot no pair named is not yielded, so new RowMap<T>(map) builds an equivalent map.
	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
	{
		for (int i = 0; i < _columns.Length; i++)
			if (_named[i]) yield return new(Slots[i].Name, _columns[i]);
	}

	IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<string, string?>>)this).GetEnumerator();

	// The validated column table and the compiled, cached, typed row readers (one per result-set layout: column names + column types),
	// so after the first query of a given shape no reflection, expression building or compilation happens again.
	// Values are read with the provider's typed getters whenever the column type matches the property type (or converts numerically), so nothing is boxed.
	// The member's declared type decides what a NULL does: Nullable<T>, a reference type declared nullable and a reference type in a context without nullable annotations receive null;
	// a member the C# type declares non-nullable (a value type, an enum included, or a reference type with nullable annotations on) is skipped,
	// so a property keeps what its constructor or initializer gave it (else its default) and a constructor parameter gets its fallback: one IsDBNull per such cell on the typed path,
	// a single read of the cell on the general path. Which reference types are declared non-nullable is read once per model type, when the slots are built (NullAnnotations).

	// The per-layout reader cache is bounded so dynamically generated column lists cannot grow memory forever.
	// Real applications use a handful of layouts; when the bound is hit the cache is simply cleared and refills.
	const int MaxLayouts = 128;

	// The per-model-type statics below (TypeShape, Slots, SlotIndex, Default) are one immutable copy per T, built once. Any cache of compiled readers keyed by model type
	// lives as long and costs a lookup per call; a static on the generic type is the cheapest place for it. Nothing that does not depend on T belongs here.

	// Whether T can be mapped at all, judged from its shape; never throws (an unmappable T holds its error), so no static initializer here can fail with it.
	static readonly Shape TypeShape = Shape.Of(typeof(T));

	// What a column can feed: one slot per parameter of the constructor the model is built through (none for a model built as it always was), then one per settable public
	// instance property no parameter covers, in declaration order (a property hidden by name keeps the first). Reflection runs once per T.
	static readonly ImmutableArray<Slot> Slots = BuildSlots();

	static ImmutableArray<Slot> BuildSlots()
	{
		if (TypeShape.Error is not null) return [];

		var seen = new HashSet<string>(StringComparer.Ordinal);
		var list = new List<Slot>();
		if (TypeShape.Constructor is { } constructor) AddParameterSlots(constructor, list, seen);

		foreach (PropertyInfo p in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			if (p.GetIndexParameters().Length == 0 && p.GetSetMethod() is { } setter && seen.Add(p.Name))
				list.Add(new Slot(p.Name, p.PropertyType, setter, MustHaveColumn(p), NullAnnotations.IsNonNullableReference(p)));
		}

		return [.. list];
	}

	// A parameter and a public property whose names are equal ignoring case are one slot: named after the property, typed by the parameter, fed through the constructor and never set again
	// (a record's positional init property). The exact name wins over the one ignoring case, each property serves one parameter, and a parameter with no property is a slot named after itself.
	static void AddParameterSlots(ConstructorInfo constructor, List<Slot> list, HashSet<string> seen)
	{
		ParameterInfo[] parameters = constructor.GetParameters();
		PropertyInfo[] properties = [.. typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0)];
		var matches = new PropertyInfo?[parameters.Length];
		bool[] taken = new bool[properties.Length];

		foreach (StringComparison comparison in (StringComparison[])[StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase])
		{
			for (int i = 0; i < parameters.Length; i++)
			{
				if (matches[i] is not null) continue;

				for (int p = 0; p < properties.Length; p++)
				{
					if (taken[p] || !string.Equals(properties[p].Name, parameters[i].Name, comparison)) continue;

					matches[i] = properties[p];
					taken[p] = true;
					break;
				}
			}
		}

		for (int i = 0; i < parameters.Length; i++)
		{
			ParameterInfo parameter = parameters[i];
			string name = matches[i]?.Name ?? parameter.Name ?? "arg" + i;
			bool added = seen.Add(name);
			Debug.Assert(added, "A slot name is unique: a parameter takes the name of a property or its own, and the exact name was matched first.");
			list.Add(new Slot(name, parameter.ParameterType, i, parameter.Name ?? name, matches[i] is not null, Slot.FallbackOf(parameter), matches[i] is { } property && MustHaveColumn(property), NullAnnotations.IsNonNullableReference(parameter)));
		}
	}

	// A required property must be fed by a column in every result shape, and a constructor parameter's default does not feed it: the property of a parameter is required all the same.
	// A constructor that sets the required members itself ([SetsRequiredMembers]) lifts that: they are ordinary properties then.
	static bool MustHaveColumn(PropertyInfo property) => !TypeShape.SetsRequiredMembers && Shape.IsRequired(property);

	// NOTE: static initializers run in textual order; each must follow what it uses.
	static readonly FrozenDictionary<string, int> SlotIndex = BuildSlotIndex().ToFrozenDictionary();

	static Dictionary<string, int> BuildSlotIndex()
	{
		var index = new Dictionary<string, int>(Slots.Length, StringComparer.Ordinal);
		for (int i = 0; i < Slots.Length; i++) index.Add(Slots[i].Name, i);
		return index;
	}

	/// <summary>The map for the model's default mapping (property name == column name), used when no map is passed.</summary>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped: it is an interface, an abstract class, a delegate or a single value such as <see cref="int"/> or <see cref="string"/>, a type with two or more public constructors and none without parameters, a class with no public constructor or a type with a <c>required</c> public field (see <see cref="RowMap{T}"/>).</exception>
	public static RowMap<T> Default
	{
		// Null when T cannot be mapped, so reading Default throws instead of a static initializer failing.
		get => field ?? throw new InvalidOperationException(TypeShape.Error);
	} = TypeShape.Error is null ? new() : null; // Declared after TypeShape, Slots and SlotIndex: its constructor reads them.

	// For an entry point that runs a command before it maps a row: checking the type first makes an unmappable T fail at the call, with nothing executed.
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter", Justification = "It checks this T's shape: the type parameter is how a caller names which T to check (RowMap<T>.ThrowIfUnmappable()).")]
	internal static void ThrowIfUnmappable() => TypeShape.ThrowIfUnmappable();

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

	// Applies the pairs and reports which slots they named.
	static bool[] Declare(ReadOnlySpan<KeyValuePair<string, string?>> mappings, string?[] columns, string paramName)
	{
		bool[] named = new bool[columns.Length];
		foreach (KeyValuePair<string, string?> pair in mappings)
		{
			if (pair.Key is null) throw new ArgumentException("A mapping has no property name.", paramName);

			bool known = TryFindSlot(pair.Key, out int index);
			if (known)
			{
				if (named[index]) throw new ArgumentException($"The {Describe(Slots[index])} is mapped more than once.", paramName);
				named[index] = true;
			}

			if (pair.Value is { Length: 0 }) throw new ArgumentException($"The property '{pair.Key}' is mapped to an empty column name. Use null to ignore it.", paramName);
			if (!known) throw new ArgumentException($"'{typeof(T)}' has no settable public property{(TypeShape.Constructor is null ? null : " or constructor parameter")} named '{pair.Key}'.", paramName);

			if (pair.Value is null && Slots[index].Required) throw new ArgumentException($"The property '{Slots[index].Name}' is required, so it cannot be mapped to null. Map it to a column.", paramName);

			columns[index] = pair.Value; // A null column means 'do not map this slot'.
		}

		return named;
	}

	// A pair's name is matched to a settable property exactly; failing that, to a constructor parameter ignoring case (nameof reaches the property of the same name, not the parameter).
	static bool TryFindSlot(string name, out int index)
	{
		if (SlotIndex.TryGetValue(name, out index)) return true;

		for (int i = 0; i < Slots.Length; i++)
		{
			if (Slots[i].Parameter is not { } parameter || !string.Equals(parameter, name, StringComparison.OrdinalIgnoreCase)) continue;

			index = i;
			return true;
		}

		return false;
	}

	static string Describe(Slot slot) => (slot.IsProperty ? "property '" : "constructor parameter '") + slot.Name + "'";

	// Each mapped slot is compared with the slots before it (the first reader of a column is the one that gets it).
	// Only what the map names is held to account: the model's own by-name twins are Map<T>()'s business.
	// The check is quadratic in the slot count and runs once per map.
	static void RejectSharedColumns(string?[] columns, bool[] named, string paramName)
	{
		for (int i = 1; i < columns.Length; i++)
		{
			if (columns[i] is not { } column) continue;

			for (int j = 0; j < i; j++)
			{
				if (!string.Equals(columns[j], column, StringComparison.OrdinalIgnoreCase)) continue;

				if (named[i] || named[j])
				{
					string who = Slots[j].IsProperty && Slots[i].IsProperty
						? $"properties '{Slots[j].Name}' and '{Slots[i].Name}'"
						: $"{Describe(Slots[j])} and the {Describe(Slots[i])}";

					throw new ArgumentException($"The {who} both read column '{column}'. Map one of them to another column, or to null to ignore it.", paramName);
				}

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
		=> Volatile.Read(ref field) ?? Interlocked.CompareExchange(ref field, new(), null) ?? field;

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

		// A property the type declares non-nullable (a value type, or a reference with nullable annotations on) is skipped when its cell is NULL; a constructor parameter of one gets its fallback.
		return Compile<object?[]>(row, layout.Names, (i, slot)
			=> Expression.Call(convert.MakeGenericMethod(slot.Type), Expression.ArrayIndex(row, Expression.Constant(i))),
			(i, slot) => slot.KeepsValueOnNull ? Expression.Call(isNullCell, Expression.ArrayIndex(row, Expression.Constant(i))) : null);
	}

	#endregion

	#region Compilation

	// What a failure reading a column names: the slot (its property, or its constructor parameter when no property carries its name), the column and the type the value had to become.
	// `Parameter` is the name of the constructor parameter the column is passed to, null for a property that is set.
	sealed class Binding(string property, string column, Type type, string? parameter)
	{
		public readonly string Property = property;
		public readonly string Column = column;
		public readonly Type Type = type;
		public readonly string? Parameter = parameter;
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
			=> ReadValue(r, Expression.Constant(i), layout.Types[i], slot, readerType, isDbNull, getValue, cell),
			(i, slot) => NullTest(r, Expression.Constant(i), layout.Types[i], slot, readerType, isDbNull, getValue, cell),
			cell);
	}

	// The skeleton shared by both mappers. A model built as it always was (a class with a public parameterless constructor, a struct with no constructor of its own):
	// `new T()`, then for each column that maps to a property, `item.Property = <cell>`. A model built through a constructor: every fed cell is read, in column order, into a typed local,
	// then `new T(arguments)`, then the properties no parameter covers are set. Only how a cell is read differs (a live reader vs buffered object[] rows).
	// `isNull` gives, for a property that must keep its value on NULL, the test that the cell is NULL, which wraps the assignment as
	// `if (!isNull) item.Property = <cell>`; it may leave the raw cell in `cell`, which readCell then converts.
	Func<TArg, T> Compile<TArg>(ParameterExpression arg, string[] names, Func<int, Slot, Expression> readCell,
		Func<int, Slot, Expression?> isNull, ParameterExpression? cell = null)
	{
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[names.Length];
		var locals = new List<ParameterExpression>();
		Slot?[] feeds = Match(names);
		RequireColumns(feeds);

		List<Expression> statements = TypeShape.Constructor is { } constructor
			? BindConstructor(constructor, item, index, bindings, names, feeds, readCell, isNull, locals)
			: CreateThenSet(item, index, bindings, names, feeds, readCell, isNull);

		return Expression.Lambda<Func<TArg, T>>(Guard(statements, item, index, cell, locals, bindings), arg).Compile();
	}

	// Every required property needs a column in this result shape, whatever else the constructor could pass (the default of a parameter never satisfies it).
	// It is raised when the plan is built: when the first row of a shape is mapped for a reader (so a query with no rows never reports it), and as soon as the columns are known for the buffered, Channel and Dataflow routes. The column named is the one looked for: a pair's, else the property's own name.
	void RequireColumns(Slot?[] feeds)
	{
		for (int s = 0; s < Slots.Length; s++)
		{
			Slot slot = Slots[s];
			if (!slot.Required || Array.IndexOf(feeds, slot) >= 0) continue;

			string column = _columns[s]!; // A required slot is never mapped to null: the pairs are validated.
			throw new ValueMappingException(
				$"No column feeds the required property '{slot.Name}' ({Shape.TypeName(slot.Type)}) of '{Shape.TypeName(typeof(T))}'; expected a column named '{column}'.",
				slot.Name, column, slot.Type);
		}
	}

	// Today's path, unchanged: `new T()`, then each cell is read and assigned in column order, with no locals.
	static List<Expression> CreateThenSet(ParameterExpression item, ParameterExpression index, Binding?[] bindings, string[] names, Slot?[] feeds,
		Func<int, Slot, Expression> readCell, Func<int, Slot, Expression?> isNull)
	{
		var statements = new List<Expression>(names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			bindings[i] = new Binding(slot.Name, names[i], slot.Type, null);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));
			Expression assign = Expression.Call(item, slot.Setter!, readCell(i, slot));
			statements.Add(isNull(i, slot) is { } skip ? Expression.IfThen(Expression.Not(skip), assign) : assign);
		}

		statements.Add(item);
		return statements;
	}

	// The constructor path. A constructor needs its arguments before anything can be set, and a SequentialAccess reader allows each cell to be read once, in ascending order:
	// so every fed cell is read, in column order, into a typed local (nothing is boxed on the typed path), the constructor is called with the locals (the fallback for a parameter no column
	// feeds, and for a NULL into a member the type declares non-nullable), and then the properties the constructor does not cover are set from their locals, a NULL skipping one the type declares non-nullable.
	static List<Expression> BindConstructor(ConstructorInfo constructor, ParameterExpression item, ParameterExpression index, Binding?[] bindings, string[] names, Slot?[] feeds,
		Func<int, Slot, Expression> readCell, Func<int, Slot, Expression?> isNull, List<ParameterExpression> locals)
	{
		var arguments = new Expression[constructor.GetParameters().Length];
		var statements = new List<Expression>(names.Length * 2 + 4);
		var sets = new List<Expression>();

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			bindings[i] = new Binding(slot.Name, names[i], slot.Type, slot.Parameter);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));

			ParameterExpression local = Expression.Variable(slot.Type, "value" + i);
			locals.Add(local);
			Expression? skip = isNull(i, slot);
			Expression value = readCell(i, slot);

			if (slot.Setter is null)
			{
				// A NULL into a parameter the type declares non-nullable (a value type, or a reference with nullable annotations on) is not a value: the parameter gets its fallback. Any other NULL is null, as it is for a property.
				Debug.Assert(slot.Fallback is not null && arguments[slot.Position] is null);
				statements.Add(Expression.Assign(local, skip is null ? value : Expression.Condition(skip, slot.Fallback!, value)));
				arguments[slot.Position] = local;
			}
			else if (skip is null)
			{
				statements.Add(Expression.Assign(local, value));
				sets.Add(Expression.Assign(index, Expression.Constant(i)));
				sets.Add(Expression.Call(item, slot.Setter, local));
			}
			else
			{
				ParameterExpression isNullCell = Expression.Variable(typeof(bool), "null" + i);
				locals.Add(isNullCell);
				statements.Add(Expression.Assign(isNullCell, skip));
				statements.Add(Expression.Assign(local, Expression.Condition(isNullCell, Expression.Default(slot.Type), value)));
				sets.Add(Expression.Assign(index, Expression.Constant(i)));
				sets.Add(Expression.IfThen(Expression.Not(isNullCell), Expression.Call(item, slot.Setter, local)));
			}
		}

		foreach (Slot slot in Slots)
		{
			if (slot.Setter is null && arguments[slot.Position] is null) arguments[slot.Position] = slot.Fallback!; // No column feeds it.
		}

		// A constructor that throws is not the fault of the last column read: the index says no column.
		statements.Add(Expression.Assign(index, Expression.Constant(-1)));
		statements.Add(Expression.Assign(item, Expression.New(constructor, arguments)));
		statements.AddRange(sets);
		statements.Add(item);
		return statements;
	}

	// `index` tracks the column being read so a failure can name the offending property and column.
	static BlockExpression Guard(List<Expression> statements, ParameterExpression item, ParameterExpression index, ParameterExpression? cell, List<ParameterExpression> locals, Binding?[] bindings)
	{
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		var variables = new List<ParameterExpression> { item, index };
		if (cell is not null) variables.Add(cell);
		variables.AddRange(locals);
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

	// The expression that is true when the cell of a slot that keeps its value on NULL (Slot.KeepsValueOnNull) is NULL, or null for any other slot.
	// On the typed path the reader answers; on the general path the cell is read once into `cell` and ReadValue converts that same value.
	static MethodCallExpression? NullTest(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue, ParameterExpression cell)
	{
		if (!slot.KeepsValueOnNull) return null;

		if (TypedRead(r, ordinal, columnType, slot.Type, readerType) is not null)
			return Expression.Call(r, isDbNull, ordinal);

		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!,
			Expression.Assign(cell, Expression.Call(r, getValue, ordinal)));
	}

	static Expression ReadValue(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue,
		ParameterExpression? cell = null)
	{
		Type propertyType = slot.Type;
		Type? underlying = Nullable.GetUnderlyingType(propertyType);
		Type target = underlying ?? propertyType;
		bool nullable = !slot.KeepsValueOnNull; // Nullable<T>, or a reference the C# type does not declare non-nullable: a NULL is null.

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
		// NullTest has already read the cell of a member that keeps its value on NULL into `cell` to test it: the cell is read once, so it is converted from there.
		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(propertyType),
			cell is not null && slot.KeepsValueOnNull ? cell : Expression.Call(r, getValue, ordinal));
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
		// A NULL never reaches here for a member the type declares non-nullable: the compiled readers skip that assignment.
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
		{
			string message = b.Parameter is { } parameter
				? $"Unable to pass value of column '{b.Column}' to constructor parameter '{parameter}' ({Shape.TypeName(b.Type)}) of '{Shape.TypeName(typeof(T))}'."
				: $"Unable to set value of property '{b.Property}' ({Shape.TypeName(b.Type)}) from column '{b.Column}'.";

			return new ValueMappingException(message, b.Property, b.Column, b.Type, ex);
		}

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

// What one column can feed: a constructor parameter, or a settable public instance property of the model, held once per model type in an ImmutableArray<Slot> in place of PropertyInfo,
// so the matcher and the compiled readers see only a name, the type a value must become and where it goes. It does not depend on the model type either.
// A slot fed through the constructor has a Position and no Setter; a parameter and a property of the same name (ignoring case) are one such slot, so the property is never set again.
sealed class Slot
{
	// A settable property.
	public Slot(string name, Type type, MethodInfo setter, bool required, bool nonNullableReference)
	{
		Name = name;
		Required = required;
		Type = type;
		Setter = setter;
		Position = -1;
		IsProperty = true;
		KeepsValueOnNull = KeepsOnNull(type, nonNullableReference);
	}

	// A constructor parameter, named after the property of that name when there is one.
	public Slot(string name, Type type, int position, string parameter, bool isProperty, Expression fallback, bool required, bool nonNullableReference)
	{
		Name = name;
		Required = required;
		Type = type;
		Position = position;
		Parameter = parameter;
		IsProperty = isProperty;
		Fallback = fallback;
		KeepsValueOnNull = KeepsOnNull(type, nonNullableReference);
	}

	// A NULL is not a value for a member the C# type declares non-nullable: a value type other than Nullable<T> (an enum counts), or a reference type declared non-nullable (NullAnnotations).
	// A property then keeps what the constructor or its initializer gave it, and a constructor parameter gets its fallback. Decided once per model type, here, never per row.
	static bool KeepsOnNull(Type type, bool nonNullableReference)
		=> type.IsValueType ? Nullable.GetUnderlyingType(type) is null : nonNullableReference;

	// Whether a NULL cell leaves the member alone (see KeepsOnNull) rather than assigning null; the compiled readers test the cell only for such a slot.
	public readonly bool KeepsValueOnNull;

	public readonly string Name;
	public readonly Type Type;

	// The method that stores the value; null for a slot fed through the constructor.
	public readonly MethodInfo? Setter;

	// The position of the constructor parameter, and its name; -1 and null for a property that is set.
	public readonly int Position;
	public readonly string? Parameter;

	// Whether a public property of this name exists (it may be get-only, as in a readonly struct), for the messages.
	public readonly bool IsProperty;

	// What the constructor receives when no column feeds the parameter: its declared default, else default(TParam).
	public readonly Expression? Fallback;

	// Whether a column must feed it in every result shape: a required property, unless the constructor sets the required members. A constructor parameter's default does not count as a feed.
	public readonly bool Required;

	// ParameterInfo reports a null default for `DateTime d = default` (and .NET Framework throws FormatException reading it), and the underlying integer for a nullable enum,
	// so a null stands for default(TParam), and an enum is converted back.
	public static Expression FallbackOf(ParameterInfo parameter)
	{
		Type type = parameter.ParameterType;
		object? value = null;
		try
		{
			if (parameter.HasDefaultValue) value = parameter.DefaultValue;
		}
		catch (FormatException)
		{
			// .NET Framework cannot read the default of a struct parameter set to `default`: that is default(TParam).
		}

		if (value is null or DBNull or Missing) return Expression.Default(type);

		Type underlying = Nullable.GetUnderlyingType(type) ?? type;
		if (underlying.IsEnum && value.GetType() != underlying) value = Enum.ToObject(underlying, value);
		return Expression.Constant(value, type);
	}
}

// Whether a property or a constructor parameter is a reference type the C# source declares non-nullable, which is when a NULL must not overwrite it (Slot.KeepsValueOnNull).
// The compiler records nullable annotations in metadata: NullableAttribute on a member (or a parameter) whose annotation differs from its context, else NullableContextAttribute on the nearest
// enclosing method or type that has one. Flag 1 is not-null, 2 is nullable, 0 or none is oblivious (nullable annotations off): only 1 counts.
// It reads those attributes as data, by full name, so it works on every target (NullabilityInfoContext does not exist on netstandard) and no attribute type is referenced.
// A property marked [AllowNull] (on the property or on its setter's value parameter) says null may be written to it, so it is nullable. A member whose declared type is a type parameter (T Value in Box<T>) is not judged: it is as oblivious as it was.
// This runs once per model type, when the slots are built, never per row.
static class NullAnnotations
{
	const string NullableAttributeName = "System.Runtime.CompilerServices.NullableAttribute";
	const string NullableContextAttributeName = "System.Runtime.CompilerServices.NullableContextAttribute";
	const string AllowNullAttributeName = "System.Diagnostics.CodeAnalysis.AllowNullAttribute";
	const BindingFlags AnyDeclared = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

	public static bool IsNonNullableReference(PropertyInfo property)
	{
		if (!IsReference(property.PropertyType) || DeclaredAsTypeParameter(property)) return false;

		IList<CustomAttributeData> own = CustomAttributeData.GetCustomAttributes(property);
		return !Has(own, AllowNullAttributeName) && !AllowsNullOnSetter(property) && (Flag(own, NullableAttributeName) ?? ContextFlag(property)) == 1;
	}

	// The compiler puts [AllowNull] written on a property on the value parameter of its setter (an auto-property has it nowhere else), which is also where the runtime reads it.
	static bool AllowsNullOnSetter(PropertyInfo property)
		=> property.SetMethod?.GetParameters() is { Length: > 0 } parameters && Has(CustomAttributeData.GetCustomAttributes(parameters[^1]), AllowNullAttributeName);

	public static bool IsNonNullableReference(ParameterInfo parameter)
	{
		if (!IsReference(parameter.ParameterType) || DeclaredAsTypeParameter(parameter)) return false;

		return (Flag(CustomAttributeData.GetCustomAttributes(parameter), NullableAttributeName) ?? ContextFlag(parameter.Member)) == 1;
	}

	static bool IsReference(Type type) => !type.IsValueType && !type.IsGenericParameter && !type.IsByRef && !type.IsPointer;

	// The members of a closed generic type report the closed type (Box<string>.Value is a string); the declaration says T. The declaration is found by its metadata token.
	static bool DeclaredAsTypeParameter(PropertyInfo property)
	{
		if (property.DeclaringType is not { IsGenericType: true, IsGenericTypeDefinition: false } closed) return false;

		foreach (PropertyInfo candidate in closed.GetGenericTypeDefinition().GetProperties(AnyDeclared))
			if (candidate.MetadataToken == property.MetadataToken) return candidate.PropertyType.IsGenericParameter;

		return false;
	}

	static bool DeclaredAsTypeParameter(ParameterInfo parameter)
	{
		if (parameter.Member is not { DeclaringType: { IsGenericType: true, IsGenericTypeDefinition: false } closed } constructor) return false;

		foreach (ConstructorInfo candidate in closed.GetGenericTypeDefinition().GetConstructors(AnyDeclared))
			if (candidate.MetadataToken == constructor.MetadataToken) return candidate.GetParameters()[parameter.Position].ParameterType.IsGenericParameter;

		return false;
	}

	// The nearest NullableContextAttribute from the member outward through its declaring types (a nested type's context applies to what it contains), or null when there is none: oblivious.
	static byte? ContextFlag(MemberInfo? member)
	{
		for (; member is not null; member = member.DeclaringType)
		{
			if (Flag(CustomAttributeData.GetCustomAttributes(member), NullableContextAttributeName) is { } flag) return flag;
		}

		return null;
	}

	// The flag of the attribute by that name: its one argument is a byte, or a byte[] with one flag per type in a generic or array type, whose first is the type itself.
	static byte? Flag(IList<CustomAttributeData> attributes, string attributeName)
	{
		foreach (CustomAttributeData attribute in attributes)
		{
			if (attribute.AttributeType.FullName != attributeName || attribute.ConstructorArguments.Count != 1) continue;

			switch (attribute.ConstructorArguments[0].Value)
			{
				case byte flag:
					return flag;
				case IList<CustomAttributeTypedArgument> { Count: > 0 } flags when flags[0].Value is byte first:
					return first;
			}
		}

		return null;
	}

	static bool Has(IList<CustomAttributeData> attributes, string attributeName)
	{
		foreach (CustomAttributeData attribute in attributes)
			if (attribute.AttributeType.FullName == attributeName) return true;

		return false;
	}
}

// Whether a model type can be mapped at all, and how it is built, decided once per model type from its shape, so that a type that cannot be mapped fails at the call, not at the first row.
// Reflection runs once and never throws: the reason is held as the message of the InvalidOperationException that RowMap<T> raises where every entry point passes
// (its constructor and RowMap<T>.Default). It does not depend on the model type, so it is not generic.
// The constructor is chosen from the type alone, never from the columns, so a type is built the same way whatever the query:
// a class with a public parameterless constructor, or a struct with no public constructor, is built as it always was (new T(), then its settable properties);
// any other type is built through its one public constructor. Two or more public constructors and none without parameters is an error, and so is a class with none.
// A `required` property is mapped, and must be fed by a column in every result shape (RowMap<T>.RequireColumns), unless the constructor the model is built through is marked [SetsRequiredMembers]:
// it promises to set them, so nothing is enforced. A `required` public field cannot be set (fields are not mapped), so it is a type-shape error here.
// Both attributes are matched by full name, so a consumer's own copy counts and the library names no type that netstandard lacks.
sealed class Shape
{
	const string RequiredMemberAttributeName = "System.Runtime.CompilerServices.RequiredMemberAttribute";
	const string SetsRequiredMembersAttributeName = "System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute";

	Shape(string? error, ConstructorInfo? constructor, bool setsRequiredMembers)
	{
		Error = error;
		Constructor = constructor;
		SetsRequiredMembers = setsRequiredMembers;
	}

	// Why the type cannot be mapped (the message), or null when it can.
	public readonly string? Error;

	// The constructor the model is built through, or null when it is built as it always was.
	public readonly ConstructorInfo? Constructor;

	// Whether the constructor the model is built through (the parameterless one too) is marked [SetsRequiredMembers]: the required members are then ordinary properties.
	public readonly bool SetsRequiredMembers;

	public static Shape Of(Type type)
	{
		string? error = Judge(type, out ConstructorInfo? constructor, out bool setsRequiredMembers);
		return new(error, constructor, setsRequiredMembers);
	}

	public void ThrowIfUnmappable()
	{
		if (Error is not null) throw new InvalidOperationException(Error);
	}

	// The type as the messages name it: the underlying name and a ? for a Nullable<T> (Int32?), else the short name (Int32, String).
	public static string TypeName(Type type) => Nullable.GetUnderlyingType(type) is { } underlying ? underlying.Name + "?" : type.Name;

	static string? Judge(Type type, out ConstructorInfo? constructor, out bool setsRequiredMembers)
	{
		constructor = null;
		setsRequiredMembers = false;
		string name = TypeName(type);

		if (IsScalar(type)) return $"'{name}' is a single value, not a row model. Use ExecuteScalar<T>() or FirstOrdinalResults<T>().";
		if (NotConcrete(type) is { } kind) return $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";

		return Choose(type, name, out constructor, out setsRequiredMembers) ?? (setsRequiredMembers ? null : RequiredField(type, name));
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

	// The constructor a model is built through: none (null) when a public parameterless constructor exists or when a struct has no public constructor (a struct always can be created,
	// as its default value, whatever non-public constructors it declares), else the one public constructor. GetConstructors lists the public instance constructors only, and never the
	// implicit parameterless one of a struct.
	static string? Choose(Type type, string name, out ConstructorInfo? constructor, out bool setsRequiredMembers)
	{
		constructor = null;
		setsRequiredMembers = false;
		ConstructorInfo[] constructors = type.GetConstructors();
		if (Array.Find(constructors, c => c.GetParameters().Length == 0) is { } parameterless)
		{
			setsRequiredMembers = Has(parameterless, SetsRequiredMembersAttributeName);
			return null;
		}

		if (type.IsValueType && constructors.Length == 0) return null;
		if (constructors.Length == 0) return $"'{name}' cannot be mapped: it has no public constructor.";

		if (constructors.Length > 1)
		{
			string[] signatures = [.. constructors.Select(c => Signature(name, c)).OrderBy(s => s, StringComparer.Ordinal)];
			return $"'{name}' cannot be mapped: it has {constructors.Length} public constructors, {string.Join(", ", signatures, 0, signatures.Length - 1)} and {signatures[^1]}, and none without parameters. Make one of them non-public, or use Map(selector).";
		}

		foreach (ParameterInfo parameter in constructors[0].GetParameters())
		{
			if (parameter.ParameterType.IsByRef || parameter.ParameterType.IsPointer)
				return $"'{name}' cannot be mapped: its constructor parameter '{parameter.Name}' is passed by reference or is a pointer. Use Map(selector).";
		}

		constructor = constructors[0];
		setsRequiredMembers = Has(constructor, SetsRequiredMembersAttributeName);
		return null;
	}

	static string Signature(string name, ConstructorInfo constructor)
		=> name + "(" + string.Join(", ", constructor.GetParameters().Select(p => TypeName(p.ParameterType) + " " + p.Name)) + ")";

	// A public instance field marked required (an inherited one too), which a map cannot set. The first one is named.
	static string? RequiredField(Type type, string name)
	{
		foreach (FieldInfo field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
		{
			if (IsRequired(field)) return $"'{name}' cannot be mapped: its required field '{field.Name}' cannot be set. Make it a property.";
		}

		return null;
	}

	public static bool IsRequired(MemberInfo member) => Has(member, RequiredMemberAttributeName);

	// Matched by the full name of the attribute: a consumer's own copy of it counts. Read as data, so no attribute is instantiated.
	static bool Has(MemberInfo member, string attributeName)
	{
		foreach (CustomAttributeData attribute in CustomAttributeData.GetCustomAttributes(member))
			if (attribute.AttributeType.FullName == attributeName) return true;

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
