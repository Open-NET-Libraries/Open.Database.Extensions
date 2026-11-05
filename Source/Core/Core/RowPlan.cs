using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Text;

namespace Open.Database.Extensions.Core;

/// <summary>
/// The compiled mapping from reader rows to <typeparamref name="T"/>.
/// One plan exists per (<typeparamref name="T"/>, field overrides) pair. Each plan caches one compiled,
/// typed deserializer per result-set layout (column names + column types), so after the first query of a
/// given shape no reflection, expression building or compilation happens again.
/// </summary>
/// <remarks>
/// Values are read with the provider's typed getters (<c>GetInt32</c>, <c>GetString</c>, …) whenever the column
/// type matches the property type (or converts numerically), so nothing is boxed.
/// <c>NULL</c> is honored for reference types and <see cref="Nullable{T}"/> properties; a <c>NULL</c> in a
/// column mapped to a non-nullable value-type property is the provider's error to raise.
/// </remarks>
internal sealed class RowPlan<T> : RowSource<T>
	where T : new()
{
	// Both caches are bounded so dynamically generated overrides or column lists cannot grow memory forever.
	// Real applications use a handful of each; when a bound is hit the cache is simply cleared and refills.
	const int MaxOverridePlans = 128;
	const int MaxLayouts = 128;

	static readonly ConcurrentDictionary<string, RowPlan<T>> OverridePlans = new(StringComparer.Ordinal);

	// Settable public instance properties by name: reflection runs once per T.
	static readonly Dictionary<string, PropertyInfo> PropertiesByName = BuildProperties();

	static Dictionary<string, PropertyInfo> BuildProperties()
	{
		var map = new Dictionary<string, PropertyInfo>(StringComparer.Ordinal);
		foreach (PropertyInfo p in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			if (p.GetIndexParameters().Length == 0 && p.GetSetMethod() is not null)
#if NETSTANDARD2_0
				if (!map.ContainsKey(p.Name)) map.Add(p.Name, p);
#else
				map.TryAdd(p.Name, p);
#endif
		}

		return map;
	}

	// NOTE: static initializers run in textual order; this must follow PropertiesByName.
	/// <summary>The plan for the model's default mapping (property name == column name).</summary>
	public static readonly RowPlan<T> Default = new(default, strict: false);

	/// <summary>Gets the plan for the given overrides (field name → column name; null column ignores the field).</summary>
	public static RowPlan<T> For(IEnumerable<KeyValuePair<string, string?>>? overrides)
	{
		if (overrides is RowMap<T> map) return map.Plan; // Predefined and already validated: no key, no lookup, no allocation.
		if (overrides is null) return Default;

		KeyValuePair<string, string?>[] items = overrides as KeyValuePair<string, string?>[] ?? overrides.ToArray();
		if (items.Length == 0) return Default;

		var sb = new StringBuilder();
		foreach (KeyValuePair<string, string?> kvp in items)
			sb.Append(kvp.Key).Append('=').Append(kvp.Value ?? "\0").Append('|');

		string key = sb.ToString();
		if (OverridePlans.TryGetValue(key, out RowPlan<T>? plan)) return plan;

		plan = new RowPlan<T>(items, strict: false);
		if (OverridePlans.Count >= MaxOverridePlans) OverridePlans.Clear();
		if (OverridePlans.TryAdd(key, plan)) return plan;
		return OverridePlans.TryGetValue(key, out RowPlan<T>? existing) ? existing : plan; // Lost a race (or the cache was just cleared): either plan is equivalent.
	}

	/// <summary>Builds the plan for a <see cref="RowMap{T}"/>: every field it names must exist and be settable, and no two properties may read one column.</summary>
	public static RowPlan<T> Validated(ReadOnlySpan<KeyValuePair<string, string?>> mappings) => new(mappings, strict: true);

	// Property name → column name for every mapped (not ignored) property.
	readonly Dictionary<string, string> _propertyMap;

	/// <summary>The names of the properties that are mapped.</summary>
	public IEnumerable<string> PropertyNames => _propertyMap.Keys;

	/// <summary>The names of the columns the mapped properties read.</summary>
	public IEnumerable<string> ColumnNames => _propertyMap.Values;

	// Column name (case-insensitive) → the property it feeds.
	readonly Dictionary<string, PropertyInfo> _columnToProperty;

	// Most recently used layout; the overwhelmingly common case is the same shape every time.
	Layout? _last;
	// Created with the first layout: a mapping that is never used holds no dictionary.
	ConcurrentDictionary<int, Layout[]>? _layouts;

	// A strict plan (a RowMap) is held to account for every field it names, and for every column it reads;
	// a lenient one (a per-call override) keeps the historical behavior: an unknown field with a null column is
	// harmless, a repeated field takes its last column, and when properties share a column the first one reads it.
	RowPlan(ReadOnlySpan<KeyValuePair<string, string?>> overrides, bool strict)
	{
		string paramName = strict ? "mappings" : nameof(overrides);

		// field name → column name; defaults to the same name.
		var fieldToColumn = new Dictionary<string, string?>(PropertiesByName.Count, StringComparer.Ordinal);
		foreach (string name in PropertiesByName.Keys) fieldToColumn[name] = name;

		HashSet<string>? named = strict ? new(StringComparer.Ordinal) : null;
		foreach (KeyValuePair<string, string?> kvp in overrides)
		{
			if (named is not null)
			{
				if (kvp.Key is null) throw new ArgumentException("A mapping has no property name.", paramName);
				if (!named.Add(kvp.Key)) throw new ArgumentException($"The property '{kvp.Key}' is mapped more than once.", paramName);
			}

			if (!PropertiesByName.ContainsKey(kvp.Key))
			{
				if (kvp.Value is null && !strict) continue; // Ignoring a field that does not exist is harmless.
				throw new ArgumentException($"'{typeof(T)}' has no settable public property named '{kvp.Key}'.", paramName);
			}

			fieldToColumn[kvp.Key] = kvp.Value; // A null column means 'ignore this field'.
		}

		_propertyMap = new Dictionary<string, string>(fieldToColumn.Count, StringComparer.Ordinal);
		foreach (KeyValuePair<string, string?> kvp in fieldToColumn)
			if (kvp.Value is not null) _propertyMap[kvp.Key] = kvp.Value;

		_columnToProperty = new Dictionary<string, PropertyInfo>(fieldToColumn.Count, StringComparer.OrdinalIgnoreCase);
		foreach (KeyValuePair<string, string?> kvp in fieldToColumn)
		{
			if (kvp.Value is null) continue;
			if (!_columnToProperty.TryGetValue(kvp.Value, out PropertyInfo? first))
				_columnToProperty.Add(kvp.Value, PropertiesByName[kvp.Key]);
			else if (strict)
				throw new ArgumentException($"The properties '{first.Name}' and '{kvp.Key}' both read column '{kvp.Value}'. Map one of them to another column, or to null to ignore it.", paramName);
		}
	}

	/// <inheritdoc />
	public override Func<DbDataReader, T> Mapper(DbDataReader reader)
	{
		Layout? l = _last;
		if (l?.Matches(reader) != true) l = Resolve(reader);
		return l.Db ??= Build<DbDataReader>(l);
	}

	/// <inheritdoc />
	public override Func<IDataRecord, T> Mapper(IDataRecord reader)
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
	public Func<object?[], T> ArrayMapper(ImmutableArray<string> names)
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
		MethodInfo convert = typeof(RowPlan<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!;

		return Compile<object?[]>(row, layout.Names, (i, property)
			=> Expression.Call(convert.MakeGenericMethod(property.PropertyType), Expression.ArrayIndex(row, Expression.Constant(i))));
	}

	#endregion

	#region Compilation

	static readonly Dictionary<Type, string> TypedGetters = new()
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
	};

	sealed class Binding(string property, string column)
	{
		public readonly string Property = property;
		public readonly string Column = column;
	}

	Func<TReader, T> Build<TReader>(Layout layout)
		where TReader : class, IDataRecord
	{
		Type readerType = typeof(TReader);
		MethodInfo isDbNull = readerType.GetMethod(nameof(IDataRecord.IsDBNull), [typeof(int)])!;
		MethodInfo getValue = readerType.GetMethod(nameof(IDataRecord.GetValue), [typeof(int)])!;

		ParameterExpression r = Expression.Parameter(readerType, "r");

		return Compile<TReader>(r, layout.Names, (i, property)
			=> ReadValue(r, Expression.Constant(i), layout.Types[i], property.PropertyType, readerType, isDbNull, getValue));
	}

	// The skeleton shared by both mappers: `new T()`, then for each column that maps to a property,
	// `item.Property = <cell>`. Only how a cell is read differs (a live reader vs buffered object[] rows).
	Func<TArg, T> Compile<TArg>(ParameterExpression arg, string[] names, Func<int, PropertyInfo, Expression> readCell)
	{
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[names.Length];
		var statements = new List<Expression>(names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		var assigned = new HashSet<PropertyInfo>();
		for (int i = 0; i < names.Length; i++)
		{
			if (!_columnToProperty.TryGetValue(names[i], out PropertyInfo? property)) continue; // Not mapped: ignore the column.
			if (!assigned.Add(property)) continue; // The first matching column wins.

			bindings[i] = new Binding(property.Name, names[i]);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));
			statements.Add(Expression.Call(item, property.GetSetMethod()!, readCell(i, property)));
		}

		statements.Add(item);
		return Expression.Lambda<Func<TArg, T>>(Guard(statements, item, index, bindings), arg).Compile();
	}

	// `index` tracks the column being read so a failure can name the offending property and column.
	static BlockExpression Guard(List<Expression> statements, ParameterExpression item, ParameterExpression index, Binding?[] bindings)
	{
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		return Expression.Block(
			typeof(T),
			[item, index],
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
								typeof(RowPlan<T>).GetMethod(nameof(MapError), BindingFlags.Static | BindingFlags.NonPublic)!,
								ex, index, Expression.Constant(bindings)),
							typeof(T))))));
	}

	static Expression ReadValue(
		ParameterExpression r, Expression ordinal,
		Type columnType, Type propertyType,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue)
	{
		Type? underlying = Nullable.GetUnderlyingType(propertyType);
		Type target = underlying ?? propertyType;
		bool nullable = underlying is not null || !propertyType.IsValueType;

		if (TypedGetters.TryGetValue(columnType, out string? getterName))
		{
			MethodInfo getter = readerType.GetMethod(getterName, [typeof(int)])!;
			Expression? typed = TryConvert(Expression.Call(r, getter, ordinal), target);
			if (typed is not null)
			{
				if (!nullable) return typed;

				// NULL → null/default; otherwise the typed value (lifted to Nullable<> when needed).
				Expression present = underlying is null ? typed : Expression.Convert(typed, propertyType);
				return Expression.Condition(
					Expression.Call(r, isDbNull, ordinal),
					Expression.Default(propertyType),
					present);
			}
		}

		// General path: the boxed value, converted. Handles enums, strings → Guid, mismatched types, unknown column types.
		return Expression.Call(
			typeof(RowPlan<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(propertyType),
			Expression.Call(r, getValue, ordinal));
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

	static TTarget ConvertValue<TTarget>(object? value)
	{
		if (value is null or DBNull)
		{
			if (default(TTarget) is null) return default!;
			throw new InvalidCastException($"A NULL value cannot be assigned to a non-nullable '{typeof(TTarget)}'. Map the column to a nullable property or COALESCE it.");
		}

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
			return new InvalidOperationException($"Unable to set value of property '{b.Property}' from column '{b.Column}'.", ex);

		return ex is InvalidOperationException ? ex : new InvalidOperationException($"Unable to create '{typeof(T)}' from the current row.", ex);
	}

	#endregion
}
