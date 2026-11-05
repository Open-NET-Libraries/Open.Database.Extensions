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
	const int MaxCustomPlans = 128;
	const int MaxLayouts = 128;

	static readonly ConcurrentDictionary<string, RowPlan<T>> Custom = new(StringComparer.Ordinal);

	// Settable public instance properties by name: reflection runs once per T.
	static readonly Dictionary<string, PropertyInfo> PropertiesByName = BuildProperties();

	static Dictionary<string, PropertyInfo> BuildProperties()
	{
		var map = new Dictionary<string, PropertyInfo>(StringComparer.Ordinal);
		foreach (PropertyInfo p in typeof(T).GetProperties())
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
	public static readonly RowPlan<T> Default = new(null);

	/// <summary>Gets the plan for the given overrides (field name → column name; null column ignores the field).</summary>
	public static RowPlan<T> For(IEnumerable<KeyValuePair<string, string?>>? overrides)
	{
		if (overrides is null) return Default;

		KeyValuePair<string, string?>[] items = overrides as KeyValuePair<string, string?>[] ?? overrides.ToArray();
		if (items.Length == 0) return Default;

		var sb = new StringBuilder();
		foreach (KeyValuePair<string, string?> kvp in items)
			sb.Append(kvp.Key).Append('=').Append(kvp.Value ?? "\0").Append('|');

		string key = sb.ToString();
		if (Custom.TryGetValue(key, out RowPlan<T>? plan)) return plan;

		plan = new RowPlan<T>(items);
		if (Custom.Count >= MaxCustomPlans) Custom.Clear();
		return Custom.TryAdd(key, plan) ? plan : Custom[key];
	}

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
	readonly ConcurrentDictionary<int, Layout[]> _layouts = new();

	RowPlan(KeyValuePair<string, string?>[]? overrides)
	{
		// field name → column name; defaults to the same name.
		var fieldToColumn = new Dictionary<string, string?>(PropertiesByName.Count, StringComparer.Ordinal);
		foreach (string name in PropertiesByName.Keys) fieldToColumn[name] = name;

		if (overrides is not null)
		{
			foreach (KeyValuePair<string, string?> kvp in overrides)
			{
				if (!PropertiesByName.ContainsKey(kvp.Key))
					throw new ArgumentException($"'{typeof(T)}' has no settable public property named '{kvp.Key}'.", nameof(overrides));
				fieldToColumn[kvp.Key] = kvp.Value; // A null column means 'ignore this field'.
			}
		}

		_propertyMap = new Dictionary<string, string>(fieldToColumn.Count, StringComparer.Ordinal);
		foreach (KeyValuePair<string, string?> kvp in fieldToColumn)
			if (kvp.Value is not null) _propertyMap[kvp.Key] = kvp.Value;

		_columnToProperty = new Dictionary<string, PropertyInfo>(fieldToColumn.Count, StringComparer.OrdinalIgnoreCase);
		foreach (KeyValuePair<string, string?> kvp in fieldToColumn)
		{
			if (kvp.Value is not null)
#if NETSTANDARD2_0
				if (!_columnToProperty.ContainsKey(kvp.Value)) _columnToProperty.Add(kvp.Value, PropertiesByName[kvp.Key]);
#else
				_columnToProperty.TryAdd(kvp.Value, PropertiesByName[kvp.Key]);
#endif
		}
	}

	/// <inheritdoc />
	public override Func<DbDataReader, T> Mapper(DbDataReader reader)
	{
		Layout? l = _last;
		if (l is null || !l.Matches(reader)) l = Resolve(reader);
		return l.Db ??= Build<DbDataReader>(l);
	}

	/// <inheritdoc />
	public override Func<IDataRecord, T> Mapper(IDataRecord reader)
	{
		Layout? l = _last;
		if (l is null || !l.Matches(reader)) l = Resolve(reader);
		return l.Rec ??= Build<IDataRecord>(l);
	}

	Layout Resolve(IDataRecord reader)
	{
		int hash = Layout.Hash(reader);
		if (_layouts.TryGetValue(hash, out Layout[]? candidates))
		{
			foreach (Layout c in candidates)
			{
				if (c.Matches(reader))
				{
					_last = c;
					return c;
				}
			}
		}

		if (_layouts.Count >= MaxLayouts) _layouts.Clear();

		var layout = new Layout(reader);
		_layouts.AddOrUpdate(
			hash,
			_ => [layout],
			(_, existing) => [.. existing, layout]);
		_last = layout;
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
				h = unchecked((h * 31) + (r.GetName(i)?.GetHashCode() ?? 0));
				h = unchecked((h * 31) + (r.GetFieldType(i) ?? typeof(object)).GetHashCode());
			}

			return h;
		}

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
	readonly ConcurrentDictionary<int, ArrayLayout[]> _arrayLayouts = new();

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
		if (l is null || !l.Matches(names)) l = ResolveArray(names);
		return l.Map ??= BuildArray(l);
	}

	ArrayLayout ResolveArray(ImmutableArray<string> names)
	{
		int hash = names.Length;
		foreach (string n in names) hash = unchecked((hash * 31) + (n?.GetHashCode() ?? 0));

		if (_arrayLayouts.TryGetValue(hash, out ArrayLayout[]? candidates))
		{
			foreach (ArrayLayout c in candidates)
			{
				if (c.Matches(names))
				{
					_lastArray = c;
					return c;
				}
			}
		}

		if (_arrayLayouts.Count >= MaxLayouts) _arrayLayouts.Clear();

		var layout = new ArrayLayout([.. names]);
		_arrayLayouts.AddOrUpdate(hash, _ => [layout], (_, existing) => [.. existing, layout]);
		_lastArray = layout;
		return layout;
	}

	Func<object?[], T> BuildArray(ArrayLayout layout)
	{
		ParameterExpression row = Expression.Parameter(typeof(object?[]), "row");
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[layout.Names.Length];
		var statements = new List<Expression>(layout.Names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		MethodInfo convert = typeof(RowPlan<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!;
		var assigned = new HashSet<PropertyInfo>();
		for (int i = 0; i < layout.Names.Length; i++)
		{
			if (!_columnToProperty.TryGetValue(layout.Names[i], out PropertyInfo? property)) continue;
			if (!assigned.Add(property)) continue;

			bindings[i] = new Binding(property.Name, layout.Names[i]);
			Expression cell = Expression.ArrayIndex(row, Expression.Constant(i));
			statements.Add(Expression.Assign(index, Expression.Constant(i)));
			statements.Add(Expression.Call(item, property.GetSetMethod()!, Expression.Call(convert.MakeGenericMethod(property.PropertyType), cell)));
		}

		statements.Add(item);
		return Expression.Lambda<Func<object?[], T>>(Guard(statements, item, index, bindings), row).Compile();
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
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[layout.Names.Length];
		var statements = new List<Expression>(layout.Names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		var assigned = new HashSet<PropertyInfo>();
		for (int i = 0; i < layout.Names.Length; i++)
		{
			if (!_columnToProperty.TryGetValue(layout.Names[i], out PropertyInfo? property)) continue; // Not mapped: ignore the column.
			if (!assigned.Add(property)) continue; // The first matching column wins.

			bindings[i] = new Binding(property.Name, layout.Names[i]);
			Expression ordinal = Expression.Constant(i);
			Expression value = ReadValue(r, ordinal, layout.Types[i], property.PropertyType, readerType, isDbNull, getValue);

			statements.Add(Expression.Assign(index, ordinal));
			statements.Add(Expression.Call(item, property.GetSetMethod()!, value));
		}

		statements.Add(item);

		return Expression.Lambda<Func<TReader, T>>(Guard(statements, item, index, bindings), r).Compile();
	}

	// `index` tracks the column being read so a failure can name the offending property and column.
	static Expression Guard(List<Expression> statements, ParameterExpression item, ParameterExpression index, Binding?[] bindings)
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
					Expression.Throw(
						Expression.Call(
							typeof(RowPlan<T>).GetMethod(nameof(MapError), BindingFlags.Static | BindingFlags.NonPublic)!,
							ex, index, Expression.Constant(bindings)),
						typeof(T)))));
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
	static Expression? TryConvert(Expression value, Type target)
	{
		if (value.Type == target) return value;
		try
		{
			return Expression.Convert(value, target);
		}
		catch (InvalidOperationException)
		{
			return null;
		}
	}

	#endregion

	#region Runtime helpers called from compiled code

	static TTarget ConvertValue<TTarget>(object? value)
	{
		if (value is null or DBNull)
		{
			if (default(TTarget) is null) return default!;
			throw new InvalidCastException($"A NULL value cannot be assigned to a non-nullable '{typeof(TTarget)}'.");
		}

		if (value is TTarget exact) return exact;

		Type target = typeof(TTarget);
		Type u = Nullable.GetUnderlyingType(target) ?? target;
		object converted;
		if (u.IsEnum)
			converted = value is string s ? Enum.Parse(u, s, true) : Enum.ToObject(u, value);
		else if (u == typeof(Guid))
			converted = value is string gs ? Guid.Parse(gs) : value is byte[] gb ? new Guid(gb) : throw new InvalidCastException($"Cannot convert '{value.GetType()}' to Guid.");
		else if (u == typeof(string))
			converted = Convert.ToString(value, CultureInfo.InvariantCulture)!;
		else
			converted = Convert.ChangeType(value, u, CultureInfo.InvariantCulture);

		return (TTarget)converted;
	}

	static Exception MapError(Exception ex, int index, Binding?[] bindings)
	{
		if (index >= 0 && index < bindings.Length && bindings[index] is { } b)
			return new InvalidOperationException($"Unable to set value of property '{b.Property}' from column '{b.Column}'.", ex);

		return ex is InvalidOperationException ? ex : new InvalidOperationException($"Unable to create '{typeof(T)}' from the current row.", ex);
	}

	#endregion
}
