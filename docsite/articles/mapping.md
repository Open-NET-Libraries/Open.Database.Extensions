# Mapping rows to objects

`Map<T>()` reads rows into your classes. It is deferred: nothing runs until you call a terminal such as `ToArray()`, and rows are mapped as the reader streams.

[!code-csharp[](../samples/Mapping.cs#map_default)]

## The rules

- **Construction.** `T` must have a public parameterless constructor (`where T : new()`). Each row creates `new T()`.
- **What is set.** Only public properties that have a public setter. Public fields are not mapped.
- **Matching.** A column is matched to the property with the same name, ignoring case. A column with no matching property is ignored. A property with no matching column keeps the value its constructor gave it.
- **Overrides.** Per call, you can name the column for a property, or ignore a property.
- **No other conventions.** There is no constructor binding (positional records do not work), no snake_case folding, and no attributes. If a column name differs from the property name, use an override.

## Overrides

Pass `(Field, Column)` pairs to `Map<T>`. A `null` column ignores the field:

[!code-csharp[](../samples/Mapping.cs#map_overrides)]

[!code-csharp[](../samples/Mapping.cs#map_ignore)]

For a mapping you use more than once, declare a plan instead (see [Reusing a mapping](#reusing-a-mapping)).

Naming a field that is not a settable public property throws `ArgumentException` when `Map` is called, unless its column is `null`:

[!code-csharp[](../samples/Mapping.cs#map_unknown_field)]

## Reusing a mapping

Passing the pairs on every call repeats the mapping at each call site. Declare it once as a `RowPlan<T>` instead. A plan is made from key/value pairs of property name and column name with the collection syntax, and the property name is written with `nameof` so a rename is caught by the compiler. Create it on first use with the C# 14 `field` keyword, so an application with hundreds of models builds only the plans it actually uses:

[!code-csharp[](../samples/Mapping.cs#map_plan)]

Pass it wherever overrides are accepted (`Map`, `Results`, `ToChannel`, `AsSourceBlock`, `ToTargetBlock`, ...):

[!code-csharp[](../samples/Mapping.cs#map_plan_use)]

A plan is checked where it is declared, not on the first query. It throws `ArgumentException` for a property that does not exist or cannot be set, a null property name, a property named twice, an empty column name, and two properties that would read the same column:

[!code-csharp[](../samples/Mapping.cs#map_plan_unknown)]

**Ignoring a property.** A `null` column means "do not map this property". It is not a no-op: the property is not read at all, not even by name, so it keeps its default value even if the result set has a column with that name. Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere:

[!code-csharp[](../samples/Mapping.cs#map_plan_ignore)]

A plan is immutable and safe to share between threads. Passing it adds no per-call allocation, key or dictionary lookup; the compiled row reader lives in the plan, and each query still matches the result-set layout against the cached one, as `Map<T>()` does. A `static readonly` field also works, but it is built when its type's static initializer first runs, together with every other static field of that type, and a plan with an error then surfaces as a `TypeInitializationException`.

**One-off queries.** Pass the pairs inline:

[!code-csharp[](../samples/Mapping.cs#map_inline)]

This builds a new plan and compiles the row reader on every call and caches nothing: a one-row read of a 12-property model takes about 0.43 ms and allocates about 29 KB in the library's in-memory benchmark, against about 0.1 µs and a few hundred bytes through a declared plan. Use it only for a one-off. A lone pair must be written as a collection expression, `Map<Person>([new(...)])`, because the compiler reads a single `new(...)` as the whole collection.

The tuple form shown under [Overrides](#overrides) is a convenience: the tuples are converted to key/value pairs on each call. Prefer a plan, or key/value pairs.

## Types and NULL

Values are read with the provider's typed getters (`GetInt32`, `GetString`, ...), so nothing is boxed on the common path. When the column type and the property type differ:

- integers convert to other integer types and to enums, range-checked (an overflow throws);
- integers and floating-point values widen to `float`, `double` and `decimal`;
- anything else goes through a general conversion: a string to an enum (ignoring case) or a `Guid`, a `byte[]` to a `Guid`, any value to `string`, and otherwise `Convert.ChangeType`.

`NULL` is honored for reference types and for `Nullable<T>` properties. A `NULL` in a column mapped to a non-nullable value-type property fails with an `InvalidOperationException` that names the property and the column.

## Terminals

`ToArray`, `ToList`, `ToImmutableArray`, `ToDictionary`, `ToHashSet`, `FirstOrDefault`, `SingleOrDefault` and `Read`, each with an `Async` twin. Prefer `ToArray` for results you only read: it allocates one exactly sized array. An empty result is the shared empty array.

[!code-csharp[](../samples/Mapping.cs#map_first_single)]

`Skip` and `Take` are part of the query: skipped rows are advanced without being mapped, and reading stops once the take is satisfied. Call `Skip` before `Take`, as the types enforce:

[!code-csharp[](../samples/Mapping.cs#map_paging)]

`Read` hands you the rows as a lazy sequence that is only valid inside the handler:

[!code-csharp[](../samples/Mapping.cs#map_stream)]

[!code-csharp[](../samples/Mapping.cs#map_dictionary_async)]

## Taking over: `Map(selector)`

When the rules are not enough, `Map(Func<IDataRecord, T>)` runs your function for each row. It has the same deferred terminals:

[!code-csharp[](../samples/Mapping.cs#map_selector)]

## How it works

The mapping from a result-set shape (column names and column types) to `T` is compiled once and cached, so a repeated query does no reflection and no compilation. The reader is opened with `SequentialAccess | SingleResult` (plus `SingleRow` for `FirstOrDefault` when nothing is skipped). `Map(selector)` does not use `SequentialAccess`, because your function may read columns in any order.
