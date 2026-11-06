# Mapping rows to objects

`Map<T>()` reads rows into your classes. It is deferred: nothing runs until you call a terminal such as `ToArray()`, and rows are mapped as the reader streams.

[!code-csharp[](../samples/Mapping.cs#map_default)]

## The rules

- **Construction.** `T` must have a public parameterless constructor (`where T : new()`). Each row creates `new T()`.
- **What is set.** Only public instance properties that have a public setter. Public fields and static properties are not mapped.
- **Matching.** A column is matched to the property with the same name, ignoring case. A column with no matching property is ignored. A property with no matching column keeps the value its constructor gave it.
- **Columns named differently.** Declare a `RowMap<T>` once that names the column for a property, or ignores a property (see [Columns named differently](#columns-named-differently)).
- **No other conventions.** There is no constructor binding (positional records do not work), no snake_case folding, and no attributes. If a column name differs from the property name, declare a `RowMap<T>`.

## Columns named differently

When a column is named differently from its property, declare a `RowMap<T>` once and pass it to every query that reads the model. A map is a list of key/value pairs of property name and column name, written as a collection expression. Write the property name with `nameof`, so the compiler catches a rename; only the column is a string. Properties the map does not mention are still matched to columns by name.

Create the map on first use with the C# 14 `field` keyword and `??=`, so an application with hundreds of models builds only the maps it actually uses:

[!code-csharp[](../samples/Mapping.cs#map_declared)]

Pass it to `Map<T>`. The same `map` parameter is on `Results`, `ToChannel`, `AsSourceBlock`, `ToTargetBlock` and the other methods that read rows into a model:

[!code-csharp[](../samples/Mapping.cs#map_declared_use)]

Declare the map once and reuse it. A map is immutable and safe to share between threads. Passing it adds no per-call allocation, key or dictionary lookup: the compiled row reader lives in the map, and each query still matches the result-set layout against the cached one, as `Map<T>()` does. A map keeps up to 128 distinct result-set layouts; a map that meets more clears them, and its queries compile again.

**When a map is checked.** A map is checked when it is first created: at first use with `field ??=`, at type initialization with `static readonly`. It throws `ArgumentException` for a property that does not exist or cannot be set (a static property, or one without a public setter), a null property name, a property named twice, an empty column name, and two properties that would read the same column. The property side is matched exactly; column names are matched ignoring case:

[!code-csharp[](../samples/Mapping.cs#map_declared_unknown)]

**Ignoring a property.** A `null` column means "do not map this property". It is not a no-op: it stops the by-name match, so the property keeps its default value even if the result set has a column with that name. The property must exist; the column need not. Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere:

[!code-csharp[](../samples/Mapping.cs#map_declared_ignore)]

**Other ways to hold a map.** A `static readonly` field also works, but it is built when its type's static initializer first runs, together with every other static field of that type, and a map with an error then surfaces as a `TypeInitializationException`. Keep such maps in their own small static class:

[!code-csharp[](../samples/Mapping.cs#map_declared_static)]

To publish exactly one instance however many threads race on the first use, use `LazyInitializer.EnsureInitialized` (the factory may still run more than once; the extra results are discarded):

[!code-csharp[](../samples/Mapping.cs#map_declared_ensure)]

A `Lazy<RowMap<T>>` also works; it adds a wrapper object per map. Compilers older than C# 14 have no `field` keyword and need an explicit backing field:

[!code-csharp[](../samples/Mapping.cs#map_declared_backing)]

Reading a held map allocates nothing and takes one to two nanoseconds whichever way it is held.

**One-off queries.** A collection expression, an `ImmutableArray` of pairs or tuples written at the call site build a new map, and compile the row reader, on every call:

[!code-csharp[](../samples/Mapping.cs#map_call_site)]

A one-row read of a 12-property model takes about 0.4 ms and allocates about 29 KB in the library's in-memory benchmark, against about 0.1 µs and a few hundred bytes through a declared map. The cost does not shrink with the result size. Use these forms only for a one-off, and declare a map for anything that runs more than occasionally.

The tuple overloads of `Map<T>` and the other methods are a convenience for the same one-off. They are checked as strictly as a map, so an unknown or repeated property throws `ArgumentException` at the call, and they cost the same. Key/value pairs are what a map stores; tuples are only a way in:

[!code-csharp[](../samples/Mapping.cs#map_tuples)]

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
