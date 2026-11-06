# Mapping rows to objects

`Map<T>()` reads rows into your classes. It is deferred: nothing runs until you call a terminal such as `ToArray()`, and rows are mapped as the reader streams.

[!code-csharp[](../samples/Mapping.cs#map_default)]

| Your columns | Write |
|---|---|
| Named like the properties, or the same with underscores (`first_name` for `FirstName`) | `Map<T>()` |
| Named differently | Declare a `RowMap<T>` once and pass it: `Map<T>(map)` |
| One property must not be read | A `null` column for it in the `RowMap<T>` |
| A column can hold NULL | Declare the property nullable (`int?`, `string?`); a non-nullable value-type property keeps its initializer, else its default |
| Constructor binding, or your own conversion | `Map(selector)` |

## The rules

- **Construction.** `T` must have a public parameterless constructor (`where T : new()`). Each row creates `new T()`.
- **What is set.** Only public instance properties that have a public setter. Public fields and static properties are not mapped.
- **Matching.** A column is matched to the property with the same name: exactly, else ignoring case, else ignoring case with the underscores removed from both (see [snake_case columns map to PascalCase properties](#snake_case-columns-map-to-pascalcase-properties)). A column with no matching property is ignored. A property with no matching column keeps the value its constructor gave it.
- **Columns named differently.** Declare a `RowMap<T>` once that names the column for a property, or ignores a property (see [Columns named differently](#columns-named-differently)).
- **No other conventions.** There is no constructor binding (positional records do not work) and no attributes. If a column name differs from the property name by more than case and underscores, declare a `RowMap<T>`.

## snake_case columns map to PascalCase properties

A column named `first_name` feeds the property `FirstName` with no `RowMap<T>` declared. Matching runs in three tiers over the whole result set, so a better match always wins, whatever the order of the columns:

1. The column name equals the property name exactly.
2. The column name equals the property name, ignoring case.
3. The column name with its underscores removed equals the property name with its underscores removed, ignoring case.

Within a tier the first matching column wins, one column feeds one property, and a column no tier matches is ignored. A property that tier 1 or 2 has filled is never taken by tier 3: with the columns `q_type` and `QType`, `QType` reads the `QType` column and `q_type` is ignored, in either column order.

Only a property that is matched by name is matched in tier 3. A property that a `RowMap<T>` pairs with a column keeps its exact meaning: it reads that column (exactly or ignoring case) and no other, and a `null` column still means "do not map this property", even if a `q_type` column exists. Underscores are the only difference tier 3 ignores, in either name, so `address_2` matches `Address2` and `http_request` matches `HTTPRequest`.

[!code-csharp[](../samples/Mapping.cs#map_snake_case)]

## Columns named differently

When a column is named differently from its property, declare a `RowMap<T>` once and pass it to every query that reads the model. A map is built from key/value pairs of property name and column name. Write the property name with `nameof`, so the compiler catches a rename; only the column is a string. Properties the map does not mention are still matched to columns by name.

Declare it with the collection syntax, created on first use with the C# 14 `field` keyword and `??=`:

[!code-csharp[](../samples/Mapping.cs#map_declared)]

Pass it to `Map<T>`. The same `map` parameter is on `Results`, `ToChannel`, `AsSourceBlock`, `ToTargetBlock` and the other methods that read rows into a model:

[!code-csharp[](../samples/Mapping.cs#map_declared_use)]

**Other ways to declare it.** All of these make the same map. The constructor takes one pair (no brackets needed) or several, an `ImmutableArray` or any `IEnumerable` of pairs also works, and a map enumerates the pairs it was declared with, so `new RowMap<T>(map)` makes an equal copy. An empty map, `[]`, maps by name like no map at all:

[!code-csharp[](../samples/Mapping.cs#map_declaration_forms)]

Declare the map once and reuse it. A map is immutable and safe to share between threads, and a declared map adds nothing per call beyond a check that the result-set layout matches the one its reader was compiled for.

**When a map is checked.** A map is checked when it is created, which with `field ??=` is at first use. It throws `ArgumentException` for a property that does not exist or cannot be set (a static property, or one without a public setter), a null property name, a property named twice, an empty column name, and two properties that would read the same column. The property side is matched exactly; column names are matched ignoring case:

[!code-csharp[](../samples/Mapping.cs#map_declared_unknown)]

**Ignoring a property.** A `null` column means "do not map this property". It is not a no-op: it stops the by-name match (underscore match included), so the property keeps the value `new T()` gave it (its initializer, else its default) even if the result set has a column with that name. The property must exist; the column need not. Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere:

[!code-csharp[](../samples/Mapping.cs#map_declared_ignore)]

Compilers older than C# 14 have no `field` keyword and need an explicit backing field:

[!code-csharp[](../samples/Mapping.cs#map_declared_backing)]

**One-off queries.** A collection expression or a `new RowMap<T>([...])` written at the call site, an `ImmutableArray` of pairs passed to the constructor, or the pairs passed to `Map<T>` build a new map, and compile the row reader, on every call:

[!code-csharp[](../samples/Mapping.cs#map_call_site)]

A one-row read of a 12-property model takes about 0.4 ms and allocates about 29 KB in the library's in-memory benchmark, against about 0.1 µs and a few hundred bytes through a declared map. The cost does not shrink with the result size. Use these forms only for a one-off, and declare a map for anything that runs more than occasionally.

`Map<T>` also takes the pairs themselves, with no `RowMap<T>` written; it is the same one-off and costs the same:

[!code-csharp[](../samples/Mapping.cs#map_pairs)]

**Tuples.** The value-tuple overloads (`(string Field, string? Column)`) are removed in 11.0; see [What's new in 11](whats-new-11.md).

## Types and NULL

Values are read with the provider's typed getters (`GetInt32`, `GetString`, ...), so nothing is boxed on the common path. When the column type and the property type differ:

- integers convert to other integer types and to enums, range-checked (an overflow throws `ValueMappingException`);
- integers and floating-point values widen to `float`, `double` and `decimal`;
- anything else goes through a general conversion: a string to an enum (ignoring case) or a `Guid`, a `byte[]` to a `Guid`, any value except a `byte[]` to `string` (a `byte[]` to `string` throws), and otherwise `Convert.ChangeType`.

## NULL in a non-nullable value-type column

A nullable property (`Nullable<T>`) or a reference type gets `null`, even a `string` that has a non-null initializer. A non-nullable value-type property (`int`, `bool`, `DateTime`, an enum, ...) keeps its initializer, else its default; nothing throws. If NULL means something, declare the property nullable (`int?`): that is the only way to tell NULL from 0.

[!code-csharp[](../samples/Models.cs#order_line_model)]

## Values that cannot be mapped

A value that cannot be mapped (an unconvertible value, an overflow) throws `ValueMappingException`, a `DataException` (not an `InvalidOperationException`). It carries `PropertyName`, `ColumnName` and `TargetType`, its message names all three (`Unable to set value of property 'Id' (Int32) from column 'id'.`), and the conversion error is its `InnerException`. Provider exceptions (`DbException`) and cancellation pass through unwrapped. Any other failure while building a row object throws `InvalidOperationException` (the cause is its inner exception, unless the cause already was an `InvalidOperationException`).

[!code-csharp[](../samples/Mapping.cs#map_value_error)]

## Terminals

`ToArray`, `ToList`, `ToImmutableArray`, `ToDictionary`, `ToHashSet`, `FirstOrDefault`, `SingleOrDefault` and `Read`, each with an `Async` twin. Prefer `ToArray` for results you only read: it allocates one exactly sized array. An empty result is the shared empty array.

[!code-csharp[](../samples/Mapping.cs#map_first_single)]

`Skip` and `Take` are part of the query: skipped rows are advanced without being mapped, and reading stops once the take is satisfied. Call `Skip` before `Take`, as the types enforce:

[!code-csharp[](../samples/Mapping.cs#map_paging)]

`Read` hands you the rows as a lazy sequence that is only valid inside the handler:

[!code-csharp[](../samples/Mapping.cs#map_stream)]

Every terminal has an `Async` twin that takes a `CancellationToken`:

[!code-csharp[](../samples/Mapping.cs#map_dictionary_async)]

## Taking over: `Map(selector)`

When the rules are not enough, `Map(Func<IDataRecord, T>)` runs your function for each row. It has the same deferred terminals:

[!code-csharp[](../samples/Mapping.cs#map_selector)]

## How it works

The mapping from a result-set shape (column names and column types) to `T` is compiled once and cached, so a repeated query does no reflection and no compilation. The reader is opened with `SequentialAccess | SingleResult` (plus `SingleRow` for `FirstOrDefault` when nothing is skipped). `Map(selector)` does not use `SequentialAccess`, because your function may read columns in any order.
