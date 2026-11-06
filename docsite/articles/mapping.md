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

Naming a field that is not a settable public property throws `ArgumentException` when `Map` is called, unless its column is `null`:

[!code-csharp[](../samples/Mapping.cs#map_unknown_field)]

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
