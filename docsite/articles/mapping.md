# Mapping rows to objects

`Map<T>()` reads rows into your classes. It is deferred: nothing runs until you call a terminal such as `ToArray()`, and rows are mapped as the reader streams.

[!code-csharp[](../samples/Mapping.cs#map_default)]

## The rules

- **Construction.** `T` must have a public parameterless constructor (`where T : new()`). Each row creates `new T()`.
- **What is set.** Only public instance properties that have a public setter. Public fields and static properties are not mapped.
- **Matching.** A column is matched to the property with the same name: exactly, else ignoring case, else ignoring case with the underscores removed from both (see [snake_case columns map to PascalCase properties](#snake_case-columns-map-to-pascalcase-properties)). A column with no matching property is ignored. A property with no matching column keeps the value its constructor gave it.
- **Columns named differently.** Declare a `RowMap<T>` once that names the column for a property, or ignores a property (see [Columns named differently](#columns-named-differently)).
- **No other conventions.** There is no constructor binding (positional records do not work) and no attributes. If a column name differs from the property name by more than case and underscores, declare a `RowMap<T>`.

## snake_case columns map to PascalCase properties

A column named `q_type` feeds the property `QType` with no `RowMap<T>` declared. Matching runs in three tiers over the whole result set, so a better match always wins, whatever the order of the columns:

1. The column name equals the property name exactly.
2. The column name equals the property name, ignoring case.
3. The column name with its underscores removed equals the property name with its underscores removed, ignoring case.

Within a tier the first matching column wins, one column feeds one property, and a column no tier matches is ignored. A property that tier 1 or 2 has filled is never taken by tier 3: with the columns `q_type` and `QType`, `QType` reads the `QType` column and `q_type` is ignored, in either column order.

Only a property that is matched by name is matched in tier 3. A property that a `RowMap<T>` pairs with a column keeps its exact meaning: it reads that column (exactly or ignoring case) and no other, and a `null` column still means "do not map this property", even if a `q_type` column exists. Underscores are the only difference tier 3 ignores, in either name, so `address_2` matches `Address2` and `http_request` matches `HTTPRequest`.

```csharp
public sealed class CountRow
{
	public string? QType { get; set; }
	public DateTime MinDate { get; set; }
}

// select q_type, min_date from counts
CountRow[] rows = connection.Command("select q_type, min_date from counts").Map<CountRow>().ToArray();
```

## Columns named differently

When a column is named differently from its property, declare a `RowMap<T>` once and pass it to every query that reads the model. A map is built from key/value pairs of property name and column name. Write the property name with `nameof`, so the compiler catches a rename; only the column is a string. Properties the map does not mention are still matched to columns by name.

Create the map on first use with the C# 14 `field` keyword and `??=`, so an application with hundreds of models builds only the maps it actually uses:

[!code-csharp[](../samples/Mapping.cs#map_declared)]

Pass it to `Map<T>`. The same `map` parameter is on `Results`, `ToChannel`, `AsSourceBlock`, `ToTargetBlock` and the other methods that read rows into a model:

[!code-csharp[](../samples/Mapping.cs#map_declared_use)]

Declare the map once and reuse it. A map is immutable and safe to share between threads. Passing it adds no per-call allocation or key, and no dictionary lookup while each query meets the same layout as the previous one: the compiled row reader lives in the map, and each query still matches the result-set layout against the cached one, as `Map<T>()` does.

**When a map is checked.** A map is checked when it is created, which with `field ??=` is at first use. It throws `ArgumentException` for a property that does not exist or cannot be set (a static property, or one without a public setter), a null property name, a property named twice, an empty column name, and two properties that would read the same column. The property side is matched exactly; column names are matched ignoring case:

[!code-csharp[](../samples/Mapping.cs#map_declared_unknown)]

**Ignoring a property.** A `null` column means "do not map this property". It is not a no-op: it stops the by-name match (underscore match included), so the property keeps its default value even if the result set has a column with that name. The property must exist; the column need not. Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere:

[!code-csharp[](../samples/Mapping.cs#map_declared_ignore)]

Compilers older than C# 14 have no `field` keyword and need an explicit backing field:

[!code-csharp[](../samples/Mapping.cs#map_declared_backing)]

**One-off queries.** A `new RowMap<T>([...])`, an `ImmutableArray` of pairs passed to its constructor, or the pairs passed to `Map<T>` build a new map, and compile the row reader, on every call:

[!code-csharp[](../samples/Mapping.cs#map_call_site)]

A one-row read of a 12-property model takes about 0.4 ms and allocates about 29 KB in the library's in-memory benchmark, against about 0.1 µs and a few hundred bytes through a declared map. The cost does not shrink with the result size. Use these forms only for a one-off, and declare a map for anything that runs more than occasionally.

`Map<T>` also takes the pairs themselves, with no `RowMap<T>` written; it is the same one-off and costs the same:

[!code-csharp[](../samples/Mapping.cs#map_pairs)]

**Tuples.** The overloads that took value tuples (`(string Field, string? Column)`) are removed in 11.0 (breaking). Before: `cmd.Results<Person>(("FirstName", "first_name"))`. Now: declare a `RowMap<Person>` and pass it, `cmd.Results<Person>(map)`, or on `Map<T>` pass the pairs, `cmd.Map<Person>(new(nameof(Person.FirstName), "first_name"))`. See [What's new in 11](whats-new-11.md).

## Types and NULL

Values are read with the provider's typed getters (`GetInt32`, `GetString`, ...), so nothing is boxed on the common path. When the column type and the property type differ:

- integers convert to other integer types and to enums, range-checked (an overflow throws);
- integers and floating-point values widen to `float`, `double` and `decimal`;
- anything else goes through a general conversion: a string to an enum (ignoring case) or a `Guid`, a `byte[]` to a `Guid`, any value to `string`, and otherwise `Convert.ChangeType`.

A `NULL` follows the property's declared type, as described in [NULL in a non-nullable value-type column](#null-in-a-non-nullable-value-type-column).

## NULL in a non-nullable value-type column

A nullable property (`Nullable<T>` or a reference type) gets `null`. A non-nullable value-type property (`int`, `bool`, `DateTime`, an enum, ...) keeps its initializer, else its default; nothing throws:

```csharp
public int Quantity { get; set; } = 1;  // NULL leaves 1
public int? Discount { get; set; }      // NULL gives null
public string? Note { get; set; }       // NULL gives null
```

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
