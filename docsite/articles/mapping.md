# Mapping rows to objects

`Map<T>()` reads rows into your classes. It is deferred: nothing runs until you call a terminal such as `ToArray()`, and rows are mapped as the reader streams.

[!code-csharp[](../samples/Mapping.cs#map_default)]

| Your columns | Write |
|---|---|
| Named like the properties, or the same with underscores (`first_name` for `FirstName`) | `Map<T>()` |
| Read into a positional record, or a class with one public constructor | `Map<T>()`: the constructor is called ([Records and constructors](#records-and-constructors)) |
| Named differently | Declare a `RowMap<T>` once and pass it: `Map<T>(map)` |
| One property must not be read | A `null` column for it in the `RowMap<T>` |
| A property must always be read | Declare it `required` ([`required` and `init` members](#required-and-init-members)) |
| A column can hold NULL | Declare the property nullable (`int?`, `string?`); a property the type declares non-nullable (`int`, or a `string` with nullable annotations on) keeps its initializer, else its default |
| Your own conversion | `Map(selector)` |

## The rules

- **Construction.** A class with a public parameterless constructor is built with `new T()` for each row. A struct with no public constructor, or with a public parameterless one, starts from its default value (its own parameterless constructor is not called). Then its properties are set. Any other type is built through its one public constructor: a positional record, a record struct, or a class with a single public constructor (see [Records and constructors](#records-and-constructors)). These throw `InvalidOperationException` at the call, even when the result is empty: an interface, an abstract class, a delegate, a single value like `int` or `string`, a class with no public constructor, two or more public constructors and none without parameters, and a type with a `required` public field (a field cannot be set).
- **What is set.** The parameters of the constructor, when the type is built through one, and the public instance properties that have a public setter (a record's `init` property included). A parameter and a property of the same name, ignoring case, are one member: it is fed once, through the constructor. Public fields and static properties are not mapped.
- **Matching.** A column is matched to the property or constructor parameter with the same name: exactly, else ignoring case, else ignoring case with the underscores removed from both (see [snake_case columns map to PascalCase properties](#snake_case-columns-map-to-pascalcase-properties)). A column with no matching member is ignored. A property with no matching column keeps the value its constructor gave it. A parameter with no matching column gets its declared default value, else its default (`0`, `null`); so does a parameter the type declares non-nullable (a value type, or a reference type with nullable annotations on) when the cell is `NULL`.
- **Columns named differently.** Declare a `RowMap<T>` once that names the column for a property, or ignores a property (see [Columns named differently](#columns-named-differently)).
- **No other conventions.** The library has no mapping attributes of its own. If a column name differs from the property name by more than case and underscores, declare a `RowMap<T>`.

## Records and constructors

A positional record is read with plain `Map<T>()`. This table:

| id | author | title | body |
|---|---|---|---|
| 1 | 42 | Post 1 | Body 1 |
| 2 | 42 | Post 2 | NULL |

is read into this record:

[!code-csharp[](../samples/Models.cs#record_post_model)]

with this call:

[!code-csharp[](../samples/Mapping.cs#map_record)]

The result is `PostRecord(1, 42, "Post 1", "Body 1")` and `PostRecord(2, 42, "Post 2", null)`: each column feeds the constructor parameter of the same name, and a NULL into `string? Body` is `null`. The `long` parameters match what SQLite returns for an `INTEGER` column; an `int` parameter works too, because integers convert.

### Which types are built through a constructor

| The type | How a row is built |
|---|---|
| A class with a public parameterless constructor (it may have other constructors) | `new T()`, then its properties are set. The other constructors are never used |
| A struct with no public constructor, or with a public parameterless one | Its default value, then its properties are set. A struct's own parameterless constructor is not called |
| A positional record, a record struct, or any class or struct with exactly one public constructor | Through that constructor, then the properties it does not cover are set |
| A class or struct with two or more public constructors and none without parameters | Not mappable: `InvalidOperationException` at the call. Use `Map(selector)`, or make all but one constructor non-public |

Only public constructors count. The choice depends on the type alone, never on the columns of a query. Use `Map(selector)` when you need to choose a constructor by the columns.

### How parameters are matched to columns

A constructor parameter is matched like a property: the same three tiers (exact, ignoring case, ignoring case and underscores) over the whole result set, with the same [`RowMap<T>` pairs](#columns-named-differently). A record's positional property and its parameter are one member: its column feeds the constructor once, and the `init` setter of the positional property is never called. The properties the constructor does not cover are set after it, from their own columns:

[!code-csharp[](../samples/Models.cs#record_extra_model)]

[!code-csharp[](../samples/Mapping.cs#map_record_extra)]

### `required` and `init` members

An `init` property is set like any other settable property. A `required` property is set the same way, and it must be fed:

- **A column must feed it in every result shape** (a `required` property of a base class too). When none does, the first row of that shape throws `ValueMappingException`, with the property, its type, the model and the column that was looked for in the message. A query with no rows never reports it. `Retrieve()` with `DequeueAs<T>()`, `DataTable.To<T>()`, `ToChannel` and the Dataflow methods have the columns before any row, so they report it even for no rows.
- **A constructor parameter's default does not feed it.** A `required` property needs its own column, even when a constructor parameter has the same name.
- **It cannot be ignored.** A `RowMap<T>` pair with a `null` column for a `required` property throws `ArgumentException` when the map is created.
- **`[SetsRequiredMembers]`.** When the constructor the type is built through carries it, nothing is enforced: the members are ordinary properties.
- **A `required` public field** cannot be set, because fields are never mapped, so the type throws `InvalidOperationException` at the call. Declare it as a property.

[!code-csharp[](../samples/Models.cs#required_model)]

[!code-csharp[](../samples/Mapping.cs#map_required)]

### A missing column and a NULL

A property no column feeds keeps what the constructor or its initializer gave it. A constructor parameter no column feeds takes its declared default value (`int stars = 3` gives `3`), else `default` (`0`, `null`). A NULL cell follows the rule in [NULL in a non-nullable column](#null-in-a-non-nullable-column), which for a parameter means its declared default instead of the initializer:

| The member is declared | A NULL gives a property | A NULL gives a constructor parameter |
|---|---|---|
| A value type: `int`, `bool`, `DateTime`, an enum | Its initializer, else its default | Its declared default, else `default` |
| A `string` with nullable annotations on | Its initializer, else `null` | Its declared default, else `null` |
| `int?`, `string?`, or `[AllowNull]` on a property | `null` | `null` (the declared default is not used: a NULL is a value) |
| A `string` with nullable annotations off, or a type parameter | `null` | `null` |

[!code-csharp[](../samples/Models.cs#review_model)]

[!code-csharp[](../samples/Mapping.cs#map_ctor_null_column)]

### A `RowMap<T>` for a record

A pair names a member by its property name, so `nameof(PostRecord.Title)` reaches the positional parameter `Title` (for a parameter the case of the name does not matter). A parameter that has no property of its name is named by the parameter itself. A `null` column means the parameter is not read: it takes its declared default, else `default`.

[!code-csharp[](../samples/Mapping.cs#map_record_declared)]

[!code-csharp[](../samples/Mapping.cs#map_record_declared_use)]

### Errors

A type that cannot be mapped throws `InvalidOperationException` at the call, before the query runs, even when the result would be empty. The name in the message is the type's short name:

[!code-csharp[](../samples/Models.cs#ambiguous_constructor_model)]

[!code-csharp[](../samples/Mapping.cs#map_unmappable)]

| The type | Message |
|---|---|
| An interface | `'IPost' cannot be mapped: it is an interface. Map to a class or struct, or use Map(selector).` |
| An abstract class or a delegate | The same, with `it is an abstract class` or `it is a delegate` |
| A single value (`int`, `string`, ...) | `'Int32' is a single value, not a row model. Use ExecuteScalar<T>() or FirstOrdinalResults<T>().` |
| No public constructor | `'Post' cannot be mapped: it has no public constructor.` |
| Two or more public constructors, none without parameters | `'PostCard' cannot be mapped: it has 2 public constructors, PostCard(String title) and PostCard(String title, String body), and none without parameters. Make one of them non-public, or use Map(selector).` |
| A `required` public field | `'Post' cannot be mapped: its required field 'Title' cannot be set. Make it a property.` |
| A constructor parameter that is `ref`, `in`, `out` or a pointer | `'Post' cannot be mapped: its constructor parameter 'id' is passed by reference or is a pointer. Use Map(selector).` |

Errors in the data throw per row: a missing `required` column and an unconvertible value throw `ValueMappingException`, and a constructor that throws is wrapped (see [Values that cannot be mapped](#values-that-cannot-be-mapped)).

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

**When a map is checked.** A map is checked when it is created, which with `field ??=` is at first use. It throws `ArgumentException` for a property or constructor parameter that does not exist (a static property, or one without a public setter that no constructor parameter covers, is not a name either), a null name, a member named twice (a positional record's property and its parameter are one member), an empty column name, a `null` column for a `required` property, and two members that would read the same column. A property name is matched exactly; a constructor parameter's name ignoring case; column names are matched ignoring case:

[!code-csharp[](../samples/Mapping.cs#map_declared_unknown)]

**Ignoring a property.** A `null` column means "do not map this property". It is not a no-op: it stops the by-name match (underscore match included), so the property keeps the value its constructor or initializer gave it (else its default), and a constructor parameter takes its declared default (else `default`), even if the result set has a column with that name. The property must exist; the column need not. A `required` property cannot be ignored. Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere:

[!code-csharp[](../samples/Mapping.cs#map_declared_ignore)]

Compilers older than C# 14 have no `field` keyword and need an explicit backing field:

[!code-csharp[](../samples/Mapping.cs#map_declared_backing)]

**One-off queries.** A collection expression or a `new RowMap<T>([...])` written at the call site, an `ImmutableArray` of pairs passed to the constructor, or the pairs passed to `Map<T>` build a new map, and compile the row reader, on every call:

[!code-csharp[](../samples/Mapping.cs#map_call_site)]

A one-row read of a 12-property model takes about 0.43 to 0.45 ms and allocates about 29.5 KB in the library's in-memory benchmark (the [`RowMap<T>` scenario](https://github.com/Open-NET-Libraries/Open.Database.Extensions/blob/master/Benchmarks/RESULTS.md#rowmapt-a-prebuilt-column-mapping-when-every-column-is-named-differently)), against about 0.1 µs and a few hundred bytes through a declared map. The cost does not shrink with the result size. Use these forms only for a one-off, and declare a map for anything that runs more than occasionally.

`Map<T>` also takes the pairs themselves, with no `RowMap<T>` written; it is the same one-off and costs the same:

[!code-csharp[](../samples/Mapping.cs#map_pairs)]

**Tuples.** The value-tuple overloads (`(string Field, string? Column)`) are removed in 11.0; see [What's new in 11](whats-new-11.md).

## Types and NULL

Values are read with the provider's typed getters (`GetInt32`, `GetString`, ...), so nothing is boxed on the common path. When the column type and the property type differ:

- integers convert to other integer types and to enums, range-checked (an overflow throws `ValueMappingException`);
- integers and floating-point values widen to `float`, `double` and `decimal`;
- anything else goes through a general conversion: a string to an enum (ignoring case) or a `Guid`, a `byte[]` to a `Guid`, any value to `string` except a `byte[]`, which throws, and otherwise `Convert.ChangeType`.

## NULL in a non-nullable column

A NULL never overwrites a member the C# type declares non-nullable. A value-type property (`int`, `bool`, `DateTime`, an enum, ...) keeps its initializer, else its default, and so does a reference-type property when nullable annotations are on: `string Name { get; set; } = "";` keeps `""`, and one with no initializer stays `null`. A constructor parameter of such a type gets its declared default value, else its default. `Nullable<T>` (`int?`), a reference declared nullable (`string?`, or `[AllowNull]` on a property), a reference type in code with nullable annotations off, and a member declared as a type parameter (`T Value`) get `null`; nothing throws. If NULL means something, declare the member nullable (`int?`, `string?`): that is the only way to tell NULL from 0 or "".

The rule reads the nullable annotations the compiler stores in the assembly, so it needs `<Nullable>enable</Nullable>` (or `#nullable enable`) in the code that declares the model.

[!code-csharp[](../samples/Models.cs#order_line_model)]

## Values that cannot be mapped

A value that cannot be mapped (an unconvertible value, an overflow) throws `ValueMappingException`, a `DataException` (not an `InvalidOperationException`). It carries `PropertyName`, `ColumnName` and `TargetType`, its message names all three (`Unable to set value of property 'Id' (Int32) from column 'id'.`), and the conversion error is its `InnerException`. A value that cannot be passed to a constructor parameter says so (`Unable to pass value of column 'post_id' to constructor parameter 'Id' (Int32) of 'Post'.`); `PropertyName` is then the member's name. A `required` property no column feeds throws it too, without an inner exception (see [`required` and `init` members](#required-and-init-members)). Provider exceptions (`DbException`) and cancellation pass through unwrapped. Any other failure while building a row object throws `InvalidOperationException` (the cause is its inner exception, unless the cause already was an `InvalidOperationException`); when a constructor throws, its message is `Unable to create 'MyApp.Post' from the current row.`, with the type's full name.

[!code-csharp[](../samples/Mapping.cs#map_value_error)]

## Terminals

`ToArray`, `ToList`, `ToImmutableArray`, `ToDictionary`, `ToHashSet`, `FirstOrDefault`, `SingleOrDefault` and `Read`, each with an `Async` twin. There is no throwing `First` or `Single`: the `OrDefault` forms return `null` for no rows. Prefer `ToArray` for results you only read: it allocates one exactly sized array. An empty result is the shared empty array.

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
