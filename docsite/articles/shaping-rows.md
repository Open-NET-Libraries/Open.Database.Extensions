# Shaping rows

`Map<T>()` reads each row into a `T`: one column feeds one member, by name. Most of the time that is all you need, and [Mapping rows to objects](mapping.md) says how it works. This page is for the other times: the row you want is not quite the row the query returns.

Every shape below has a recipe, and none of them needs a new API. Where the library has no feature for something, the page says so and says what to write instead. Every example is compiled and run against an in-memory SQLite database.

| You want | Write |
|---|---|
| A NULL to become an empty string | The member's initializer, or a [factory map](#nulls-that-mean-an-empty-string) |
| A value that is not in the row | Set it [after the close](#a-value-that-is-not-in-the-row), or return it from the query |
| A column to be left alone | A pair with a `null` column |
| A mapping that fails when a column is missing | A `required` member (not with a factory map) |
| A mapping that reads only what you list | A factory map |
| A converter inside the collection syntax | Not available: [`Convert` on the map](#a-converter-in-the-collection) |
| A row with more than 16 values | A constructor, or a factory: [no limit](#more-than-16-parameters) |
| Only some of the rows | The query's `WHERE`, else [filter after the close](#rows-you-do-not-want) |
| One column, by name | Select only that column and use `Map<long?>()` |

## What a factory map is

A constructor makes a `T` from the columns of a row, matching each parameter to a column by name. A factory map does the same with a function you write: `RowMap<T>.From(factory)` takes a delegate, usually a lambda, and calls it once per row. Use one when the type should not carry a column-shaped constructor, when it is not yours, or when it has several constructors.

- **The parameters are matched to columns by name,** with the three tiers a constructor parameter uses: exactly, ignoring case, then ignoring case and underscores. `plant_type` feeds `plantType`. A pair renames one, and in a factory map the pair's key is the name of the **parameter**.
- **It maps only its parameters.** No member of the `T` is set from a column afterwards. What the function does not take, the map does not read.
- **A NULL and a missing column follow the constructor rule.** A nullable parameter (`string?`, `int?`) receives `null`. A NULL into a non-nullable parameter, and a parameter no column feeds, get the declared default value, else `default`. Nothing throws. A function that must tell NULL from a value declares the parameter nullable, because a non-nullable `DateTime` receives `0001-01-01` for a NULL.
- **Declare it once.** A map builds its row reader on first use and keeps it, so it belongs in a `static` property, not in the call.
- **It is checked when `From` runs,** not by the compiler: a function that returns another type, a pair that names no parameter, and a single value as the `T` (`RowMap<long>.From(...)`) each throw there. For a map held in a property, that is the first use of the property.

## NULLs that mean an empty string

You may look for a `WhenNull(member, "")`, or for a `string?` converter that is called for NULL, so that `Code` gets `""` when the column is NULL. Neither exists, and neither is needed: a NULL never overwrites a member the C# type declares non-nullable, and a converter is not called for it. The member's declaration is where its NULL value lives, so write the value there:

[!code-csharp[](../samples/ShapingRows.cs#shape_plant_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_null_initializer)]

With a row whose `plant_type` is NULL, `Code` is `""`. With `'  PRESS '` it is `"PRESS"`. The model needs nullable annotations on (`<Nullable>enable</Nullable>`), as [the NULL rules](mapping.md#null-in-a-non-nullable-column) say.

The other recipe is a factory map. The function receives `null` for a NULL in a parameter declared `string?`, and decides what it means:

[!code-csharp[](../samples/ShapingRows.cs#shape_null_factory)]

[!code-csharp[](../samples/ShapingRows.cs#shape_null_initializer_use)]

A converter that must see a NULL asks for a nullable value type as its column type (`DateTime?`): see [Converting a column](mapping.md#converting-a-column). A nullable `string?` says nothing at run time, so a converter over text is never called for NULL.

## A value that is not in the row

You may look for a `.With(nameof(Post.Serial), value)` that puts a value from outside the query into every row. A map is declared once and nothing per call lives in it, so there is no such method. Two recipes cover it.

When the member is settable, set it after the close. The reader is closed when the terminal returns, so the rows are yours:

[!code-csharp[](../samples/ShapingRows.cs#shape_stamp_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_set_after_close)]

When the member is `init`-only, or the type is a record, let the query return the value as a column (in a stored procedure, `select @serial as serial` beside the rest). The map fills it like any other member, and it needs no setter:

[!code-csharp[](../samples/ShapingRows.cs#shape_stamp_init_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_set_in_query)]

## Leaving a column alone

You may look for an `Ignore(member)`. It exists as a pair whose column is the C# `null` (not SQL NULL): the member is not read, even if the result has a column of its name.

[!code-csharp[](../samples/ShapingRows.cs#shape_employee_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_ignore)]

[!code-csharp[](../samples/ShapingRows.cs#shape_ignore_use)]

`Desk` is `""` in every row although the query returns `desk`. The `null` needs no `!` after it.

## Being strict

There is no strict mode, because a mode would change every rule. There are three ways to say what you mean, each at the place it applies:

- **A member that must have a column** is `required`. When the columns of a result include none for it, the first row throws `ValueMappingException`, and an empty result never reports it, because no row is read. The routes that know the columns before the first row report it before any row: see [`required` and `init` members](mapping.md#required-and-init-members). This holds for `Map<T>()` without a factory.
- **A member that must never be filled** is a pair with a `null` column, as above.
- **A map that reads only what it lists** is a factory map. Its function takes the columns it names and no member is filled by name afterwards.

[!code-csharp[](../samples/ShapingRows.cs#shape_strict_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_required_use)]

[!code-csharp[](../samples/ShapingRows.cs#shape_factory_lists)]

A factory map does not fail on a missing column. A parameter no column feeds follows the constructor rule, so it gets its default:

[!code-csharp[](../samples/ShapingRows.cs#shape_factory_missing)]

A `required` member fails a missing column only for `Map<T>()` without a factory. A factory map never fails on a missing column, whatever the `T` marks `required`.

## A converter in the collection

You may look for the converter inside the collection syntax, as `[new("Code", "plant_type", static code => code.Trim())]`. That form does not exist: it was declined. A converter in the collection needs a new element type, which breaks every signature that declares a map. It trades the compile-time `Func<TColumn, TMember>` for a `Delegate` that is checked at run time, and it adds no ability. The form stays the one in the first recipe above: a map, then `Convert<TColumn, TMember>(member, function)`.

## More than 16 parameters

The Func delegates of C# stop at 16 parameters, so a lambda with more looks like a limit. There is none. A record or a class with a constructor of 17 parameters is built by `Map<T>()` like any other:

[!code-csharp[](../samples/ShapingRows.cs#shape_wide_model)]

[!code-csharp[](../samples/ShapingRows.cs#shape_wide_use)]

A factory takes more than 16 parameters too. The sample checks one with 17: the compiler gives such a lambda a delegate type of its own, and `From` accepts it.

## Rows you do not want

You may look for a way to skip a row before it is built: a predicate over the raw columns, or a factory that may return nothing. The library has no such feature. These are the places for the filter, best first.

**In the query.** The `WHERE` is the place for it: the database skips the rows, nothing is read or built for them, and the connection closes sooner. A column that only decides whether a row is kept belongs in the `WHERE`, not in the select list.

[!code-csharp[](../samples/ShapingRows.cs#shape_filter_where)]

**After the close.** When the decision needs your code, read the rows, let the reader close, then filter the array.

[!code-csharp[](../samples/ShapingRows.cs#shape_filter_after_close)]

**A factory that returns `null`.** It works today, and it is the way to read a value only to decide, because the parameter that holds it is not kept in the object. But the skipped rows are `null` entries in the result until you drop them, so it is a workaround and not a feature.

[!code-csharp[](../samples/ShapingRows.cs#shape_filter_factory)]

[!code-csharp[](../samples/ShapingRows.cs#shape_filter_factory_use)]

If you have a filter that cannot live in the query, tell us why: that is the case a feature would have to serve.

## One column, by name

You may look for `Map<long?>("last_id")`. There is no name to give: `Map<long?>()` reads the first column of each row as one value, whatever its name, and `long?` gives `null` for a NULL. Select only the column you want:

[!code-csharp[](../samples/ShapingRows.cs#shape_one_value)]

A factory map is not the way for one value: `RowMap<long>.From(...)` is rejected, because a single value is not a row model. To read a column that is not the first of several, write the read yourself with [`Map(selector)`](mapping.md#taking-over-mapselector).
