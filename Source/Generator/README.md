# Open.Database.Extensions.Generator (prototype)

A Roslyn incremental source generator that serves a plain `Map<T>()` with generated row readers, through C# interceptors.
It is a **prototype**: not packaged, not referenced by Core, and Core builds, tests and ships without it.

## What it does

For each call `x.Map<T>()` with a closed model type `T`, where `x` is an `IExecuteReader`, an `IDbCommand` or an `IDataReader`,
it emits an interceptor that calls the existing public overload `Map(Func<IDataRecord, T>)` with a generated row reader.
The generated reader follows the runtime `RowMap<T>` rules:

- the model shape (create then set, or the one public constructor; init-only and required members; inherited properties);
- the column match, once per result set (declared `[Column(Order)]` ordinals, then exact, ignoring case, ignoring case and underscores);
- the typed reads and the conversions the runtime makes, the NULL rules (kept or assigned), and the runtime's exception messages.

Anything it does not reproduce exactly is handed to the runtime map (see "Declines to the runtime").

## Read modes (chosen once per result set)

| Mode | When | Per cell |
|---|---|---|
| Constant ordinals | every member declares `[Column(Order)]` and sits there, every read exact | the typed read at a literal ordinal, in `Order` order |
| Full | every member fed, columns ascending in member order, every read exact | the typed read at the layout's ordinal |
| Full, converting | as Full, with a converting read (an `int` member from a `long` column, ...) | a switch on the read chosen for that column |
| Guarded | a member without a column, or the columns in another order | a loop over the fed members in column order, a switch per member and per read |

Every mode reads each cell once, in ascending column order, so a `SequentialAccess` reader is served too.

## Known limits

- **A known silent difference.** The binding is per reader, and is renewed when the reader's column count changes. On the same reader, a query value kept and
  run again after `NextResult` (`var q = reader.Map<T>(); q.ToList(); reader.NextResult(); q.ToList();`) whose second set has the same column count but
  another layout reads the second set with the first set's ordinals: wrong values, or a cast error, where the runtime map reads it right.
  A different column count is caught. Closing it needs a per-result-set hook in Core (additive, 11.x); until then the generator does not ship on by default.
  Both cases are pinned by tests.
- Each query allocates the binder and its delegate, and one binding per result set; two evaluations of one call site are not equal queries.

## Use (in this repository)

```xml
<ProjectReference Include="..\..\Source\Generator\Open.Database.Extensions.Generator.csproj"
                  OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
<Import Project="..\..\Source\Generator\build\Open.Database.Extensions.Generator.targets" />
```

The targets file allows the generator's interceptor namespace and stands in for the `buildTransitive` targets a package would carry.
`<OdxGenerateMaps>false</OdxGenerateMaps>` switches it off. It needs C# 12 or later.

## Declines to the runtime

Not intercepted (the call runs as today): scalars, tuples, anonymous, abstract, generic or inaccessible types, a model with more than one public constructor
and no parameterless one, overridden or hidden properties, a required member fed by a constructor parameter, a `DataTable` receiver, and a call inside a generic method (`Map<T>()` with an open `T`).

Per result set, handed to the runtime map: a column whose type has no exact or converting typed read, declared columns that do not match (the runtime then throws its own exception),
and a layout that leaves an init-only or required member without a column. Per row: a NULL the runtime would keep in an init-only or required member
(the runtime builds that row from the values already read).

## Tests

`Tests/Open.Database.Extensions.Generator.Tests` compares every intercepted call with `Map(RowMap<T>.Default)` on the same data, on the twin models of
`Tests/Open.Database.Extensions.Generator.Models` (another assembly).
