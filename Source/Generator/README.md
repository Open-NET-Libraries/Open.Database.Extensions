# Open.Database.Extensions.Generator (prototype)

A Roslyn incremental source generator that serves a plain `Map<T>()` with generated row readers, through C# interceptors.
It is a **prototype**: not packaged, not referenced by Core, and Core builds, tests and ships without it.

## What it does

For each call `x.Map<T>()` with a closed model type `T`, where `x` is an `IExecuteReader`, an `IDbCommand` or an `IDataReader`,
it emits an interceptor that calls the existing public overload `Map(Func<IDataRecord, T>)` with a generated row reader.
The generated reader follows the runtime `RowMap<T>` rules. Anything it does not reproduce exactly is handed to the runtime map (see "Declines to the runtime").

## Known limits

- **Wrong values, silently, after `NextResult` with the same column count.** The binding is per reader and is renewed only when the reader's column count
  changes. On the same reader, a query value kept and run again after `NextResult` (`var q = reader.Map<T>(); q.ToList(); reader.NextResult(); q.ToList();`)
  whose second set has the same column count but another layout (other column order or names) reads the second set at the FIRST set's ordinals: values
  land in the wrong members, or a cast error is thrown, where the runtime map re-checks the layout per result set and reads it right.
  The row index in an error report also continues from the first set instead of restarting at 0 (a different column count restarts it).
  Neither can be detected without a check on every row, which this generator does not make; a per-result-set hook in Core would close both.
  Pinned by tests.
- **Errors.** A failed row is a `ValueMappingException` naming the model type and the row (0-based, among the rows this query mapped), the exception inside;
  no member or column is named. A member declared non-nullable is read unchecked, with its typed getter: a NULL there throws (the provider's getter, or the cast
  of `DBNull`), reported as that row's error; the runtime map keeps the member's value instead. A nullable member is tested with `IsDBNull` and gets null.
  An opt-in attribute that checks every non-nullable member is planned.
- Each query allocates the binder and its delegate, and one binding per result set; two evaluations of one call site are not equal queries.

## Use (in this repository)

```xml
<ProjectReference Include="..\..\Source\Generator\Open.Database.Extensions.Generator.csproj"
                  OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
<Import Project="..\..\Source\Generator\build\Open.Database.Extensions.Generator.targets" />
```

`<OdxGenerateMaps>false</OdxGenerateMaps>` switches it off. It needs C# 12 or later.

## Declines to the runtime

Not intercepted (the call runs the runtime map): scalars, tuples, anonymous, abstract, generic or inaccessible types, a model with more than one public constructor
and no parameterless one, overridden or hidden properties, a required member fed by a constructor parameter, a `DataTable` receiver, and a call inside a generic method (`Map<T>()` with an open `T`).

Per result set, handed to the runtime map: a column whose type has no exact or converting typed read, declared columns that do not match (the runtime then throws its own exception),
and a layout that leaves an init-only or required member without a column. Per row: a NULL the runtime would keep in an init-only or required member
(the runtime builds that row from the values already read).

## Tests

`Tests/Open.Database.Extensions.Generator.Tests` compares every intercepted call with `Map(RowMap<T>.Default)` on the same data, on the twin models of
`Tests/Open.Database.Extensions.Generator.Models` (another assembly).
