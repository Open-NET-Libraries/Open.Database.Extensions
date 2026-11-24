# Open.Database.Extensions.Generator (unpackaged prototype)

A Roslyn incremental source generator that serves a plain `Map<T>()` with generated row readers, through C# interceptors.
It is an **unpackaged prototype**: not packaged, not referenced by Core, and Core builds, tests and ships without it.

## What it does

For each call `x.Map<T>()` with a closed model type `T`, where `x` is an `IExecuteReader`, an `IDbCommand` or an `IDataReader`,
it emits an interceptor that calls the existing public overload `Map(Func<IDataRecord, T>)` with a generated row reader.
The generated reader follows the runtime `RowMap<T>` rules. Anything it does not reproduce exactly is handed to the runtime map (see "Declines to the runtime").

- **NULL.** As the runtime map: a member declared non-nullable (a value type other than `Nullable<T>`, a reference annotated non-nullable) is read
  unchecked, with its typed getter alone, so a NULL there throws (the provider's getter); a member read by `GetValue` (no typed getter for its type)
  makes the runtime's check and throws `SqlNullValueException`. A nullable member is tested with `IsDBNull` and gets null.
- **Errors.** As the runtime map: a failed row is a `ValueMappingException` naming the model type (`Type.Name`) and the 0-based row, the exception inside,
  in Core's words (chosen by the type of the exception inside); no member or column is named. A `DbException` or a cancellation passes through untouched.

## Known limits

- **Wrong values, silently, after `NextResult` with the same column count.** The binding is per reader and is renewed only when the reader's column count
  changes. On the same reader, a query value kept and run again after `NextResult` (`var q = reader.Map<T>(); q.ToList(); reader.NextResult(); q.ToList();`)
  whose second set has the same column count but another layout (other column order or names) reads the second set at the FIRST set's ordinals: values
  land in the wrong members, or a cast error is thrown, where the runtime map re-checks the layout per result set and reads it right.
  The row index in an error report also continues from the first set instead of restarting at 0 (a different column count restarts it).
  Neither can be detected without a check on every row, which this generator does not make; a per-result-set hook in Core would close both.
  Pinned by tests.
- **The row after `Skip(n)`.** Core skips rows without calling the row reader, so the generated report counts only the rows it mapped: n lower than the
  runtime's row. Pinned by a test; a hook in Core would close it.
- **NULL policies are not generated.** A model with `NullPolicy` or `NullValue` is declined to the runtime map (below) until a later change teaches the
  generator those attributes.
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
A model that declares a NULL policy is not intercepted either, so the policy is never silently ignored: `NullPolicy` on the type or a base type, `NullPolicy`
or `NullValue` on a property or on a constructor or positional record parameter.

Per result set, handed to the runtime map: a column whose type has no exact or converting typed read, declared columns that do not match (the runtime then throws its own exception),
and a layout that leaves an init-only or required member without a column. Such a result set reports a failed row as the runtime map does, with its row.

## Tests

`Tests/Open.Database.Extensions.Generator.Tests` compares every intercepted call with `Map(RowMap<T>.Default)` on the same data, on the twin models of
`Tests/Open.Database.Extensions.Generator.Models` (another assembly), and the policy models of its own assembly (the models library references nothing of ODX).
