﻿namespace Open.Database.Extensions;

/// <summary>
/// A predefined, read-only mapping of the properties of <typeparamref name="T"/> to the columns that feed them.
/// Define it once and pass it wherever field-mapping overrides are accepted
/// (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …) instead of repeating the overrides in every call.
/// </summary>
/// <remarks>
/// <para>
/// A mapping is checked where it is built: a property that does not exist or cannot be set, a property named twice,
/// or two properties that would read the same column throw an <see cref="ArgumentException"/> immediately,
/// not on the first query. Properties it does not mention are still matched to columns by name, and a <see langword="null"/>
/// column ignores the property.
/// </para>
/// <para>
/// Nothing is registered anywhere; the first <c>RowMap&lt;T&gt;</c> for a model does initialize the library's per-model static state
/// (a one-time reflection pass over the model's properties). A mapping is immutable and safe to share
/// between threads, and any number of different mappings may exist for the same model. Two mappings built from the same
/// pairs behave identically. It holds its own compiled, typed row reader (built on first use for each result-set layout
/// it meets), so passing it adds no per-call allocation, key or dictionary lookup; as with <c>Map&lt;T&gt;()</c>, each query
/// still matches the result-set layout against the cached one.
/// </para>
/// <para>
/// Because an application may have hundreds of models, create each mapping on first use so that only the ones
/// actually used ever exist. A reference write is atomic and publishing an object reference is a release in the .NET memory model
/// (<c>docs/design/specs/Memory-model.md</c> in dotnet/runtime: "Atomic memory accesses", "Object assignment", "Data-dependent reads are ordered", "Examples and common patterns"), so a thread that sees the
/// mapping sees it fully built. The one effect of two threads racing on first use is that two equivalent, immutable mappings may be built.
/// </para>
/// <para>
/// A mapping with an error throws <see cref="ArgumentException"/> where it is built. With <c>field ??=</c> that is the first use;
/// in a <c>static readonly</c> field it surfaces as a <see cref="TypeInitializationException"/> wrapping the <see cref="ArgumentException"/>.
/// </para>
/// <para>
/// Passing a mapping to a query for a different model type is accepted and treated as ordinary overrides:
/// it works if that type has the same settable properties and throws an <see cref="ArgumentException"/> if it does not,
/// but it does not get the precompiled speed.
/// </para>
/// </remarks>
/// <example>
/// Created on first use with the C# 14 <c>field</c> keyword, one property per mapping:
/// <code>
/// static class Maps
/// {
///     public static RowMap&lt;Post&gt; Post =&gt; field ??= new(("Id", "post_id"), ("Title", "post_title"), ("Body", null));
///     public static RowMap&lt;Customer&gt; Customer =&gt; field ??= new(("Id", "customer_id"), ("Name", "full_name"));
/// }
///
/// var posts = connection.Command("select * from posts").Map&lt;Post&gt;(Maps.Post).ToList();
/// </code>
/// A <c>static readonly RowMap&lt;Post&gt; PostMap = new(...)</c> field also works: it is built when its containing type's static initializer
/// first runs, together with the type's other static fields, so keep such mappings in their own small static class.
/// For exactly one published instance use <c>LazyInitializer.EnsureInitialized(ref field, static () =&gt; new(...))</c>
/// (the factory may still run more than once; it adds a method call per read).
/// Compilers older than C# 14 need an explicit backing field: <c>static RowMap&lt;Post&gt;? _post; public static RowMap&lt;Post&gt; Post =&gt; _post ??= new(...);</c>
/// </example>
/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
public sealed class RowMap<T> : IEnumerable<KeyValuePair<string, string?>>, IEnumerable<(string Field, string? Column)>
	where T : new()
{
	readonly ImmutableArray<KeyValuePair<string, string?>> _pairs;

	// The shared compiled mapper; an override parameter that receives this object uses it as is.
	internal RowPlan<T> Plan { get; }

	/// <summary>
	/// Builds the mapping from field (property) names to column names. A <see langword="null"/> column ignores that field.
	/// </summary>
	/// <param name="mappings">The (field, column) pairs. Properties not listed are matched to columns by name.</param>
	/// <exception cref="ArgumentException">A field does not exist, cannot be set, is listed twice, or shares its column with another property.</exception>
	public RowMap(params IEnumerable<(string Field, string? Column)> mappings)
	{
		if (mappings is null) throw new ArgumentNullException(nameof(mappings));
		Contract.EndContractBlock();

		ImmutableArray<KeyValuePair<string, string?>>.Builder builder = ImmutableArray.CreateBuilder<KeyValuePair<string, string?>>();
		foreach ((string field, string? column) in mappings)
			builder.Add(new(field, column));

		_pairs = builder.DrainToImmutable();
		Plan = RowPlan<T>.Validated(_pairs.AsSpan());
	}

	// Explicit on purpose: the pairs are for the override parameters to consume, not a collection to browse.
	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
		=> ((IEnumerable<KeyValuePair<string, string?>>)_pairs).GetEnumerator();

	IEnumerator<(string Field, string? Column)> IEnumerable<(string Field, string? Column)>.GetEnumerator()
	{
		foreach (KeyValuePair<string, string?> pair in _pairs)
			yield return (pair.Key, pair.Value);
	}

	IEnumerator IEnumerable.GetEnumerator()
		=> ((IEnumerable<KeyValuePair<string, string?>>)_pairs).GetEnumerator();
}
