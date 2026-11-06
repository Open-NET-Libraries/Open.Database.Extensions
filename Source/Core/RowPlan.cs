namespace Open.Database.Extensions;

/// <summary>
/// A prebuilt, read-only plan for mapping rows to <typeparamref name="T"/> when the columns are named differently from the properties.
/// It is created once, where it is declared, from key/value pairs of property name and column name, and then passed wherever
/// field-mapping overrides are accepted (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …) instead of repeating the pairs in every call.
/// </summary>
/// <remarks>
/// <para>
/// A plan is created from a collection expression of <see cref="KeyValuePair{TKey, TValue}"/> pairs, or by assigning an
/// <see cref="ImmutableArray{T}"/> of them to a <c>RowPlan&lt;T&gt;</c>. Either way it is checked where it is declared:
/// a property that does not exist or cannot be set, a property named twice, an empty column name, or two properties that would read the same column
/// throw an <see cref="ArgumentException"/> immediately, not on the first query. Properties it does not mention are still matched to columns by name.
/// </para>
/// <para>
/// A <see langword="null"/> column means "do not map this property": the property is not read at all, not even by name,
/// so it keeps its default value even when the result set has a column with its name. Use it for a heavy column you do not want read,
/// a name that collides with another mapping, or a property you fill in elsewhere.
/// </para>
/// <para>
/// Nothing is registered anywhere; the first <c>RowPlan&lt;T&gt;</c> for a model does initialize the library's per-model static state
/// (a one-time reflection pass over the model's properties). A plan is immutable and safe to share
/// between threads, and any number of different plans may exist for the same model. Two plans built from the same
/// pairs behave identically. The plan holds its compiled, typed row reader (built on first use for each result-set layout
/// it meets), so passing it adds no per-call allocation, key or dictionary lookup; as with <c>Map&lt;T&gt;()</c>, each query
/// still matches the result-set layout against the cached one.
/// </para>
/// <para>
/// Because an application may have hundreds of models, create each plan on first use so that only the ones
/// actually used ever exist. A reference write is atomic and publishing an object reference is a release in the .NET memory model
/// (<c>docs/design/specs/Memory-model.md</c> in dotnet/runtime: "Atomic memory accesses", "Object assignment", "Data-dependent reads are ordered", "Examples and common patterns"), so a thread that sees the
/// plan sees it fully built. The one effect of two threads racing on first use is that two equivalent, immutable plans may be built.
/// </para>
/// <para>
/// A plan with an error throws <see cref="ArgumentException"/> where it is declared. With <c>field ??=</c> that is the first use;
/// in a <c>static readonly</c> field it surfaces as a <see cref="TypeInitializationException"/> wrapping the <see cref="ArgumentException"/>.
/// </para>
/// <para>
/// The implicit conversion from <see cref="ImmutableArray{T}"/> builds a plan (it validates and allocates), so use it only at a declaration;
/// no parameter of this library takes a <c>RowPlan&lt;T&gt;</c>, so a call site can never trigger it.
/// Passing a plan to a query for a different model type is accepted and treated as ordinary overrides:
/// it works if that type has the same settable properties and throws an <see cref="ArgumentException"/> if it does not,
/// but it does not get the precompiled speed.
/// </para>
/// </remarks>
/// <example>
/// Created on first use with the C# 14 <c>field</c> keyword, one property per plan, from a collection expression
/// (the property name is written with <c>nameof</c> so a rename is caught by the compiler):
/// <code>
/// static class Plans
/// {
///     public static RowPlan&lt;Post&gt; PostPlan =&gt; field ??= [new(nameof(Post.Id), "post_id"), new(nameof(Post.Title), "post_title")];
/// }
///
/// var posts = connection.Command("select * from posts").Map&lt;Post&gt;(Plans.PostPlan).ToList();
/// </code>
/// To leave a property out, give it a <see langword="null"/> column. The property is not read, not even by name:
/// <code>
/// // Body keeps its default value even though the result set has a Body column (for example a heavy column you do not want read).
/// public static RowPlan&lt;Post&gt; PostSummaryPlan =&gt; field ??= [new(nameof(Post.Id), "post_id"), new(nameof(Post.Body), null)];
/// </code>
/// For a one-off, pass the pairs inline. That builds a new plan and compiles the row reader on every call and caches nothing (about 30 KB for a one-row read, against a few hundred bytes through a declared plan), so use it only for a one-off:
/// <code>
/// connection.Command("select * from posts").Map&lt;Post&gt;(new(nameof(Post.Id), "post_id"), new(nameof(Post.Body), null)).ToList();
/// </code>
/// A <c>static readonly RowPlan&lt;Post&gt; PostPlan = [...]</c> field also works: it is built when its containing type's static initializer
/// first runs, together with the type's other static fields, so keep such plans in their own small static class.
/// For exactly one published instance use <c>LazyInitializer.EnsureInitialized(ref field, static () =&gt; [...])</c>
/// (the factory may still run more than once; it adds a method call per read).
/// Reading a held plan allocates nothing and takes one to two nanoseconds whichever way it is held (see Benchmarks/RESULTS.md).
/// Compilers older than C# 14 need an explicit backing field: <c>static RowPlan&lt;Post&gt;? _post; public static RowPlan&lt;Post&gt; PostPlan =&gt; _post ??= [...];</c>
/// </example>
/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
[CollectionBuilder(typeof(RowPlan), nameof(RowPlan.Create))]
public sealed partial class RowPlan<T> : IEnumerable<KeyValuePair<string, string?>>, IRowSource<T>
	where T : new()
{
	// The pairs the plan was declared with: the one retained copy, shared with the array the caller built (never copied from it).
	// It exists so the plan can be enumerated by the override parameters that take pairs; a plan built for per-call overrides holds none.
	readonly ImmutableArray<KeyValuePair<string, string?>> _pairs;

	// Not public: a plan is created by the conversion below or by a collection expression, and nowhere else.
	internal RowPlan(ImmutableArray<KeyValuePair<string, string?>> mappings)
		: this(Declared(mappings), mappings, strict: true)
	{
	}

	/// <summary>
	/// Creates the plan from (property name, column name) pairs and validates it. Use it where the plan is declared, never in a call:
	/// it allocates and validates every time it runs.
	/// </summary>
	/// <param name="mappings">
	/// The (property name, column name) pairs. Properties not listed are matched to columns by name.
	/// A pair whose column is <see langword="null"/> means "do not map this property": it is not read at all, not even by name,
	/// so it keeps its default value even when the result set has a column with its name.
	/// </param>
	/// <exception cref="ArgumentException">A property does not exist, cannot be set, is listed twice, has an empty column name, or shares its column with another property; or <paramref name="mappings"/> is the default <see cref="ImmutableArray{T}"/>.</exception>
	public static implicit operator RowPlan<T>(ImmutableArray<KeyValuePair<string, string?>> mappings)
		=> new(mappings);

	static ReadOnlySpan<KeyValuePair<string, string?>> Declared(ImmutableArray<KeyValuePair<string, string?>> mappings)
	{
		if (mappings.IsDefault) throw new ArgumentException("The mappings array is the default (uninitialized) ImmutableArray.", nameof(mappings));
		Contract.EndContractBlock();

		return mappings.AsSpan();
	}

	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
		=> ((IEnumerable<KeyValuePair<string, string?>>)_pairs).GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator()
		=> ((IEnumerable<KeyValuePair<string, string?>>)_pairs).GetEnumerator();
}

/// <summary>
/// The builder behind the collection syntax of <see cref="RowPlan{T}"/>: <c>RowPlan&lt;Post&gt; plan = [new(nameof(Post.Id), "post_id")];</c>.
/// </summary>
public static class RowPlan
{
	/// <summary>Creates and validates a <see cref="RowPlan{T}"/> from (property name, column name) pairs. This is what a collection expression of pairs calls.</summary>
	/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
	/// <param name="mappings">The (property name, column name) pairs. A pair whose column is <see langword="null"/> means "do not map this property".</param>
	/// <returns>The validated, read-only plan.</returns>
	/// <exception cref="ArgumentException">A property does not exist, cannot be set, is listed twice, has an empty column name, or shares its column with another property.</exception>
	public static RowPlan<T> Create<T>(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		where T : new()
		=> new(ImmutableArray.Create(mappings));
}
