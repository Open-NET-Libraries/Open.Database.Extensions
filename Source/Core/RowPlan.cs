namespace Open.Database.Extensions;

/// <summary>
/// A prebuilt, read-only plan for mapping rows to <typeparamref name="T"/> when the columns are named differently from the properties.
/// Create it once, where it is declared, from key/value pairs of property name and column name, then pass it wherever
/// field-mapping overrides are accepted (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …) instead of repeating the pairs in every call.
/// </summary>
/// <remarks>
/// <para>
/// Create a plan with a collection expression of <see cref="KeyValuePair{TKey, TValue}"/> pairs, with the constructor, or by assigning an
/// <see cref="ImmutableArray{T}"/> of them to a <c>RowPlan&lt;T&gt;</c>. Every form validates where the plan is declared, not on the first query, and throws an
/// <see cref="ArgumentException"/> for: a property that does not exist or cannot be set, a null property name, a property named twice, an empty column name,
/// or two properties that would read the same column when at least one of them is named in the pairs (properties the model itself maps to the same column by name are left alone). Properties the plan does not mention are still matched to columns by name.
/// </para>
/// <para>
/// The property side of a pair is matched exactly (ordinal, case-sensitive). Column names are matched ignoring case, so two columns that differ only by case collide.
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
/// A null property name is an <see cref="ArgumentException"/> for a declared plan; the per-call overloads that take pairs or tuples (not plans) throw <see cref="ArgumentNullException"/> for it.
/// </para>
/// <para>
/// The implicit conversion from <see cref="ImmutableArray{T}"/> builds a plan (it validates and allocates), so use it only at a declaration;
/// no parameter of this library takes a <c>RowPlan&lt;T&gt;</c>, so a call site can never trigger it.
/// Passing a plan to a query for a different model type is accepted and treated as ordinary overrides: it throws an <see cref="ArgumentException"/>
/// when a pair with a non-null column names a property that model lacks, while a pair with a null column that names a missing property is ignored.
/// It does not get the precompiled speed.
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
/// For a one-off, pass the pairs inline. That builds a new plan and compiles the row reader on every call and caches nothing (about 0.43 ms and 29 KB for a one-row read of a 12-property model on the in-memory provider, against about 0.1 µs and a few hundred bytes through a declared plan), so use it only for a one-off:
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

	/// <summary>
	/// Creates the plan from (property name, column name) pairs and validates it, building its tables once. Use it where the plan is declared, never in a call:
	/// it allocates and validates every time it runs. The pairs are kept as they are (the array is shared, not copied).
	/// </summary>
	/// <param name="mappings">
	/// The (property name, column name) pairs. Properties not listed are matched to columns by name.
	/// A pair whose column is <see langword="null"/> means "do not map this property": it is not read at all, not even by name,
	/// so it keeps its default value even when the result set has a column with its name.
	/// </param>
	/// <exception cref="ArgumentException">
	/// A property does not exist or cannot be set; a property name is null; a property is listed twice; a column name is empty;
	/// two properties would read the same column (column names match ignoring case); or <paramref name="mappings"/> is the default <see cref="ImmutableArray{T}"/>.
	/// </exception>
	public RowPlan(ImmutableArray<KeyValuePair<string, string?>> mappings)
		: this(Declared(mappings), mappings, strict: true)
	{
	}

	/// <summary>Creates a plan from <paramref name="mappings"/>; see <see cref="RowPlan{T}(ImmutableArray{KeyValuePair{string, string}})"/>. Use it only at a declaration.</summary>
	/// <param name="mappings">The (property name, column name) pairs.</param>
	/// <exception cref="ArgumentException">A property does not exist or cannot be set; a property name is null; a property is listed twice; a column name is empty; two properties would read the same column; or <paramref name="mappings"/> is the default <see cref="ImmutableArray{T}"/>.</exception>
	[SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "The public constructor and RowPlan.Create are the named alternates.")]
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
	/// <exception cref="ArgumentException">A property does not exist or cannot be set; a property name is null; a property is listed twice; a column name is empty; or two properties would read the same column.</exception>
	public static RowPlan<T> Create<T>(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		where T : new()
		=> new(ImmutableArray.Create(mappings));
}
