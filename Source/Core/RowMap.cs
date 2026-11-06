namespace Open.Database.Extensions;

/// <summary>
/// A prebuilt, read-only mapping of the properties of <typeparamref name="T"/> to the columns that feed them, for when the columns are named
/// differently from the properties. Declare it once, from key/value pairs of property name and column name, and pass it as the <c>map</c>
/// argument of the query methods (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …) instead of repeating the mapping in every call.
/// </summary>
/// <remarks>
/// <para>
/// Create a map with a collection expression of <see cref="KeyValuePair{TKey, TValue}"/> pairs, with the constructor, or by assigning an
/// <see cref="ImmutableArray{T}"/> of pairs to a <c>RowMap&lt;T&gt;</c>. It is validated when it is created, and throws an <see cref="ArgumentException"/> for:
/// a property that does not exist or cannot be set (a static property, or one without a public setter), a null property name, a property named twice, an empty column name,
/// or two properties that would read the same column when at least one of them is named in the pairs (properties the model itself maps to the same column by name are left alone).
/// Properties the map does not mention are still matched to columns by name.
/// </para>
/// <para>
/// The property side of a pair is matched exactly (ordinal, case-sensitive). Column names are matched ignoring case, so two columns that differ only by case collide.
/// </para>
/// <para>
/// A <see langword="null"/> column means "do not map this property": the property is not read at all, not even by name,
/// so it keeps its default value even when the result set has a column with its name. The property must exist; the column need not.
/// Use it for a heavy column you do not want read, a name that collides with another mapping, or a property you fill in elsewhere.
/// </para>
/// <para>
/// A map is immutable and safe to share between threads, and any number of different maps may exist for the same model. Two maps built from the same
/// pairs behave identically. The map holds its compiled, typed row reader, built on first use for each result-set layout it meets (it keeps up to 128 layouts;
/// a map that meets more clears them when the cache fills, so queries with ever-changing layouts compile again). Passing a map therefore adds no per-call
/// allocation, key or dictionary lookup; as with <c>Map&lt;T&gt;()</c>, each query still matches the result-set layout against the cached one.
/// Nothing is registered anywhere; the first <c>RowMap&lt;T&gt;</c> for a model does initialize the library's per-model static state (a one-time reflection pass over the model's properties).
/// </para>
/// <para>
/// Declare each map once and reuse it. A collection expression or an <see cref="ImmutableArray{T}"/> written at a call site builds a new map, and compiles its row reader, on every call.
/// Because an application may have hundreds of models, create each map on first use so that only the ones actually used ever exist.
/// A reference write is atomic and publishing an object reference is a release in the .NET memory model
/// (<c>docs/design/specs/Memory-model.md</c> in dotnet/runtime: "Atomic memory accesses", "Object assignment", "Data-dependent reads are ordered", "Examples and common patterns"), so a thread that sees the
/// map sees it fully built. The one effect of two threads racing on first use is that two equivalent, immutable maps may be built.
/// </para>
/// <para>
/// A map with an error throws <see cref="ArgumentException"/> when it is first created: with <c>field ??=</c> that is at first use, and in a <c>static readonly</c> field it is at type initialization,
/// where it surfaces as a <see cref="TypeInitializationException"/> wrapping the <see cref="ArgumentException"/>.
/// </para>
/// <para>
/// A map enumerates the mapping it holds: every settable property of the model, in declaration order, with the column it reads (<see langword="null"/>: not mapped).
/// It is enumerable because the collection syntax requires it.
/// </para>
/// </remarks>
/// <example>
/// Created on first use with the C# 14 <c>field</c> keyword, one property per map, from a collection expression
/// (the property name is written with <c>nameof</c> so a rename is caught by the compiler):
/// <code>
/// static class Maps
/// {
///     public static RowMap&lt;Post&gt; PostMap =&gt; field ??= [new(nameof(Post.Id), "post_id"), new(nameof(Post.Title), "post_title")];
/// }
///
/// var posts = connection.Command("select * from posts").Map&lt;Post&gt;(Maps.PostMap).ToList();
/// </code>
/// To leave a property out, give it a <see langword="null"/> column. The property is not read, not even by name:
/// <code>
/// // Body keeps its default value even though the result set has a Body column (for example a heavy column you do not want read).
/// public static RowMap&lt;Post&gt; PostSummaryMap =&gt; field ??= [new(nameof(Post.Id), "post_id"), new(nameof(Post.Body), null)];
/// </code>
/// A <c>static readonly RowMap&lt;Post&gt; PostMap = [...]</c> field also works: it is built when its containing type's static initializer
/// first runs, together with the type's other static fields, so keep such maps in their own small static class.
/// For exactly one published instance use <c>LazyInitializer.EnsureInitialized(ref field, static () =&gt; [...])</c>
/// (the factory may still run more than once; it adds a method call per read).
/// Reading a held map allocates nothing and takes one to two nanoseconds whichever way it is held.
/// Compilers older than C# 14 need an explicit backing field: <c>static RowMap&lt;Post&gt;? _post; public static RowMap&lt;Post&gt; PostMap =&gt; _post ??= [...];</c>
/// </example>
/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
[CollectionBuilder(typeof(RowMap), nameof(RowMap.Create))]
public sealed partial class RowMap<T> : IEnumerable<KeyValuePair<string, string?>>, IRowSource<T>
	where T : new()
{
	/// <summary>
	/// Creates the map from (property name, column name) pairs and validates it, building its tables once. Use it where the map is declared, never in a call:
	/// it allocates and validates every time it runs.
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
	public RowMap(ImmutableArray<KeyValuePair<string, string?>> mappings)
		: this(Declared(mappings, nameof(mappings)), nameof(mappings))
	{
	}

	// For the callers that take their pairs in through a parameter of their own (the tuple convenience overloads): the exceptions name it.
	internal RowMap(ImmutableArray<KeyValuePair<string, string?>> mappings, string paramName)
		: this(Declared(mappings, paramName), paramName)
	{
	}

	/// <summary>Creates a map from <paramref name="mappings"/>; see <see cref="RowMap{T}(ImmutableArray{KeyValuePair{string, string}})"/>. Use it only at a declaration.</summary>
	/// <param name="mappings">The (property name, column name) pairs.</param>
	/// <exception cref="ArgumentException">A property does not exist or cannot be set; a property name is null; a property is listed twice; a column name is empty; two properties would read the same column; or <paramref name="mappings"/> is the default <see cref="ImmutableArray{T}"/>.</exception>
	[SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "The public constructor and RowMap.Create are the named alternates.")]
	public static implicit operator RowMap<T>(ImmutableArray<KeyValuePair<string, string?>> mappings)
		=> new(mappings);

	static ReadOnlySpan<KeyValuePair<string, string?>> Declared(ImmutableArray<KeyValuePair<string, string?>> mappings, string paramName)
	{
		if (mappings.IsDefault) throw new ArgumentException("The mappings array is the default (uninitialized) ImmutableArray.", paramName);
		Contract.EndContractBlock();

		return mappings.AsSpan();
	}

	// The collection syntax needs a type that can be enumerated. A map enumerates the mapping it holds: every settable property of the model,
	// in declaration order, with the column it reads (null: the property is not mapped). It keeps nothing but that table.
	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
	{
		for (int i = 0; i < Properties.Length; i++)
			yield return new(Properties[i].Name, _columns[i]);
	}

	IEnumerator IEnumerable.GetEnumerator()
		=> ((IEnumerable<KeyValuePair<string, string?>>)this).GetEnumerator();
}

/// <summary>
/// The builder behind the collection syntax of <see cref="RowMap{T}"/>: <c>RowMap&lt;Post&gt; map = [new(nameof(Post.Id), "post_id")];</c>.
/// </summary>
public static class RowMap
{
	/// <summary>Creates and validates a <see cref="RowMap{T}"/> from (property name, column name) pairs. This is what a collection expression of pairs calls.</summary>
	/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
	/// <param name="mappings">The (property name, column name) pairs. A pair whose column is <see langword="null"/> means "do not map this property".</param>
	/// <returns>The validated, read-only map.</returns>
	/// <exception cref="ArgumentException">A property does not exist or cannot be set; a property name is null; a property is listed twice; a column name is empty; or two properties would read the same column.</exception>
	public static RowMap<T> Create<T>(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		where T : new()
		=> new(ImmutableArray.Create(mappings));
}
