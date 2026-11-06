namespace Open.Database.Extensions;

/// <summary>
/// A validated, read-only mapping of the properties of <typeparamref name="T"/> to the columns that feed them, for when the columns are named differently from the properties.
/// Declare it once and pass it as the <c>map</c> argument of the query methods (<c>Map&lt;T&gt;</c>, <c>Results&lt;T&gt;</c>, <c>ToChannel</c>, …).
/// </summary>
/// <remarks>
/// Properties the map does not mention still map by name; a <see langword="null"/> column means "do not map this property".
/// The pairs are validated when the map is constructed: an unknown or unsettable property, a property named twice, an empty column name,
/// or two properties reading one column throw <see cref="ArgumentException"/>.
/// A map is immutable and thread-safe, and caches its compiled row readers. One built at a call site is rebuilt on every call, so declare it once.
/// </remarks>
/// <example>
/// <code>
/// public static RowMap&lt;Post&gt; PostMap =&gt; field ??= new([new(nameof(Post.Id), "post_id"), new(nameof(Post.Title), "post_title")]);
///
/// var posts = connection.Command("select * from posts").Map&lt;Post&gt;(PostMap).ToList();
/// </code>
/// </example>
/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
public sealed partial class RowMap<T> : IRowSource<T>
	where T : new()
{
	/// <summary>Creates and validates the map from (property name, column name) pairs; use it where the map is declared, never in a call.</summary>
	/// <param name="mappings">The (property name, column name) pairs; a <see langword="null"/> column means "do not map this property".</param>
	/// <exception cref="ArgumentException">A pair is invalid (see <see cref="RowMap{T}"/>), or <paramref name="mappings"/> is the default <see cref="ImmutableArray{T}"/>.</exception>
	public RowMap(ImmutableArray<KeyValuePair<string, string?>> mappings)
		: this(mappings, nameof(mappings))
	{
	}
}
