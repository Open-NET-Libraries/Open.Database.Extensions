
namespace Open.Database.Extensions.Core;

/// <summary>
/// Transforms retrieved data into models of type <typeparamref name="T"/>.
/// </summary>
/// <remarks>
/// <para>
/// The mapping is a compiled, cached <see cref="RowMap{T}"/>:
/// constructing a transformer and applying it to a result does not rebuild lookups or property setters per query.
/// </para>
/// <para>
/// A model is built as <c>Map&lt;T&gt;()</c> builds it:
/// a type with a public parameterless constructor with <c>new T()</c>, then its properties are set;
/// any other type through its one public constructor, then the properties it does not cover.
/// A type that cannot be mapped throws <see cref="InvalidOperationException"/> when the transformer is created (see <see cref="RowMap{T}"/>).
/// </para>
/// </remarks>
/// <typeparam name="T">The type of the model to create from the data.</typeparam>
public class Transformer<T>
{

	/// <summary>
	/// The maximum number of arrays to hold in the local array pool per bucket.
	/// May also define how many records are pre-buffered before transforming.
	/// </summary>
	protected const int MaxArrayBuffer = 1024;

	/// <summary>
	/// The pool of buffers for transforming.
	/// </summary>
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter.", Justification = "<Pending>")]
	protected internal static readonly ArrayPool<object> LocalPool = ArrayPool<object>.Create(1024, MaxArrayBuffer);

	readonly RowMap<T> _plan;

	/// <summary>
	/// Finds the columns of the record that feed a property.
	/// </summary>
	/// <remarks>
	/// The columns are returned as their ordinals in ascending order and, aligned with them, their names,
	/// matched exactly as a row read matches them (exactly, ignoring case, then ignoring underscores).
	/// </remarks>
	/// <param name="record">The record whose columns are matched.</param>
	/// <param name="ordinals">The ordinals of the columns that feed a property, in ascending order.</param>
	/// <param name="names">The names of those columns, aligned with <paramref name="ordinals"/>.</param>
	internal void MatchingColumns(IDataRecord record, out int[] ordinals, out ImmutableArray<string> names) => _plan.MatchingColumns(record, out ordinals, out names);

	/// <summary>
	/// Initializes a new instance of the <see cref="Transformer{T}"/> class using the optional map.
	/// </summary>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='map']/*" />
	protected internal Transformer(RowMap<T>? map = null)
		=> _plan = map ?? RowMap<T>.Default;

	/// <summary>
	/// Processes the results of the transformer.
	/// </summary>
	/// <param name="transformer">The transformer to use.</param>
	protected class Processor(Transformer<T> transformer)
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="Processor"/> class for the named columns.
		/// </summary>
		/// <param name="transformer">The transformer to use.</param>
		/// <param name="names">The names of columns/properties to acquire.</param>
		public Processor(Transformer<T> transformer, ImmutableArray<string> names)
			: this(transformer) => SetNames(names);

		/// <summary>
		/// Gets the transformer being used.
		/// </summary>
		/// <value>The transformer the processor was created with.</value>
		public Transformer<T> Transformer { get; } = transformer;

		/// <summary>
		/// Gets the resultant transform function.
		/// </summary>
		/// <value>A function that creates a <typeparamref name="T"/> from an array of column values.</value>
		/// <remarks>
		/// Until <see cref="SetNames"/> gives it columns, it builds <typeparamref name="T"/> from no columns:
		/// its properties keep their initializers and its constructor parameters take their declared defaults.
		/// It is built from the map on first use, and only when no names were set;
		/// a static default could not reach the instance's map, and <c>new T()</c> no longer compiles.
		/// </remarks>
		public Func<object?[], T> Transform
		{
			get => field ??= Transformer._plan.ArrayMapper([]);
			private set;
		}

		/// <summary>
		/// Sets the columns to process, which allows for deferred initialization.
		/// </summary>
		/// <param name="names">The column/property names to process.</param>
		public void SetNames(ImmutableArray<string> names)
			=> Transform = Transformer._plan.ArrayMapper(names);
	}
}
