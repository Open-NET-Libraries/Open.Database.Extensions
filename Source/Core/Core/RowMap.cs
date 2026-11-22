using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// Represents a validated, read-only mapping of the properties and constructor parameters of <typeparamref name="T"/> to the columns that feed them.
/// </summary>
/// <remarks>
/// <para>
/// Use a map when the columns are named differently from the members.
/// Declare it once and pass it as the <c>map</c> argument of the query methods (<c>Map&lt;T&gt;</c>, <c>ToChannel&lt;T&gt;</c>, …).
/// A map is immutable and thread-safe, and caches its compiled row readers. One built at a call site is rebuilt on every call, so declare it once.
/// Declare it with the collection syntax, <c>RowMap&lt;Post&gt; map = [new(nameof(Post.Id), "post_id")];</c>, or with the constructor, from one pair or several; a map enumerates the pairs it was declared with.
/// </para>
/// <para>
/// Properties the map does not mention still map by name: a column feeds the property of that name exactly, else ignoring case, else ignoring case and underscores (<c>q_type</c> feeds <c>QType</c>).
/// A property with a pair is only matched by its column, exactly or ignoring case; a <see langword="null"/> column means "do not map this property".
/// </para>
/// <para>
/// A type with no public parameterless constructor is built through its one public constructor (a positional record, a record struct, a class with one constructor), and each parameter is fed like a property.
/// A parameter and a public property whose names differ only in case are one member, fed once, through the constructor, and a pair names it by the property's name or by the parameter's, ignoring case.
/// A parameter no column feeds, or a <see langword="null"/> column ignores, receives its declared default value, else <see langword="default"/>; so does a parameter the type declares non-nullable when the cell is NULL.
/// </para>
/// <para>
/// A <see langword="required"/> property must be fed by a column in every result shape, and a constructor parameter's default never feeds it:
/// when none does, building the row reader throws <see cref="ValueMappingException"/> (<see cref="ValueMappingException.ColumnName"/> is the column looked for),
/// which happens when the first row of that shape is mapped, so a query with no rows never reports it
/// (the buffered routes and the Channel and Dataflow methods know the columns before any row, and report it as soon as they have them).
/// A constructor marked <c>SetsRequiredMembers</c> promises to set them: nothing is enforced then, and they map like any other property.
/// </para>
/// <para>
/// The pairs are validated when the map is constructed: an unknown or unsettable property or constructor parameter, a member named twice, an empty column name, a <see langword="null"/> column for a required property,
/// or two members reading one column throw <see cref="ArgumentException"/>.
/// </para>
/// <para>
/// A type that cannot be mapped throws <see cref="InvalidOperationException"/> when the map is constructed or <see cref="Default"/> is read, which every query method does at the call, even when the result is empty:
/// </para>
/// <list type="bullet">
/// <item><description>an interface, an abstract class or a delegate;</description></item>
/// <item><description>a single value such as <see cref="int"/> or <see cref="string"/> (use <c>Map&lt;T&gt;()</c> on a command to read one column);</description></item>
/// <item><description>a dictionary or a dynamic object (<c>Dictionary&lt;string, object?&gt;</c>, <see cref="System.Dynamic.ExpandoObject"/>: they have no properties to set, so use <c>Map(selector)</c>);</description></item>
/// <item><description>a type with two or more public constructors and none without parameters;</description></item>
/// <item><description>a class with no public constructor;</description></item>
/// <item><description>a type with a <see langword="required"/> public field (a field is never mapped, so it cannot be set), unless the constructor it is built through is marked <c>[SetsRequiredMembers]</c>;</description></item>
/// <item><description>a type whose <c>[Column]</c> and <c>[NotMapped]</c> declarations cannot all be honoured (see below).</description></item>
/// </list>
/// <para>
/// A NULL never overwrites a member the C# type declares non-nullable: a value type other than <see cref="Nullable{T}"/> (an enum counts), or a reference type with nullable annotations on (<c>string</c>, not <c>string?</c>).
/// A property of such a type keeps the value its constructor or initializer gave it, else its default; a constructor parameter receives its declared default value, else <see langword="default"/>.
/// Anything else receives <see langword="null"/>: <see cref="Nullable{T}"/>, a reference type declared nullable (<c>string?</c>, or <c>[AllowNull]</c> on a property), a reference type in code with nullable annotations off, and a member declared as a type parameter.
/// The annotations are read from the assembly the model is declared in, once per model type.
/// </para>
/// <para>
/// A member that declares its column is a hard instruction, and a result that does not say the same throws.
/// The attributes are those of <c>System.ComponentModel.DataAnnotations.Schema</c>, read by full name:
/// </para>
/// <list type="bullet">
/// <item><description><c>[Column("name")]</c> reads that column (exactly, else ignoring case; never inferred), and none in the result throws <see cref="ValueMappingException"/>;</description></item>
/// <item><description><c>[Column(Order = n)]</c> says column <c>n</c> is the member's (its declared name, else its own name, exactly or ignoring case, never ignoring underscores), and another column there, or too few columns, throws <see cref="ValueMappingException"/> naming the member, the ordinal and what is there;</description></item>
/// <item><description><c>[NotMapped]</c> leaves the member out: no column feeds it, whatever its name, unless a pair names it.</description></item>
/// </list>
/// <para>
/// These are checked once per result set, when the row reader is built, never per row, and a result set that fails is checked again the next time it comes back.
/// A <c>[NotMapped]</c> member that is also <c>[Column]</c>, required, or set by a constructor parameter, two members that declare the same ordinal or name, and a declaration with an empty name or a negative ordinal are a type error (<see cref="InvalidOperationException"/>).
/// A pair that names a declared member overrides its declarations for that map:
/// it reads the pair's column, a <see langword="null"/> column leaves it unread, and a <c>[NotMapped]</c> member it names is mapped; its own <c>Name</c> and <c>Order</c> checks do not apply.
/// A type error above still throws whatever the map says, and the other members keep their declarations.
/// A NULL follows the NULL rules above, declared or not.
/// </para>
/// <para>
/// A column that needs a trim, a parse or a format on its way to a member has a converter:
/// <c>map.Convert&lt;long, string&gt;(nameof(Order.Number), n =&gt; n.ToString("D6"))</c> returns a new map (this one is unchanged)
/// in which the member receives what the delegate makes of its column, and every other member still maps by name.
/// The delegate is called inside the compiled row reader, on the value read with the typed getter when the column is a <c>TColumn</c> (nothing is boxed),
/// else read the way <c>Map&lt;int&gt;()</c> reads a single value: an integer converts only within the range of <c>TColumn</c>, else the row throws <see cref="ValueMappingException"/>.
/// </para>
/// <para>
/// A converter is not called for a NULL cell, and the member follows the NULL rules above:
/// <see langword="null"/> for a nullable member, its own value (a property) or its declared default (a constructor parameter) for a non-nullable one.
/// A converter that must see NULL names a <see cref="Nullable{T}"/> as <c>TColumn</c> (<c>int?</c>, <c>DateTime?</c>): it is called with <see langword="null"/>,
/// and what it returns is the member's value, a non-nullable member's included.
/// A reference type never receives <see langword="null"/> (<c>string?</c> says nothing at run time, where the annotation does not exist), so a converter of text sees only text.
/// A converter is not a pair: a map enumerates the pairs it was declared with and the columns <c>Convert</c> was given, never the delegates.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public static RowMap&lt;Post&gt; PostMap
///     =&gt; field ??=
///     [
///         new(nameof(Post.Id), "post_id"),
///         new(nameof(Post.Title), "post_title")
///     ];
///
/// var posts = await connection
///     .Command("select * from posts")
///     .Map&lt;Post&gt;(PostMap)
///     .ToListAsync(cancellationToken);
/// </code>
/// </example>
/// <typeparam name="T">The model type whose properties and constructor parameters are mapped.</typeparam>
[CollectionBuilder(typeof(RowMap), nameof(RowMap.Create))]
public sealed class RowMap<T> : IEnumerable<KeyValuePair<string, string?>>, IRowSource<T>
{
	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class from one (property name, column name) pair, so <c>new RowMap&lt;T&gt;(new("A", "b"))</c> compiles.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='declare-once']/*" />
	/// </remarks>
	/// <param name="mapping">The (property name, column name) pair; a <see langword="null"/> column means "do not map this property".</param>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	[OverloadResolutionPriority(1)]
	public RowMap(KeyValuePair<string, string?> mapping) : this([mapping]) { }

	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class from (property name, column name) pairs.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='declare-once']/*" />
	/// </remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mappings']/*" />
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	public RowMap(params ReadOnlySpan<KeyValuePair<string, string?>> mappings)
	{
		// A model type that cannot be mapped fails here, at the call, not when the first row is read.
		ThrowIfUnmappable();
		_slots = ModelSlots;
		_slotIndex = ModelSlotIndex;

		// slot → column, in slot order; a slot reads the column its member declares ([Column(Name)]), else the column of its own name, unless a pair says otherwise.
		// A [NotMapped] member reads none (null) unless a pair names it.
		string?[] columns = new string?[_slots.Length];
		for (int i = 0; i < columns.Length; i++) columns[i] = _slots[i].NotMapped ? null : _slots[i].ColumnName ?? _slots[i].Name;

		bool[] named = Declare(mappings, columns, nameof(mappings));
		RejectSharedColumns(columns, named, nameof(mappings));

		_columns = columns;
		_named = named;
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class from (property name, column name) pairs held in an <see cref="ImmutableArray{T}"/>.
	/// </summary>
	/// <remarks>
	/// A default array is no mappings.
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='declare-once']/*" />
	/// </remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mappings']/*" />
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	[OverloadResolutionPriority(-1)]
	public RowMap(ImmutableArray<KeyValuePair<string, string?>> mappings) : this(mappings.AsSpan()) { }

	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class from (property name, column name) pairs.
	/// </summary>
	/// <remarks>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='declare-once']/*" />
	/// </remarks>
	/// <param name="mappings">The (property name, column name) pairs, or <see langword="null"/> for none; a <see langword="null"/> column means "do not map this property".</param>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	/// <exception cref="ArgumentException"><paramref name="mappings"/> is a factory map, which cannot be rebuilt from its pairs: its function is not among them. Use the map itself.</exception>
	[OverloadResolutionPriority(-2)]
	public RowMap(IEnumerable<KeyValuePair<string, string?>>? mappings) : this(GetMappingSpan(mappings)) { }

	private static ReadOnlySpan<KeyValuePair<string, string?>> GetMappingSpan(IEnumerable<KeyValuePair<string, string?>>? mappings) => mappings switch
	{
		null => default,
		KeyValuePair<string, string?>[] array => array,
		ImmutableArray<KeyValuePair<string, string?>> ia => ia.AsSpan(),
		// The pairs of a factory map do not hold its function: a map rebuilt from them would be a map of the model's own members, silently.
		RowMap<T> { _factory: not null } => throw new ArgumentException("A factory map cannot be rebuilt from its pairs: its function is not among them. Use the map itself.", nameof(mappings)),
		_ => mappings.ToArray()
	};

	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class for what <see cref="From(Delegate, ReadOnlySpan{KeyValuePair{string, string?}})"/> makes.
	/// </summary>
	/// <remarks>
	/// The slots are the factory's parameters, so a type with several constructors, which no constructor map accepts, can be built through one;
	/// a single value is no row model either way.
	/// </remarks>
	/// <param name="factory">The delegate that builds a <typeparamref name="T"/>.</param>
	/// <param name="pairs">The (parameter name, column name) pairs.</param>
	RowMap(Delegate factory, ReadOnlySpan<KeyValuePair<string, string?>> pairs)
	{
		if (Shape.IsScalar(typeof(T))) TypeShape.ThrowIfUnmappable();

		_factory = factory;
		_slots = BuildFactorySlots(factory);
		_slotIndex = BuildSlotIndex(_slots);

		string?[] columns = new string?[_slots.Length];
		for (int i = 0; i < columns.Length; i++) columns[i] = _slots[i].ColumnName ?? _slots[i].Name;

		bool[] named = Declare(pairs, columns, nameof(pairs));
		RejectSharedColumns(columns, named, nameof(pairs));

		_columns = columns;
		_named = named;
	}

	/// <summary>
	/// Builds one slot per parameter of the factory.
	/// </summary>
	/// <remarks>
	/// The parameters are read from the method behind the delegate (a lambda's parameters keep their names and their nullable annotations there), as a constructor's are.
	/// A delegate bound to its first argument has one parameter less than its method: the delegate type's own <c>Invoke</c> names them then.
	/// </remarks>
	/// <param name="factory">The delegate that builds a <typeparamref name="T"/>.</param>
	/// <returns>The slots, in the order of the parameters.</returns>
	/// <exception cref="ArgumentException">The factory does not return a <typeparamref name="T"/>, a parameter is passed by reference or is a pointer, or two parameters share a name.</exception>
	static ImmutableArray<Slot> BuildFactorySlots(Delegate factory)
	{
		MethodInfo invoke = factory.GetType().GetMethod("Invoke")!;
		if (invoke.ReturnType == typeof(void) || !typeof(T).IsAssignableFrom(invoke.ReturnType))
			throw new ArgumentException($"The factory returns {Shape.TypeName(invoke.ReturnType)}, which cannot be assigned to {Shape.TypeName(typeof(T))}.", nameof(factory));

		ParameterInfo[] parameters = invoke.GetParameters();
		ParameterInfo[] declared = factory.Method.GetParameters();
		if (declared.Length == parameters.Length) parameters = declared;

		var seen = new HashSet<string>(StringComparer.Ordinal);
		var list = new List<Slot>(parameters.Length);
		for (int i = 0; i < parameters.Length; i++)
		{
			ParameterInfo parameter = parameters[i];
			string name = parameter.Name ?? "arg" + i;
			if (parameter.ParameterType.IsByRef || parameter.ParameterType.IsPointer)
				throw new ArgumentException($"The factory parameter '{name}' is passed by reference or is a pointer.", nameof(factory));
			if (!seen.Add(name))
				throw new ArgumentException($"The factory has two parameters named '{name}'.", nameof(factory));

			list.Add(new Slot(name, parameter.ParameterType, i, name, false, Slot.FallbackOf(parameter), false, NullAnnotations.IsNonNullableReference(parameter)));
		}

		return [.. list];
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class for what <c>Convert</c> makes.
	/// </summary>
	/// <remarks>
	/// It holds the slots, the factory and the validated pairs of the map <c>Convert</c> was called on, and the converters so far with one more.
	/// </remarks>
	RowMap(
		ImmutableArray<Slot> slots,
		FrozenDictionary<string, int> slotIndex,
		Delegate? factory,
		string?[] columns,
		bool[] named,
		Converter?[] converters)
	{
		_slots = slots;
		_slotIndex = slotIndex;
		_factory = factory;
		_columns = columns;
		_named = named;
		_converters = converters;
	}

	/// <summary>Returns <see cref="Default"/> when <paramref name="mappings"/> is empty; otherwise creates a new map from them, exactly as the constructor does.</summary>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mappings']/*" />
	/// <returns>The default map, or a new map built from <paramref name="mappings"/>.</returns>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	[OverloadResolutionPriority(1)]
	public static RowMap<T> From(ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> mappings.Length == 0 ? Default : new RowMap<T>(mappings);

	/// <summary>Creates a map that builds each <typeparamref name="T"/> by calling <paramref name="factory"/>, each of its parameters fed by the column of its name (a pair renames one) under the rules of a constructor parameter.</summary>
	/// <param name="factory">A delegate returning a <typeparamref name="T"/>; each parameter is matched to a column by name and is the slot a pair names. No member of <typeparamref name="T"/> is set afterwards.</param>
	/// <param name="pairs">The (parameter name, column name) pairs; a <see langword="null"/> column means "do not map this parameter".</param>
	/// <returns>The new map.</returns>
	/// <remarks>
	/// <para>
	/// The pairs a factory map enumerates are the ones it was declared with, not its function:
	/// spreading them into a new map (<c>[.. map]</c>) gives a map of the model's own members, and <c>new RowMap&lt;T&gt;(map)</c> throws <see cref="ArgumentException"/>.
	/// Pass the map itself.
	/// </para>
	/// <para>
	/// Declare it once.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="factory"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">The factory does not return a <typeparamref name="T"/>, a parameter is passed by reference or two share a name, or a pair is invalid (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> is a single value such as <see cref="int"/> or <see cref="string"/>.</exception>
	public static RowMap<T> From(Delegate factory, params ReadOnlySpan<KeyValuePair<string, string?>> pairs)
		=> new(factory ?? throw new ArgumentNullException(nameof(factory)), pairs);

	/// <summary>Returns an enumerator over the pairs the map was declared with.</summary>
	/// <remarks>
	/// That also makes a map a collection-expression target:
	/// each slot a pair named, in slot order, with the column the pair gave it (<see langword="null"/>: an ignored slot).
	/// A slot no pair named is not yielded, so <c>new RowMap&lt;T&gt;(map)</c> builds an equivalent map for a map of the model's own members;
	/// a factory map's pairs do not hold its function, so that constructor throws for one (<c>GetMappingSpan</c>),
	/// and <c>[.. map]</c> gives a member map (see <see cref="From(Delegate, ReadOnlySpan{KeyValuePair{string, string?}})"/>).
	/// </remarks>
	/// <returns>An enumerator over the (property name, column name) pairs.</returns>
	IEnumerator<KeyValuePair<string, string?>> IEnumerable<KeyValuePair<string, string?>>.GetEnumerator()
	{
		for (int i = 0; i < _columns.Length; i++)
			if (_named[i]) yield return new(_slots[i].Name, _columns[i]);
	}

	IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<string, string?>>)this).GetEnumerator();

	/// <summary>
	/// Returns a new map in which <paramref name="member"/> receives
	/// what <paramref name="convert"/> makes of its column,
	/// for a column that needs a trim, a parse or a format on its way to the member.
	/// Every other member maps as it did, and this map is unchanged.
	/// </summary>
	/// <remarks>
	/// The column is the one the map already names for the member (a pair),
	/// else the one the member matches by name
	/// (exactly, ignoring case, then ignoring case and underscores).
	/// To name the column here, use
	/// <see cref="Convert{TColumn, TMember}(string, string, Func{TColumn, TMember})"/>.
	/// The delegate is not called for a NULL cell,
	/// unless <typeparamref name="TColumn"/> is a <see cref="Nullable{T}"/>
	/// (see <see cref="RowMap{T}"/>).
	/// A lambda does not say what the column is read as:
	/// name the types, <c>Convert&lt;long, string&gt;(...)</c>,
	/// or type the parameter of the lambda.
	/// </remarks>
	/// <typeparam name="TColumn">The type the column is read as and the delegate receives: the column's own type, else any type a single value can be read as (<c>Map&lt;TColumn&gt;()</c>); an integer converts only within its range.</typeparam>
	/// <typeparam name="TMember">The type the delegate returns: the member's type, or one assignable to it.</typeparam>
	/// <param name="member">The name of the property (exactly), or of the constructor parameter (ignoring case), that receives the converted value.</param>
	/// <param name="convert">Makes the value of the member of the value of the column. It runs once per row, inside the compiled row reader.</param>
	/// <returns>The new map.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="member"/> or <paramref name="convert"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">The member is unknown or unsettable, has a converter already, is mapped to <see langword="null"/> (it reads no column), or <typeparamref name="TMember"/> cannot be assigned to it.</exception>
	public RowMap<T> Convert<TColumn, TMember>(
		string member,
		Func<TColumn, TMember> convert)
	{
		if (member is null) throw new ArgumentNullException(nameof(member));
		if (convert is null) throw new ArgumentNullException(nameof(convert));
		Contract.EndContractBlock();

		return Converting(member, null, convert);
	}

	/// <summary>
	/// Returns a new map in which <paramref name="member"/> receives
	/// what <paramref name="convert"/> makes of <paramref name="column"/>:
	/// a pair (<c>new(member, column)</c>) with a converter.
	/// Every other member maps as it did, and this map is unchanged.
	/// </summary>
	/// <remarks>
	/// Like a pair, it is matched exactly, else ignoring case.
	/// A member the map has a pair for is converted without naming its column:
	/// <see cref="Convert{TColumn, TMember}(string, Func{TColumn, TMember})"/>.
	/// </remarks>
	/// <typeparam name="TColumn">The type the column is read as and the delegate receives: the column's own type, else any type a single value can be read as (<c>Map&lt;TColumn&gt;()</c>); an integer converts only within its range.</typeparam>
	/// <typeparam name="TMember">The type the delegate returns: the member's type, or one assignable to it.</typeparam>
	/// <param name="member">The name of the property (exactly), or of the constructor parameter (ignoring case), that receives the converted value.</param>
	/// <param name="column">The name of the column the member reads.</param>
	/// <param name="convert">Makes the value of the member of the value of the column. It runs once per row, inside the compiled row reader.</param>
	/// <returns>The new map.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="member"/>, <paramref name="column"/> or <paramref name="convert"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">The member is unknown or unsettable, is mapped by a pair or has a converter already, <paramref name="column"/> is empty or is read by another member, or <typeparamref name="TMember"/> cannot be assigned to the member.</exception>
	public RowMap<T> Convert<TColumn, TMember>(
		string member,
		string column,
		Func<TColumn, TMember> convert)
	{
		if (member is null) throw new ArgumentNullException(nameof(member));
		if (column is null) throw new ArgumentNullException(nameof(column));
		if (convert is null) throw new ArgumentNullException(nameof(convert));
		Contract.EndContractBlock();

		return Converting(member, column, convert);
	}

	/// <summary>
	/// Returns a new map with the converter, for both forms of <c>Convert</c>.
	/// </summary>
	/// <remarks>
	/// A map is immutable: the new one has copies of the pairs, with the column (when the call names it) and the converter of the member.
	/// </remarks>
	/// <typeparam name="TColumn">The type the column is read as and the delegate receives.</typeparam>
	/// <typeparam name="TMember">The type the delegate returns.</typeparam>
	/// <param name="member">The name of the property or constructor parameter that receives the converted value.</param>
	/// <param name="column">The name of the column the member reads, or <see langword="null"/> for the one the map already names for it.</param>
	/// <param name="convert">Makes the value of the member of the value of the column.</param>
	/// <returns>The new map.</returns>
	/// <exception cref="ArgumentException">The member is unknown, has a converter already, is mapped to <see langword="null"/>, is mapped by a pair when <paramref name="column"/> is given, <paramref name="column"/> is empty or is read by another member, or <typeparamref name="TMember"/> cannot be assigned to the member.</exception>
	RowMap<T> Converting<TColumn, TMember>(
		string member,
		string? column,
		Func<TColumn, TMember> convert)
	{
		if (!TryFindSlot(member, out int index))
			throw new ArgumentException(UnknownMember(member), nameof(member));

		Slot slot = _slots[index];
		if (_converters?[index] is not null)
			throw new ArgumentException($"The {Describe(slot)} has a converter already.", nameof(member));

		if (!slot.Type.IsAssignableFrom(typeof(TMember)))
		{
			throw new ArgumentException(
				$"The converter of the {Describe(slot)} returns {Shape.TypeName(typeof(TMember))}, which cannot be assigned to {Shape.TypeName(slot.Type)}.",
				nameof(convert));
		}

		string?[] columns = [.. _columns];
		bool[] named = [.. _named];
		if (column is null)
		{
			if (columns[index] is null)
				throw new ArgumentException($"The {Describe(slot)} is mapped to null, so it reads no column to convert.", nameof(member));
		}
		else
		{
			if (column.Length == 0)
				throw new ArgumentException($"The {Describe(slot)} is converted from an empty column name.", nameof(column));

			if (named[index])
				throw new ArgumentException($"The {Describe(slot)} is mapped more than once: a pair names its column already.", nameof(member));

			columns[index] = column;
			named[index] = true;
			RejectSharedColumns(columns, named, nameof(column));
		}

		Converter?[] converters = _converters is null ? new Converter?[_slots.Length] : [.. _converters];
		converters[index] = new Converter(
			convert,
			typeof(Func<TColumn, TMember>),
			typeof(TColumn),
			typeof(TMember));

		return new RowMap<T>(_slots, _slotIndex, _factory, columns, named, converters);
	}

	// The compiled, cached readers of a layout are described on Layout.

	/// <summary>The number of layouts a map keeps readers for before its cache is cleared.</summary>
	/// <remarks>
	/// The per-layout reader cache is bounded so dynamically generated column lists cannot grow memory forever.
	/// Real applications use a handful of layouts; when the bound is hit the cache is simply cleared and refills.
	/// </remarks>
	const int MaxLayouts = 128;

	// The statics below are one immutable copy per model type: see TypeShape.

	/// <summary>
	/// Whether <typeparamref name="T"/> can be mapped at all, judged from its shape.
	/// </summary>
	/// <remarks>
	/// <para>
	/// It never throws (an unmappable <typeparamref name="T"/> holds its error), so no static initializer here can fail with it.
	/// </para>
	/// <para>
	/// The per-model-type statics (<c>TypeShape</c>, <c>ModelSlots</c>, <c>ModelSlotIndex</c>, <see cref="Default"/>) are one immutable copy per <typeparamref name="T"/>, built once.
	/// Any cache of compiled readers keyed by model type lives as long and costs a lookup per call; a static on the generic type is the cheapest place for it.
	/// Nothing that does not depend on <typeparamref name="T"/> belongs here.
	/// </para>
	/// </remarks>
	static readonly Shape TypeShape = Shape.Of(typeof(T));

	/// <summary>
	/// Holds the slots of <typeparamref name="T"/> and what its members declare, built once.
	/// </summary>
	/// <remarks>
	/// Reflection runs once per <typeparamref name="T"/>.
	/// A property marked <c>[NotMapped]</c> keeps its slot but reads no column of its own (a pair can still name it).
	/// What the members declare (<c>[Column]</c>, <c>[NotMapped]</c>) is judged here too, once.
	/// </remarks>
	static readonly SlotTable Model = BuildSlots();

	/// <summary>
	/// What a column can feed: one slot per parameter of the constructor the model is built through
	/// (none for a model built as it always was), then one per settable public instance property no parameter covers,
	/// in declaration order (a property hidden by name keeps the first).
	/// </summary>
	static readonly ImmutableArray<Slot> ModelSlots = Model.Slots;

	/// <summary>
	/// Why <typeparamref name="T"/> cannot be mapped (its shape, else a contradiction in what its members declare), or <see langword="null"/> when it can.
	/// </summary>
	static readonly string? TypeError = TypeShape.Error ?? Model.Error;

	/// <summary>
	/// Whether any member declares a column or an ordinal.
	/// </summary>
	/// <remarks>
	/// Only then does a result set pay the checks of <c>Match</c> for it (once per layout, never per row).
	/// </remarks>
	static readonly bool HasDecorators = Model.Decorated;

	/// <summary>
	/// Represents the slots of a model type, and the verdict on what its members declare.
	/// </summary>
	/// <param name="slots">The slots, in order.</param>
	/// <param name="error">Why the declarations cannot be honoured, or <see langword="null"/> when they can.</param>
	/// <param name="decorated"><see langword="true"/> if any slot declares a column name or an ordinal; otherwise, <see langword="false"/>.</param>
	sealed class SlotTable(ImmutableArray<Slot> slots, string? error, bool decorated)
	{
		/// <summary>The slots, in order.</summary>
		public readonly ImmutableArray<Slot> Slots = slots;

		/// <summary>Why the declarations cannot be honoured, or <see langword="null"/> when they can.</summary>
		public readonly string? Error = error;

		/// <summary>Whether any slot declares a column name or an ordinal.</summary>
		public readonly bool Decorated = decorated;
	}

	/// <summary>
	/// Builds the slots of <typeparamref name="T"/> and judges what its members declare.
	/// </summary>
	/// <returns>The slots, none when the shape of <typeparamref name="T"/> cannot be mapped, with the first declaration error, if any.</returns>
	static SlotTable BuildSlots()
	{
		if (TypeShape.Error is not null) return new([], null, false);

		var seen = new HashSet<string>(StringComparer.Ordinal);
		var list = new List<Slot>();
		string? error = null;
		if (TypeShape.Constructor is { } constructor) AddParameterSlots(constructor, list, seen, ref error);

		foreach (PropertyInfo p in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			if (p.GetIndexParameters().Length != 0 || p.GetSetMethod() is not { } setter || !seen.Add(p.Name)) continue;

			Decoration declared = Decoration.Of(p);
			if (declared.NotMapped || declared.Invalid is not null) error ??= DeclarationError(p, declared, null);

			list.Add(new Slot(p.Name, p.PropertyType, setter, MustHaveColumn(p), NullAnnotations.IsNonNullableReference(p), declared.Column, declared.Order, declared.NotMapped));
		}

		bool decorated = list.Exists(s => s.ColumnName is not null || s.Order >= 0);
		if (decorated) error ??= DuplicateDeclaration(list);

		return new([.. list], error, decorated);
	}

	/// <summary>
	/// Adds a slot for each parameter of the constructor, matched to the property it feeds.
	/// </summary>
	/// <remarks>
	/// A parameter and a public property whose names are equal ignoring case are one slot:
	/// named after the property, typed by the parameter, fed through the constructor and never set again (a record's positional init property).
	/// The exact name wins over the one ignoring case, each property serves one parameter, and a parameter with no property is a slot named after itself.
	/// The slot takes what its property declares (<c>[property: Column("x")]</c> on a positional record); a parameter with no property declares nothing.
	/// </remarks>
	/// <param name="constructor">The constructor the model is built through.</param>
	/// <param name="list">The slots, to add to.</param>
	/// <param name="seen">The names of the slots so far.</param>
	/// <param name="error">The first declaration error so far; set when a declaration cannot be honoured and none was found before.</param>
	static void AddParameterSlots(ConstructorInfo constructor, List<Slot> list, HashSet<string> seen, ref string? error)

	{
		ParameterInfo[] parameters = constructor.GetParameters();
		PropertyInfo[] properties = [.. typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0)];
		var matches = new PropertyInfo?[parameters.Length];
		bool[] taken = new bool[properties.Length];

		foreach (StringComparison comparison in (StringComparison[])[StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase])
		{
			for (int i = 0; i < parameters.Length; i++)
			{
				if (matches[i] is not null) continue;

				for (int p = 0; p < properties.Length; p++)
				{
					if (taken[p] || !string.Equals(properties[p].Name, parameters[i].Name, comparison)) continue;

					matches[i] = properties[p];
					taken[p] = true;
					break;
				}
			}
		}

		for (int i = 0; i < parameters.Length; i++)
		{
			ParameterInfo parameter = parameters[i];
			string name = matches[i]?.Name ?? parameter.Name ?? "arg" + i;
			bool added = seen.Add(name);
			Debug.Assert(added, "A slot name is unique: a parameter takes the name of a property or its own, and the exact name was matched first.");

			Decoration declared = matches[i] is { } property ? Decoration.Of(property) : Decoration.None;
			if (declared.NotMapped || declared.Invalid is not null) error ??= DeclarationError(matches[i]!, declared, parameter);

			list.Add(new Slot(name, parameter.ParameterType, i, parameter.Name ?? name, matches[i] is not null, Slot.FallbackOf(parameter), matches[i] is { } required && MustHaveColumn(required), NullAnnotations.IsNonNullableReference(parameter), declared.Column, declared.Order));
		}
	}

	/// <summary>
	/// Returns why a declaration cannot be honoured, so the type cannot be mapped.
	/// </summary>
	/// <remarks>
	/// A declaration cannot be honoured when it is not valid (an empty name, a negative ordinal),
	/// or when it is <c>[NotMapped]</c> and also a <c>[Column]</c>, required (no column could feed it), or set by a constructor parameter.
	/// </remarks>
	/// <param name="property">The property that declares it.</param>
	/// <param name="declared">What the property declares.</param>
	/// <param name="parameter">The constructor parameter that sets the property, or <see langword="null"/> for none.</param>
	/// <returns>The message, or <see langword="null"/> for a plain <c>[NotMapped]</c>, which leaves the property out.</returns>
	static string? DeclarationError(PropertyInfo property, Decoration declared, ParameterInfo? parameter)
	{
		string type = Shape.TypeName(typeof(T));
		if (declared.Invalid is { } invalid)
			return $"'{type}' cannot be mapped: property '{property.Name}' has {invalid}. A declaration is a hard rule, so it has to be valid.";

		if (declared.HasColumnAttribute)
			return $"'{type}' cannot be mapped: property '{property.Name}' is both [NotMapped] and [Column]. A member is mapped or it is not.";

		if (parameter is not null)
			return $"'{type}' cannot be mapped: property '{property.Name}' is [NotMapped], but constructor parameter '{parameter.Name}' sets it. Remove [NotMapped], or the parameter.";

		return MustHaveColumn(property)
			? $"'{type}' cannot be mapped: property '{property.Name}' is [NotMapped] and required, so no column could feed it. Remove one of them."
			: null;
	}

	/// <summary>
	/// Returns why two members cannot both be satisfied, or <see langword="null"/> when none clash.
	/// </summary>
	/// <remarks>
	/// Two members that declare the same ordinal, or the same column name (ignoring case), cannot both be satisfied.
	/// The first pair is named.
	/// </remarks>
	/// <param name="slots">The slots, in order.</param>
	/// <returns>The message, or <see langword="null"/> when no two slots declare the same ordinal or column name.</returns>
	static string? DuplicateDeclaration(List<Slot> slots)
	{
		for (int j = 1; j < slots.Count; j++)
		{
			for (int i = 0; i < j; i++)
			{
				if (slots[i].Order >= 0 && slots[i].Order == slots[j].Order)
					return $"'{Shape.TypeName(typeof(T))}' cannot be mapped: '{slots[i].Name}' and '{slots[j].Name}' both declare [Column(Order = {slots[i].Order})].";

				if (slots[i].ColumnName is { } column && string.Equals(column, slots[j].ColumnName, StringComparison.OrdinalIgnoreCase))
					return $"'{Shape.TypeName(typeof(T))}' cannot be mapped: '{slots[i].Name}' and '{slots[j].Name}' both declare [Column(\"{column}\")].";
			}
		}

		return null;
	}

	/// <summary>
	/// Returns whether the property must be fed by a column in every result shape.
	/// </summary>
	/// <remarks>
	/// A constructor parameter's default does not feed a required property: the property of a parameter is required all the same.
	/// A constructor that sets the required members itself (<c>[SetsRequiredMembers]</c>) lifts that: they are ordinary properties then.
	/// </remarks>
	static bool MustHaveColumn(PropertyInfo property) => !TypeShape.SetsRequiredMembers && Shape.IsRequired(property);

	/// <summary>The index of <see cref="ModelSlots"/> by name.</summary>
	/// <remarks>
	/// Static initializers run in textual order; each must follow what it uses.
	/// </remarks>
	static readonly FrozenDictionary<string, int> ModelSlotIndex = BuildSlotIndex(ModelSlots);

	static FrozenDictionary<string, int> BuildSlotIndex(ImmutableArray<Slot> slots)
	{
		var index = new Dictionary<string, int>(slots.Length, StringComparer.Ordinal);
		for (int i = 0; i < slots.Length; i++) index.Add(slots[i].Name, i);
		return index.ToFrozenDictionary();
	}

	/// <summary>Gets the map for the model's default mapping (property name == column name), used when no map is passed.</summary>
	/// <value>The default map of <typeparamref name="T"/>.</value>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped: it is an interface, an abstract class, a delegate, a single value such as <see cref="int"/> or <see cref="string"/>, a dictionary or a dynamic object, a type with two or more public constructors and none without parameters, a class with no public constructor, a type with a <c>required</c> public field, unless its constructor is marked <c>[SetsRequiredMembers]</c>, or a type whose <c>[Column]</c> and <c>[NotMapped]</c> declarations cannot all be honoured (see <see cref="RowMap{T}"/>).</exception>
	public static RowMap<T> Default
	{
		// Null when T cannot be mapped, so reading Default throws instead of a static initializer failing.
		get => field ?? throw new InvalidOperationException(TypeError);
	} = TypeError is null ? new() : null; // Declared after TypeShape, ModelSlots and ModelSlotIndex: its constructor reads them.

	/// <summary>
	/// Throws when <typeparamref name="T"/> cannot be mapped.
	/// </summary>
	/// <remarks>
	/// It is for an entry point that runs a command before it maps a row: checking the type first makes an unmappable <typeparamref name="T"/> fail at the call, with nothing executed.
	/// </remarks>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter", Justification = "It checks this T's shape: the type parameter is how a caller names which T to check (RowMap<T>.ThrowIfUnmappable()).")]
	internal static void ThrowIfUnmappable()
	{
		if (TypeError is not null) throw new InvalidOperationException(TypeError);
	}

	/// <summary>What this map's columns can feed: the model's own slots (<c>ModelSlots</c>), or the parameters of its factory (<c>From(factory, pairs)</c>).</summary>
	readonly ImmutableArray<Slot> _slots;
	/// <summary>The slots by name.</summary>
	readonly FrozenDictionary<string, int> _slotIndex;

	/// <summary>The function that builds a <typeparamref name="T"/> from its parameters in place of the model's constructor, or <see langword="null"/> for a map of the model's own members.</summary>
	readonly Delegate? _factory;

	/// <summary>The column each property reads, aligned with the slots; <see langword="null"/> means the property is not mapped (ignored).</summary>
	/// <remarks>
	/// With the named flags, all the map keeps of the pairs.
	/// </remarks>
	readonly string?[] _columns;

	/// <summary>
	/// Aligned with the slots: <see langword="true"/> when a pair named the property, so what the pair says is exact and final (a pair whose column equals the property's own name included).
	/// </summary>
	/// <remarks>
	/// Only a property no pair named is eligible for the underscore tier.
	/// </remarks>
	readonly bool[] _named;

	/// <summary>
	/// Aligned with the slots: the converter the value of a slot goes through on its way from its column (<c>Convert</c>), or <see langword="null"/> for a slot with none.
	/// </summary>
	/// <remarks>
	/// The array itself is <see langword="null"/> for a map nothing was converted in, so such a map pays nothing for it.
	/// </remarks>
	readonly Converter?[]? _converters;

	/// <summary>Gets the names of the properties that are mapped.</summary>
	internal IEnumerable<string> PropertyNames
	{
		get
		{
			for (int i = 0; i < _columns.Length; i++)
				if (_columns[i] is not null) yield return _slots[i].Name;
		}
	}

	/// <summary>Gets the names of the columns the mapped properties read.</summary>
	internal IEnumerable<string> ColumnNames
	{
		get
		{
			foreach (string? column in _columns)
				if (column is not null) yield return column;
		}
	}

	/// <summary>
	/// Finds the columns of the record that feed a property, as their ordinals in ascending order and, aligned with them, their actual names in the record.
	/// </summary>
	/// <remarks>
	/// It is the same match (<c>Match</c>) a row read uses,
	/// so the pipelines that pick columns before mapping them agree with <c>Map&lt;T&gt;()</c> on which column feeds which property.
	/// </remarks>
	/// <param name="record">The record whose columns are matched.</param>
	/// <param name="ordinals">The ordinals of the columns that feed a property, in ascending order.</param>
	/// <param name="names">The names of those columns in the record, aligned with <paramref name="ordinals"/>.</param>
	internal void MatchingColumns(IDataRecord record, out int[] ordinals, out ImmutableArray<string> names)
	{
		string[] all = new string[record.FieldCount];
		for (int i = 0; i < all.Length; i++) all[i] = record.GetName(i);

		Slot?[] feeds = Match(all);
		int count = 0;
		foreach (Slot? feed in feeds)
			if (feed is not null) count++;

		ordinals = new int[count];
		var matched = ImmutableArray.CreateBuilder<string>(count);
		for (int i = 0, next = 0; next < count; i++)
		{
			if (feeds[i] is null) continue;

			ordinals[next++] = i;
			matched.Add(all[i]);
		}

		names = matched.MoveToImmutable();
	}

	/// <summary>
	/// Returns the property each result column feeds (<see langword="null"/>: the column is ignored).
	/// </summary>
	/// <remarks>
	/// <para>
	/// It never runs per row; it is linear in the columns plus the properties.
	/// </para>
	/// <para>
	/// A column a member declares by ordinal (<c>[Column(Order = n)]</c>) is that member's before any tier runs, so none gives it away; a column that is not its throws.
	/// </para>
	/// <para>
	/// Three tiers run over the whole result set, in order, so a lower tier never takes a property a higher one has filled:
	/// the column a property reads exactly, the same ignoring case,
	/// then, only for a property no pair named, a column equal to the property's name ignoring case and underscores.
	/// Within a tier the first matching column wins, and a column feeds one property.
	/// </para>
	/// </remarks>
	Slot?[] Match(string[] names)
	{
		var feeds = new Slot?[names.Length];
		bool[] filled = new bool[_columns.Length];

		// A column a member declares by ordinal ([Column(Order = n)]) is its before any tier runs, so none gives it away; a column that is not its throws.
		int claimed = HasDecorators ? ClaimOrdinals(names, feeds, filled) : 0;

		int unmatched = names.Length - claimed - Feed(ColumnIndex(StringComparer.Ordinal), names, feeds, filled);
		if (unmatched > 0) unmatched -= Feed(ColumnIndex(StringComparer.OrdinalIgnoreCase), names, feeds, filled);
		if (unmatched > 0 && NameIndex(filled) is { } byName) Feed(byName, names, feeds, filled);

		return feeds;
	}

	/// <summary>
	/// Runs one tier: one pass over the columns that no earlier tier matched.
	/// </summary>
	/// <remarks>
	/// Each column looks up its property, which it takes unless an earlier column already did.
	/// It returns how many columns it fed.
	/// </remarks>
	int Feed(Dictionary<string, int> index, string[] names, Slot?[] feeds, bool[] filled)
	{
		int fed = 0;
		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not null || names[i] is null || !index.TryGetValue(names[i], out int p) || filled[p]) continue;

			filled[p] = true;
			feeds[i] = _slots[p];
			fed++;
		}

		return fed;
	}

	/// <summary>
	/// Builds the index of tiers 1 and 2: column name to the first property, in declaration order, that reads it (compared as the tier says).
	/// </summary>
	/// <remarks>
	/// A member that declares its column (<c>[Column(Name)]</c>) is asked first: another member reading the same name does not take it.
	/// </remarks>
	Dictionary<string, int> ColumnIndex(StringComparer comparer)
	{
		var index = new Dictionary<string, int>(_columns.Length, comparer);

		// A member that declares its column ([Column(Name)]) is asked first: another member reading the same name does not take it.
		if (HasDecorators)
		{
			for (int p = 0; p < _columns.Length; p++)
				if (_slots[p].ColumnName is not null && !_named[p] && _columns[p] is { } declared && !index.ContainsKey(declared)) index.Add(declared, p);
		}

		for (int p = 0; p < _columns.Length; p++)
			if (_columns[p] is { } column && !index.ContainsKey(column)) index.Add(column, p);

		return index;
	}

	/// <summary>
	/// Gives each column that a member declares by ordinal (<c>[Column(Order = n)]</c>) to that member, before any tier runs.
	/// </summary>
	/// <remarks>
	/// Column <c>n</c> must be the column of the member that declares it:
	/// its <c>[Column]</c> name, else the member's own name, exactly or ignoring case and never ignoring underscores
	/// (an undecorated member is matched loosely; a declared one is not).
	/// It is that member's from then on.
	/// It runs once per layout; nothing here is per row.
	/// </remarks>
	/// <param name="names">The names of the columns of the result.</param>
	/// <param name="feeds">The property each column feeds; set for the claimed columns.</param>
	/// <param name="filled">Whether each slot is settled; set for the claiming slots.</param>
	/// <returns>The number of columns claimed.</returns>
	/// <exception cref="ValueMappingException">The result has too few columns, or the column at the ordinal is not the member's.</exception>
	int ClaimOrdinals(string[] names, Slot?[] feeds, bool[] filled)
	{
		int claimed = 0;
		for (int p = 0; p < _slots.Length; p++)
		{
			// A pair that names the member overrides what it declares: the column is the pair's, and the ordinal is not checked.
			Slot slot = _slots[p];
			if (slot.Order < 0 || _named[p]) continue;

			string expected = _columns[p] ?? slot.Name;
			if (slot.Order >= names.Length)
				throw OrderMismatch(slot, expected, $"the result has {names.Length} column{(names.Length == 1 ? "" : "s")}");

			string actual = names[slot.Order];
			// A declaration is a hard rule: the name, exactly or ignoring case, never the looser tiers of an undecorated member (no underscores).
			if (!string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase)) throw OrderMismatch(slot, expected, $"column {slot.Order} is '{actual}'");

			feeds[slot.Order] = slot;
			filled[p] = true;
			claimed++;
		}

		return claimed;
	}

	/// <summary>
	/// Creates the exception for a member whose declared ordinal does not hold in the result.
	/// </summary>
	/// <param name="slot">The member that declares the ordinal.</param>
	/// <param name="expected">The name of the column that must be at the ordinal.</param>
	/// <param name="found">What is there instead.</param>
	/// <returns>The exception to throw.</returns>
	ValueMappingException OrderMismatch(Slot slot, string expected, string found)
		=> new(
			$"The {Describe(slot)} of '{Shape.TypeName(typeof(T))}' is declared [Column(Order = {slot.Order})], so column {slot.Order} must be '{expected}', but {found}.",
			slot.Name,
			expected,
			slot.Type);

	/// <summary>
	/// Builds the index of tier 3: property name to the first property, in declaration order, of the unfilled properties no pair named
	/// (every named property is left out, whatever its column), compared ignoring case and underscores.
	/// </summary>
	/// <remarks>
	/// A property that declares its column (<c>[Column(Name)]</c>) or is <c>[NotMapped]</c> is left out too.
	/// It returns <see langword="null"/> when there is none, so nothing is built for a result set whose properties are all settled.
	/// </remarks>
	Dictionary<string, int>? NameIndex(bool[] filled)
	{
		Dictionary<string, int>? index = null;
		for (int p = 0; p < _columns.Length; p++)
		{
			string name = _slots[p].Name;
			if (filled[p] || _named[p] || _slots[p].ColumnName is not null || _slots[p].NotMapped) continue;

			index ??= new(_columns.Length, new IgnoreCaseAndUnderscores());
#if NETSTANDARD2_0
			if (!index.ContainsKey(name)) index.Add(name, p);
#else
			index.TryAdd(name, p);
#endif
		}

		return index;
	}

	/// <summary>
	/// Compares names ignoring case and ignoring every <c>_</c> in either name.
	/// </summary>
	/// <remarks>
	/// No stripped copy of a name is ever made: the hash and the comparison walk the characters.
	/// </remarks>
	sealed class IgnoreCaseAndUnderscores : IEqualityComparer<string>
	{
		public bool Equals(string? x, string? y)
		{
			if (ReferenceEquals(x, y)) return true;
			if (x is null || y is null) return false;

			int i = 0, j = 0;
			while (true)
			{
				while (i < x.Length && x[i] == '_') i++;
				while (j < y.Length && y[j] == '_') j++;
				if (i == x.Length || j == y.Length) return i == x.Length && j == y.Length;
				if (char.ToUpperInvariant(x[i++]) != char.ToUpperInvariant(y[j++])) return false;
			}
		}

		public int GetHashCode(string name)
		{
			int hash = 17;
			foreach (char c in name)
				if (c != '_') hash = unchecked(hash * 31 + char.ToUpperInvariant(c));

			return hash;
		}
	}

	/// <summary>The layout most recently used; the overwhelmingly common case is the same shape every time.</summary>
	Layout? _last;
	/// <summary>Every layout seen so far, by hash; created with the first layout, so a map that is never read holds no dictionary.</summary>
	ConcurrentDictionary<int, Layout[]>? _layouts;

	/// <summary>
	/// Applies the pairs and reports which slots they named.
	/// </summary>
	bool[] Declare(ReadOnlySpan<KeyValuePair<string, string?>> mappings, string?[] columns, string paramName)
	{
		bool[] named = new bool[columns.Length];
		foreach (KeyValuePair<string, string?> pair in mappings)
		{
			if (pair.Key is null) throw new ArgumentException("A mapping has no property name.", paramName);

			bool known = TryFindSlot(pair.Key, out int index);
			if (known)
			{
				if (named[index]) throw new ArgumentException($"The {Describe(_slots[index])} is mapped more than once.", paramName);
				named[index] = true;
			}

			if (pair.Value is { Length: 0 }) throw new ArgumentException($"The property '{pair.Key}' is mapped to an empty column name. Use null to ignore it.", paramName);
			if (!known) throw new ArgumentException(UnknownMember(pair.Key), paramName);

			if (pair.Value is null && _slots[index].Required) throw new ArgumentException($"The property '{_slots[index].Name}' is required, so it cannot be mapped to null. Map it to a column.", paramName);

			columns[index] = pair.Value; // A null column means 'do not map this slot'.
		}

		return named;
	}

	/// <summary>
	/// Finds the slot a pair's name refers to.
	/// </summary>
	/// <remarks>
	/// A pair's name is matched to a settable property exactly; failing that, to a constructor parameter ignoring case
	/// (<c>nameof</c> reaches the property of the same name, not the parameter).
	/// </remarks>
	bool TryFindSlot(string name, out int index)
	{
		if (_slotIndex.TryGetValue(name, out index)) return true;

		for (int i = 0; i < _slots.Length; i++)
		{
			if (_slots[i].Parameter is not { } parameter || !string.Equals(parameter, name, StringComparison.OrdinalIgnoreCase)) continue;

			index = i;
			return true;
		}

		return false;
	}

	string UnknownMember(string name)
		=> _factory is not null
			? $"The factory of '{typeof(T)}' has no parameter named '{name}'."
			: $"'{typeof(T)}' has no settable public property{(TypeShape.Constructor is null ? null : " or constructor parameter")} named '{name}'.";

	string Describe(Slot slot) => (slot.IsProperty ? "property '" : _factory is null ? "constructor parameter '" : "factory parameter '") + slot.Name + "'";

	/// <summary>
	/// Throws when two slots read one column and a pair names either.
	/// </summary>
	/// <remarks>
	/// Each mapped slot is compared with the slots before it (the first reader of a column is the one that gets it).
	/// Only what the map names is held to account: the model's own by-name twins are <c>Map&lt;T&gt;()</c>'s business.
	/// The check is quadratic in the slot count and runs once per map.
	/// </remarks>
	void RejectSharedColumns(string?[] columns, bool[] named, string paramName)
	{
		for (int i = 1; i < columns.Length; i++)
		{
			if (columns[i] is not { } column) continue;

			for (int j = 0; j < i; j++)
			{
				if (!string.Equals(columns[j], column, StringComparison.OrdinalIgnoreCase)) continue;

				if (named[i] || named[j])
				{
					string who = _slots[j].IsProperty && _slots[i].IsProperty
						? $"properties '{_slots[j].Name}' and '{_slots[i].Name}'"
						: $"{Describe(_slots[j])} and the {Describe(_slots[i])}";

					throw new ArgumentException($"The {who} both read column '{column}'. Map one of them to another column, or to null to ignore it.", paramName);
				}

				break;
			}
		}
	}

	Func<DbDataReader, T> IRowSource<T>.Mapper(DbDataReader reader)
	{
		Layout? l = _last;
		if (l?.Matches(reader) != true) l = Resolve(reader);
		return l.Db ??= Build<DbDataReader>(l);
	}

	Func<IDataRecord, T> IRowSource<T>.Mapper(IDataRecord reader)
	{
		Layout? l = _last;
		if (l?.Matches(reader) != true) l = Resolve(reader);
		return l.Rec ??= Build<IDataRecord>(l);
	}

	Layout Resolve(IDataRecord reader)
	{
		ConcurrentDictionary<int, Layout[]> layouts = Cache(ref _layouts);
		int hash = Layout.Hash(reader);
		return _last = Find(layouts, hash, l => l.Matches(reader))
			?? Remember(layouts, hash, new Layout(reader));
	}

	/// <summary>
	/// Returns the dictionary of a layout cache, created on first use.
	/// </summary>
	/// <remarks>
	/// It is shared by both layout caches: a lost race just drops an empty dictionary.
	/// </remarks>
	static ConcurrentDictionary<int, TLayout[]> Cache<TLayout>(ref ConcurrentDictionary<int, TLayout[]>? field)
		where TLayout : class
		=> Volatile.Read(ref field) ?? Interlocked.CompareExchange(ref field, new(), null) ?? field;

	/// <summary>
	/// Finds a layout by hash and match, or returns <see langword="null"/>.
	/// </summary>
	/// <remarks>
	/// It is shared by both layout caches.
	/// </remarks>
	static TLayout? Find<TLayout>(ConcurrentDictionary<int, TLayout[]> cache, int hash, Func<TLayout, bool> matches)
		where TLayout : class
	{
		if (cache.TryGetValue(hash, out TLayout[]? candidates))
		{
			foreach (TLayout c in candidates)
				if (matches(c)) return c;
		}

		return null;
	}

	static TLayout Remember<TLayout>(ConcurrentDictionary<int, TLayout[]> cache, int hash, TLayout layout)
		where TLayout : class
	{
		if (cache.Count >= MaxLayouts) cache.Clear();
		cache.AddOrUpdate(hash, _ => [layout], (_, existing) => [.. existing, layout]);
		return layout;
	}

	/// <summary>
	/// Represents one result-set layout (the column names and types) and the compiled readers for it.
	/// </summary>
	/// <remarks>
	/// The map holds the validated column table and the compiled, cached, typed row readers (one per result-set layout: column names and column types),
	/// so after the first query of a given shape no reflection, expression building or compilation happens again.
	/// Values are read with the provider's typed getters whenever the column type matches the property type (or converts numerically), so nothing is boxed.
	/// The member's declared type decides what a NULL does:
	/// <see cref="Nullable{T}"/>, a reference type declared nullable and a reference type in a context without nullable annotations receive <see langword="null"/>;
	/// a member the C# type declares non-nullable (a value type, an enum included, or a reference type with nullable annotations on) is skipped,
	/// so a property keeps what its constructor or initializer gave it (else its default) and a constructor parameter gets its fallback:
	/// one <c>IsDBNull</c> per such cell on the typed path, a single read of the cell on the general path.
	/// Which reference types are declared non-nullable is read once per model type, when the slots are built (<c>NullAnnotations</c>).
	/// </remarks>
	sealed class Layout
	{
		public readonly string[] Names;
		public readonly Type[] Types;
		public Func<DbDataReader, T>? Db;
		public Func<IDataRecord, T>? Rec;

		public Layout(IDataRecord r)
		{
			int n = r.FieldCount;
			Names = new string[n];
			Types = new Type[n];
			for (int i = 0; i < n; i++)
			{
				Names[i] = r.GetName(i);
				Types[i] = r.GetFieldType(i) ?? typeof(object); // Be tolerant of providers that do not report a type.
			}
		}

		public static int Hash(IDataRecord r)
		{
			int n = r.FieldCount;
			int h = n;
			for (int i = 0; i < n; i++)
			{
				h = unchecked(h * 31 + (r.GetName(i)?.GetHashCode() ?? 0));
				h = unchecked(h * 31 + (r.GetFieldType(i) ?? typeof(object)).GetHashCode());
			}

			return h;
		}

		/// <summary>
		/// Returns whether the reader has this layout.
		/// </summary>
		/// <remarks>
		/// The same check is written twice on purpose: this runs on every query, and virtual calls on <see cref="DbDataReader"/> beat the interface calls the <see cref="IDataRecord"/> overload must make.
		/// </remarks>
		public bool Matches(DbDataReader r)
		{
			string[] names = Names;
			Type[] types = Types;
			if (r.FieldCount != names.Length) return false;
			for (int i = 0; i < names.Length; i++)
			{
				if (!string.Equals(r.GetName(i), names[i], StringComparison.Ordinal) || (r.GetFieldType(i) ?? typeof(object)) != types[i])
					return false;
			}

			return true;
		}

		/// <inheritdoc cref="Matches(DbDataReader)"/>
		public bool Matches(IDataRecord r)
		{
			string[] names = Names;
			Type[] types = Types;
			if (r.FieldCount != names.Length) return false;
			for (int i = 0; i < names.Length; i++)
			{
				if (!string.Equals(r.GetName(i), names[i], StringComparison.Ordinal) || (r.GetFieldType(i) ?? typeof(object)) != types[i])
					return false;
			}

			return true;
		}
	}

	#region Array rows (the buffered Retrieve/QueryResult route)

	/// <summary>The layout of the <c>object[]</c> rows most recently used.</summary>
	/// <remarks>
	/// Rows that were already copied out of a reader as <c>object[]</c> (boxed values)
	/// are mapped by a compiled, cached per-column-list delegate too,
	/// rather than rebuilding lookups and property setters per query.
	/// </remarks>
	ArrayLayout? _lastArray;
	/// <summary>Every layout of the <c>object[]</c> rows seen so far, by hash of the column names.</summary>
	ConcurrentDictionary<int, ArrayLayout[]>? _arrayLayouts;

	sealed class ArrayLayout(string[] names)
	{
		public readonly string[] Names = names;
		public Func<object?[], T>? Map;

		public bool Matches(ImmutableArray<string> candidate)
		{
			string[] mine = Names;
			if (candidate.Length != mine.Length) return false;
			for (int i = 0; i < mine.Length; i++)
				if (!string.Equals(candidate[i], mine[i], StringComparison.Ordinal)) return false;
			return true;
		}
	}

	/// <summary>Returns the mapper from <c>object[]</c> rows whose columns are, in order, <paramref name="names"/>.</summary>
	/// <param name="names">The names of the columns, in order.</param>
	/// <returns>The delegate that maps a row to a <typeparamref name="T"/>.</returns>
	/// <exception cref="ValueMappingException">A required property has no column among <paramref name="names"/>.</exception>
	internal Func<object?[], T> ArrayMapper(ImmutableArray<string> names)
	{
		ArrayLayout? l = _lastArray;
		if (l?.Matches(names) != true) l = ResolveArray(names);
		return l.Map ??= BuildArray(l);
	}

	ArrayLayout ResolveArray(ImmutableArray<string> names)
	{
		int hash = names.Length;
		foreach (string n in names) hash = unchecked(hash * 31 + (n?.GetHashCode() ?? 0));

		ConcurrentDictionary<int, ArrayLayout[]> layouts = Cache(ref _arrayLayouts);
		return _lastArray = Find(layouts, hash, l => l.Matches(names))
			?? Remember(layouts, hash, new ArrayLayout([.. names]));
	}

	Func<object?[], T> BuildArray(ArrayLayout layout)
	{
		ParameterExpression row = Expression.Parameter(typeof(object?[]), "row");
		MethodInfo convert = typeof(RowMap<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!;

		MethodInfo isNullCell = typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!;

		// The cell of column i as a slot's type: NULL is its default.
		Expression Read(int i, Slot slot)
			=> Expression.Call(convert.MakeGenericMethod(slot.Type), Expression.ArrayIndex(row, Expression.Constant(i)));

		// Whether that cell is NULL, asked only for a slot that keeps its value on NULL.
		Expression? IsNull(int i, Slot slot)
			=> slot.KeepsValueOnNull ? Expression.Call(isNullCell, Expression.ArrayIndex(row, Expression.Constant(i))) : null;

		// A property the type declares non-nullable (a value type, or a reference with nullable annotations on) is skipped when its cell is NULL; a constructor parameter of one gets its fallback.
		// A slot with a converter reads its column as the converter's type, and is skipped or defaulted by that (see Converted).
		return Compile<object?[]>(
			row,
			layout.Names,
			(i, slot) => ConverterOf(slot) is { } converter
				? Converted(converter, slot, Read(i, converter.Column), IsNull(i, converter.Column))
				: Read(i, slot),
			(i, slot) => ConverterOf(slot) is { } converter
				? SkipsOnNull(slot, IsNull(i, converter.Column))
				: IsNull(i, slot));
	}

	#endregion

	#region Compilation

	/// <summary>Names what a failure reading a column concerns.</summary>
	/// <remarks>
	/// It holds the slot (its property, or its constructor parameter when no property carries its name),
	/// the column and the type the value had to become.
	/// </remarks>
	/// <param name="property">The name of the slot.</param>
	/// <param name="column">The name of the column.</param>
	/// <param name="type">The type the value had to become.</param>
	/// <param name="parameter">The name of the constructor parameter the column is passed to, <see langword="null"/> for a property that is set.</param>
	sealed class Binding(string property, string column, Type type, string? parameter)
	{
		public readonly string Property = property;
		public readonly string Column = column;
		public readonly Type Type = type;
		public readonly string? Parameter = parameter;
	}

	Func<TReader, T> Build<TReader>(Layout layout)
		where TReader : class, IDataRecord
	{
		Type readerType = typeof(TReader);
		MethodInfo isDbNull = readerType.GetMethod(nameof(IDataRecord.IsDBNull), [typeof(int)])!;
		MethodInfo getValue = readerType.GetMethod(nameof(IDataRecord.GetValue), [typeof(int)])!;

		ParameterExpression r = Expression.Parameter(readerType, "r");

		// The general path reads a cell once into `cell`, tests it for NULL, and converts that same value.
		ParameterExpression cell = Expression.Variable(typeof(object), "cell");

		// What column i gives a slot, and whether its NULL leaves the slot alone, as the type it is read as:
		// the member's own, or the one the converter of the slot receives (see Converted).
		Expression Read(int i, Slot slot)
			=> ReadValue(r, Expression.Constant(i), layout.Types[i], slot, readerType, isDbNull, getValue, cell);

		MethodCallExpression? IsNull(int i, Slot slot)
			=> NullTest(r, Expression.Constant(i), layout.Types[i], slot, readerType, isDbNull, getValue, cell);

		return Compile<TReader>(
			r,
			layout.Names,
			(i, slot) => ConverterOf(slot) is { } converter
				? Converted(converter, slot, Read(i, converter.Column), IsNull(i, converter.Column))
				: Read(i, slot),
			(i, slot) => ConverterOf(slot) is { } converter
				? SkipsOnNull(slot, IsNull(i, converter.Column))
				: IsNull(i, slot),
			cell);
	}

	/// <summary>
	/// Returns the slot's converter, or <see langword="null"/> for a slot without one
	/// (every slot of a map nothing was converted in).
	/// </summary>
	Converter? ConverterOf(Slot slot) => _converters?[_slots.IndexOf(slot)];

	/// <summary>
	/// Returns what a slot with a converter reads:
	/// the delegate applied to the column read as the delegate's own parameter type.
	/// </summary>
	/// <remarks>
	/// The column is a slot of that type (<c>Converter.Column</c>), so it is read, and its NULL judged, as a member of that type is:
	/// only a <see cref="Nullable{T}"/> is given NULL (<c>columnIsNull</c> is <see langword="null"/> then),
	/// and what the delegate returns is the value of the member, a non-nullable one's too.
	/// For anything else the delegate is not called on NULL:
	/// the slot is skipped (<c>SkipsOnNull</c>) when the member keeps its value on NULL,
	/// and is otherwise <see langword="default"/>, <see langword="null"/> for a nullable member.
	/// </remarks>
	static Expression Converted(Converter converter, Slot slot, Expression column, Expression? columnIsNull)
	{
		Expression value = converter.Invoke(column, slot.Type);
		return columnIsNull is not null && !slot.KeepsValueOnNull
			? Expression.Condition(columnIsNull, Expression.Default(slot.Type), value)
			: value;
	}

	/// <summary>
	/// Returns the test that leaves a slot with a converter alone when the cell is NULL, or <see langword="null"/> for none.
	/// </summary>
	/// <remarks>
	/// A property keeps its value then, and a constructor parameter gets its fallback.
	/// </remarks>
	static Expression? SkipsOnNull(Slot slot, Expression? columnIsNull) => slot.KeepsValueOnNull ? columnIsNull : null;

	/// <summary>
	/// Builds the reader of a single value (<c>FirstColumn&lt;T&gt;</c>):
	/// column 0 read exactly as a property of type <typeparamref name="T"/> is,
	/// with the same typed getter, conversion and NULL rule.
	/// </summary>
	/// <remarks>
	/// A NULL into a type the C# type declares non-nullable is <c>default(T)</c>;
	/// any other NULL is <see langword="null"/>.
	/// It is built from the helpers below, which the single-column reader also uses.
	/// </remarks>
	/// <typeparam name="TReader">The type of the reader.</typeparam>
	/// <param name="columnType">The type the reader reports for column 0.</param>
	/// <returns>The delegate that reads column 0 of the current row.</returns>
	internal static Func<TReader, T> FirstColumnReader<TReader>(Type columnType)
		where TReader : class, IDataRecord
	{
		Type readerType = typeof(TReader);
		MethodInfo isDbNull = readerType.GetMethod(
			nameof(IDataRecord.IsDBNull),
			[typeof(int)])!;
		MethodInfo getValue = readerType.GetMethod(
			nameof(IDataRecord.GetValue),
			[typeof(int)])!;

		ParameterExpression r = Expression.Parameter(readerType, "r");
		ParameterExpression cell = Expression.Variable(typeof(object), "cell");
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		Expression ordinal = Expression.Constant(0);
		var slot = new Slot(typeof(T));

		Expression value = ReadValue(
			r,
			ordinal,
			columnType,
			slot,
			readerType,
			isDbNull,
			getValue,
			cell);
		if (NullTest(
			r,
			ordinal,
			columnType,
			slot,
			readerType,
			isDbNull,
			getValue,
			cell) is { } isNull)
		{
			value = Expression.Condition(
				isNull,
				Expression.Default(typeof(T)),
				value);
		}

		Expression guarded = Expression.TryCatch(
			value,
			Expression.Catch(
				ex,
				Expression.Condition(
					Expression.OrElse(
						Expression.TypeIs(ex, typeof(DbException)),
						Expression.TypeIs(
							ex,
							typeof(OperationCanceledException))),
					Expression.Rethrow(typeof(T)),
					Expression.Throw(
						Expression.Call(
							typeof(RowMap<T>).GetMethod(
								nameof(FirstColumnError),
								BindingFlags.Static | BindingFlags.NonPublic)!,
							ex,
							r),
						typeof(T)))));

		return Expression
			.Lambda<Func<TReader, T>>(
				Expression.Block(typeof(T), [cell], guarded),
				r)
			.Compile();
	}

	static ValueMappingException FirstColumnError(
		Exception ex,
		IDataRecord record)
	{
		string column;
		try { column = record.GetName(0); }
		catch (Exception) { column = ""; } // Nothing to name: the failure being reported matters more.

		return new ValueMappingException(
			$"Unable to read column '{column}' as {Shape.TypeName(typeof(T))}.",
			"",
			column,
			typeof(T),
			ex);
	}

	/// <summary>
	/// Converts the scalar of a command (<c>ExecuteScalar&lt;T&gt;</c>, <c>ExecuteReturn&lt;T&gt;</c>):
	/// a value that has arrived boxed, converted as column 0 of <c>Map&lt;T&gt;()</c> is.
	/// </summary>
	/// <remarks>
	/// <para>
	/// NULL, <see cref="DBNull"/> and no row are <c>default(T)</c>;
	/// a value that is already a <typeparamref name="T"/> is returned as it is;
	/// any other value takes the conversion <c>Map&lt;T&gt;()</c> gives a column of that
	/// type (a checked integer conversion, a widening to a fractional type,
	/// else <c>ConvertValue</c>), chosen by the runtime type of the value,
	/// which is the type the reader reports for the column.
	/// </para>
	/// <para>
	/// It is built from the same helpers (<c>TypedGetters</c>, <c>TryConvert</c>, <c>ConvertValue</c>),
	/// the same ones the single-column reader uses; the two agree for every provider tested, which the parity matrix checks.
	/// </para>
	/// </remarks>
	/// <param name="value">The scalar as it arrived.</param>
	/// <returns>The value as a <typeparamref name="T"/>.</returns>
	/// <exception cref="ValueMappingException">The value cannot be converted: it is the exception the column read gives.</exception>
	internal static T ConvertScalar(object? value)
	{
		if (value is null or DBNull) return default!;
		if (value is T exact) return exact;

		try
		{
			Func<object, T> convert
				= Scalars.GetOrAdd(value.GetType(), BuildScalar);
			return convert(value);
		}
		catch (Exception ex)
		{
			throw new ValueMappingException(
				$"Unable to read the scalar as {Shape.TypeName(typeof(T))}.",
				"",
				"",
				typeof(T),
				ex);
		}
	}

	/// <summary>
	/// The converters of the scalars, one per runtime type of the value:
	/// as many as the types a provider returns for one <typeparamref name="T"/>.
	/// </summary>
	static readonly ConcurrentDictionary<Type, Func<object, T>> Scalars = new();

	/// <summary>
	/// Builds the converter of the scalars of one runtime type:
	/// the typed read of <c>Map&lt;T&gt;()</c> (<c>TypedRead</c> and <c>ReadValue</c>) applied to an unboxed value.
	/// </summary>
	/// <remarks>
	/// A type with no direct conversion to <typeparamref name="T"/> takes the general path.
	/// </remarks>
	static Func<object, T> BuildScalar(Type valueType)
	{
		Type? underlying = Nullable.GetUnderlyingType(typeof(T));
		ParameterExpression input = Expression.Parameter(typeof(object), "value");

		if (TypedGetters.ByType.ContainsKey(valueType)
			&& TryConvert(
				Expression.Convert(input, valueType),
				underlying ?? typeof(T)) is { } typed)
		{
			return Expression
				.Lambda<Func<object, T>>(
					underlying is null
						? typed
						: Expression.Convert(typed, typeof(T)),
					input)
				.Compile();
		}

		return static value => ConvertValue<T>(value);
	}

	/// <summary>
	/// Builds the compiled row reader that both mappers share.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A model built as it always was (a class with a public parameterless constructor, a struct with no constructor of its own)
	/// is built as <c>new T()</c>, then for each column that maps to a property, <c>item.Property = &lt;cell&gt;</c>.
	/// A model built through a constructor reads every fed cell, in column order, into a typed local,
	/// then calls <c>new T(arguments)</c>, then sets the properties no parameter covers.
	/// Only how a cell is read differs (a live reader vs buffered <c>object[]</c> rows).
	/// </para>
	/// <para>
	/// <c>isNull</c> gives, for a property that must keep its value on NULL, the test that the cell is NULL,
	/// which wraps the assignment as <c>if (!isNull) item.Property = &lt;cell&gt;</c>;
	/// it may leave the raw cell in <c>cell</c>, which <c>readCell</c> then converts.
	/// </para>
	/// </remarks>
	Func<TArg, T> Compile<TArg>(ParameterExpression arg, string[] names, Func<int, Slot, Expression> readCell,
		Func<int, Slot, Expression?> isNull, ParameterExpression? cell = null)
	{
		ParameterExpression item = Expression.Variable(typeof(T), "item");
		ParameterExpression index = Expression.Variable(typeof(int), "index");

		var bindings = new Binding?[names.Length];
		var locals = new List<ParameterExpression>();
		Slot?[] feeds = Match(names);
		RequireColumns(feeds);

		List<Expression> statements = _factory is { } factory
			? BindConstructor(_slots.Length, arguments => CallFactory(factory, arguments), item, index, bindings, names, feeds, readCell, isNull, locals)
			: TypeShape.Constructor is { } constructor
				? BindConstructor(constructor.GetParameters().Length, arguments => Expression.New(constructor, arguments), item, index, bindings, names, feeds, readCell, isNull, locals)
				: CreateThenSet(item, index, bindings, names, feeds, readCell, isNull);

		return Expression.Lambda<Func<TArg, T>>(Guard(statements, item, index, cell, locals, bindings), arg).Compile();
	}

	/// <summary>
	/// Throws when a required property, or a member that declares its column, has no column in this result shape.
	/// </summary>
	/// <remarks>
	/// Every required property needs a column in this result shape, whatever else the constructor could pass
	/// (the default of a parameter never satisfies it).
	/// It is raised when the plan is built: when the first row of a shape is mapped for a reader
	/// (so a query with no rows never reports it), and as soon as the columns are known for the buffered, Channel and Dataflow routes.
	/// The column named is the one looked for: a pair's, else the property's own name.
	/// A member that declares its column (<c>[Column(Name)]</c>) needs it the same way, unless a pair names the member: only what the map says is looked for then.
	/// </remarks>
	/// <exception cref="ValueMappingException">A required property, or a member that declares its column, has no column among the ones that feed a slot.</exception>
	void RequireColumns(Slot?[] feeds)
	{
		for (int s = 0; s < _slots.Length; s++)
		{
			Slot slot = _slots[s];
			// A pair that names a declared member overrides the declaration: only what the map says is looked for.
			if ((!slot.Required && (slot.ColumnName is null || _named[s])) || Array.IndexOf(feeds, slot) >= 0) continue;

			string column = _columns[s]!; // A required slot is never mapped to null (the pairs are validated), and a declared one is not either.
			throw new ValueMappingException(
				slot.Required
					? $"No column feeds the required property '{slot.Name}' ({Shape.TypeName(slot.Type)}) of '{Shape.TypeName(typeof(T))}'; expected a column named '{column}'."
					: $"No column feeds the {Describe(slot)} of '{Shape.TypeName(typeof(T))}', which is declared [Column(\"{column}\")]; expected a column named '{column}'.",
				slot.Name, column, slot.Type);
		}
	}

	/// <summary>
	/// Builds the call of the factory with the arguments.
	/// </summary>
	/// <remarks>
	/// The factory is a constant of the expression tree, as a converter's delegate is:
	/// held once by the compiled row reader, and calling it allocates nothing
	/// (<c>FactoryMapTests.TheFactoryRoute_AllocatesNothingPerRow_BeyondTheConstructorRoute</c>).
	/// What it returns is a <typeparamref name="T"/>, or a type assignable to <typeparamref name="T"/> (checked when the map is built).
	/// </remarks>
	static Expression CallFactory(Delegate factory, Expression[] arguments)
	{
		Expression call = Expression.Invoke(Expression.Constant(factory, factory.GetType()), arguments);
		return call.Type == typeof(T) ? call : Expression.Convert(call, typeof(T));
	}

	/// <summary>
	/// Builds the statements of a model created as it always was.
	/// </summary>
	/// <remarks>
	/// It is <c>new T()</c>, then each cell is read and assigned in column order, with no locals.
	/// </remarks>
	static List<Expression> CreateThenSet(ParameterExpression item, ParameterExpression index, Binding?[] bindings, string[] names, Slot?[] feeds,
		Func<int, Slot, Expression> readCell, Func<int, Slot, Expression?> isNull)
	{
		var statements = new List<Expression>(names.Length * 2 + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			bindings[i] = new Binding(slot.Name, names[i], slot.Type, null);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));
			Expression assign = Expression.Call(item, slot.Setter!, readCell(i, slot));
			statements.Add(isNull(i, slot) is { } skip ? Expression.IfThen(Expression.Not(skip), assign) : assign);
		}

		statements.Add(item);
		return statements;
	}

	/// <summary>
	/// Builds the statements of a model created through a constructor.
	/// </summary>
	/// <remarks>
	/// A constructor needs its arguments before anything can be set, and a <c>SequentialAccess</c> reader allows each cell to be read once, in ascending order:
	/// so every fed cell is read, in column order, into a typed local (nothing is boxed on the typed path),
	/// the constructor is called with the locals (the fallback for a parameter no column feeds, and for a NULL into a member the type declares non-nullable),
	/// and then the properties the constructor does not cover are set from their locals, a NULL skipping one the type declares non-nullable.
	/// </remarks>
	List<Expression> BindConstructor(int parameterCount, Func<Expression[], Expression> create, ParameterExpression item, ParameterExpression index, Binding?[] bindings, string[] names, Slot?[] feeds,
		Func<int, Slot, Expression> readCell, Func<int, Slot, Expression?> isNull, List<ParameterExpression> locals)
	{
		var arguments = new Expression[parameterCount];
		var statements = new List<Expression>(names.Length * 2 + 4);
		var sets = new List<Expression>();

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			bindings[i] = new Binding(slot.Name, names[i], slot.Type, slot.Parameter);
			statements.Add(Expression.Assign(index, Expression.Constant(i)));

			ParameterExpression local = Expression.Variable(slot.Type, "value" + i);
			locals.Add(local);
			Expression? skip = isNull(i, slot);
			Expression value = readCell(i, slot);

			if (slot.Setter is null)
			{
				// A NULL into a parameter the type declares non-nullable (a value type, or a reference with nullable annotations on) is not a value: the parameter gets its fallback. Any other NULL is null, as it is for a property.
				Debug.Assert(slot.Fallback is not null && arguments[slot.Position] is null);
				statements.Add(Expression.Assign(local, skip is null ? value : Expression.Condition(skip, slot.Fallback!, value)));
				arguments[slot.Position] = local;
			}
			else if (skip is null)
			{
				statements.Add(Expression.Assign(local, value));
				sets.Add(Expression.Assign(index, Expression.Constant(i)));
				sets.Add(Expression.Call(item, slot.Setter, local));
			}
			else
			{
				ParameterExpression isNullCell = Expression.Variable(typeof(bool), "null" + i);
				locals.Add(isNullCell);
				statements.Add(Expression.Assign(isNullCell, skip));
				statements.Add(Expression.Assign(local, Expression.Condition(isNullCell, Expression.Default(slot.Type), value)));
				sets.Add(Expression.Assign(index, Expression.Constant(i)));
				sets.Add(Expression.IfThen(Expression.Not(isNullCell), Expression.Call(item, slot.Setter, local)));
			}
		}

		foreach (Slot slot in _slots)
		{
			if (slot.Setter is null && arguments[slot.Position] is null) arguments[slot.Position] = slot.Fallback!; // No column feeds it.
		}

		// A constructor that throws is not the fault of the last column read: the index says no column.
		statements.Add(Expression.Assign(index, Expression.Constant(-1)));
		statements.Add(Expression.Assign(item, create(arguments)));
		statements.AddRange(sets);
		statements.Add(item);
		return statements;
	}

	/// <summary>
	/// Wraps the statements so a failure names the offending property and column.
	/// </summary>
	/// <remarks>
	/// <c>index</c> tracks the column being read.
	/// </remarks>
	static BlockExpression Guard(List<Expression> statements, ParameterExpression item, ParameterExpression index, ParameterExpression? cell, List<ParameterExpression> locals, Binding?[] bindings)
	{
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		var variables = new List<ParameterExpression> { item, index };
		if (cell is not null) variables.Add(cell);
		variables.AddRange(locals);
		return Expression.Block(
			typeof(T),
			variables,
			Expression.Assign(index, Expression.Constant(-1)),
			Expression.TryCatch(
				Expression.Block(typeof(T), statements),
				Expression.Catch(
					ex,
					// The provider's own failures and cancellation propagate untouched; everything else is wrapped with
					// the property and column that were being read.
					Expression.Condition(
						Expression.OrElse(
							Expression.TypeIs(ex, typeof(DbException)),
							Expression.TypeIs(ex, typeof(OperationCanceledException))),
						Expression.Rethrow(typeof(T)),
						Expression.Throw(
							Expression.Call(
								typeof(RowMap<T>).GetMethod(nameof(MapError), BindingFlags.Static | BindingFlags.NonPublic)!,
								ex, index, Expression.Constant(bindings)),
							typeof(T))))));
	}

	/// <summary>
	/// Returns the typed (unboxed) read of a cell as the target type,
	/// or <see langword="null"/> when the column type has no typed getter or does not convert directly.
	/// </summary>
	static Expression? TypedRead(ParameterExpression r, Expression ordinal, Type columnType, Type target, Type readerType)
	{
		if (!TypedGetters.ByType.TryGetValue(columnType, out string? getterName)) return null;

		MethodInfo getter = readerType.GetMethod(getterName, [typeof(int)])!;
		return TryConvert(Expression.Call(r, getter, ordinal), target);
	}

	/// <summary>
	/// Returns the expression that is <see langword="true"/> when the cell of a slot that keeps its value on NULL (<c>Slot.KeepsValueOnNull</c>) is NULL,
	/// or <see langword="null"/> for any other slot.
	/// </summary>
	/// <remarks>
	/// On the typed path the reader answers; on the general path the cell is read once into <c>cell</c> and <c>ReadValue</c> converts that same value.
	/// </remarks>
	static MethodCallExpression? NullTest(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue, ParameterExpression cell)
	{
		if (!slot.KeepsValueOnNull) return null;

		if (TypedRead(r, ordinal, columnType, slot.Type, readerType) is not null)
			return Expression.Call(r, isDbNull, ordinal);

		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!,
			Expression.Assign(cell, Expression.Call(r, getValue, ordinal)));
	}

	static Expression ReadValue(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue,
		ParameterExpression? cell = null)
	{
		Type propertyType = slot.Type;
		Type? underlying = Nullable.GetUnderlyingType(propertyType);
		Type target = underlying ?? propertyType;
		bool nullable = !slot.KeepsValueOnNull; // Nullable<T>, or a reference the C# type does not declare non-nullable: a NULL is null.

		if (TypedRead(r, ordinal, columnType, target, readerType) is { } typed)
		{
			if (!nullable) return typed;

			// NULL → null/default; otherwise the typed value (lifted to Nullable<> when needed).
			Expression present = underlying is null ? typed : Expression.Convert(typed, propertyType);
			return Expression.Condition(
				Expression.Call(r, isDbNull, ordinal),
				Expression.Default(propertyType),
				present);
		}

		// General path: the boxed value, converted. Handles enums, strings → Guid, mismatched types, unknown column types.
		// NullTest has already read the cell of a member that keeps its value on NULL into `cell` to test it: the cell is read once, so it is converted from there.
		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(propertyType),
			cell is not null && slot.KeepsValueOnNull ? cell : Expression.Call(r, getValue, ordinal));
	}

	static bool IsIntegral(Type t)
		=> t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort)
			|| t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong);

	static bool IsFractional(Type t) => t == typeof(float) || t == typeof(double) || t == typeof(decimal);

	/// <summary>
	/// Returns the expression converted to the target type, or <see langword="null"/> when the types have no direct conversion.
	/// </summary>
	/// <remarks>
	/// Direct typed conversions exist only where they cannot silently change the value:
	/// integer to integer or enum is range-checked (it throws on overflow),
	/// and integers and floating point widen to a fractional type.
	/// Three more are the exact call the general path makes, without boxing the result:
	/// an integer to <see cref="bool"/> (not zero),
	/// text to <see cref="DateTime"/> and text to <see cref="decimal"/> (parsed with the invariant culture).
	/// Anything else (fractional to integer, which must round, other text, and so on) takes the general path.
	/// </remarks>
	static Expression? TryConvert(Expression value, Type target)
	{
		Type source = value.Type;
		if (source == target) return value;

		if (target == typeof(bool) && IsIntegral(source))
			return Expression.NotEqual(value, Expression.Default(source));

		if (source == typeof(string))
		{
			if (target == typeof(DateTime))
			{
				return Expression.Call(
					typeof(DateTime).GetMethod(
						nameof(DateTime.Parse),
						[typeof(string), typeof(IFormatProvider)])!,
					value,
					Expression.Constant(
						CultureInfo.InvariantCulture,
						typeof(IFormatProvider)));
			}

			if (target == typeof(decimal))
			{
				return Expression.Call(
					typeof(decimal).GetMethod(
						nameof(decimal.Parse),
						[typeof(string), typeof(NumberStyles), typeof(IFormatProvider)])!,
					value,
					Expression.Constant(NumberStyles.Number),
					Expression.Constant(
						CultureInfo.InvariantCulture,
						typeof(IFormatProvider)));
			}
		}

		Type t = target.IsEnum ? Enum.GetUnderlyingType(target) : target;
		try
		{
			if (IsIntegral(source) && IsIntegral(t)) return Expression.ConvertChecked(value, target);
			if ((IsIntegral(source) || IsFractional(source)) && IsFractional(t)) return Expression.Convert(value, target);
		}
		catch (InvalidOperationException)
		{
			// No such conversion: fall through to the general path.
		}

		return null;
	}

	#endregion

	#region Runtime helpers called from compiled code

	static bool IsNullCell(object? value) => value is null or DBNull;

	static TTarget ConvertValue<TTarget>(object? value)
	{
		// A NULL never reaches here for a member the type declares non-nullable: the compiled readers skip that assignment.
		if (value is null or DBNull) return default!;

		if (value is TTarget exact) return exact;

		Type target = typeof(TTarget);
		Type u = Nullable.GetUnderlyingType(target) ?? target;
		return (TTarget)GetConverted(value, u);

		static object GetConverted(object value, Type u)
		{
			if (u.IsEnum)
			{
				return value is string s
					? Enum.Parse(u, s, true)
					: Enum.ToObject(u, value);
			}

			if (u == typeof(Guid))
			{
				if (value is string gs)
					return Guid.Parse(gs);

				if (value is byte[] gb)
					return new Guid(gb);

				throw new InvalidCastException($"Cannot convert '{value.GetType()}' to Guid.");
			}

			if (u == typeof(string))
			{
				if (value is byte[])
					throw new InvalidCastException("A byte array cannot be assigned to a string; map it to byte[] or convert it explicitly.");

				return System.Convert.ToString(value, CultureInfo.InvariantCulture)!;
			}

			// The date and time types are not IConvertible, so Convert.ChangeType cannot read them from what a provider returns: a DateTime for a date column,
			// a TimeSpan for a time column, text from SQLite. A value that is already the target type never gets here.
			if (u == typeof(DateTimeOffset))
			{
				return value switch
				{
					DateTime dt => new DateTimeOffset(dt),
					string ds => DateTimeOffset.Parse(ds, CultureInfo.InvariantCulture),
					_ => throw new InvalidCastException($"Cannot convert '{value.GetType()}' to DateTimeOffset."),
				};
			}

			if (u == typeof(TimeSpan))
			{
				return value switch
				{
					DateTime dt => dt.TimeOfDay,
					string ts => TimeSpan.Parse(ts, CultureInfo.InvariantCulture),
					_ => throw new InvalidCastException($"Cannot convert '{value.GetType()}' to TimeSpan."),
				};
			}

#if NET10_0_OR_GREATER
			if (u == typeof(DateOnly))
			{
				return value switch
				{
					DateTime dt => DateOnly.FromDateTime(dt),
					DateTimeOffset dto => DateOnly.FromDateTime(dto.DateTime),
					string ds => DateOnly.Parse(ds, CultureInfo.InvariantCulture),
					_ => throw new InvalidCastException($"Cannot convert '{value.GetType()}' to DateOnly."),
				};
			}

			if (u == typeof(TimeOnly))
			{
				return value switch
				{
					TimeSpan ts => TimeOnly.FromTimeSpan(ts),
					DateTime dt => TimeOnly.FromDateTime(dt),
					DateTimeOffset dto => TimeOnly.FromDateTime(dto.DateTime),
					string text => TimeOnly.Parse(text, CultureInfo.InvariantCulture),
					_ => throw new InvalidCastException($"Cannot convert '{value.GetType()}' to TimeOnly."),
				};
			}
#endif

			return System.Convert.ChangeType(value, u, CultureInfo.InvariantCulture);
		}
	}

	static Exception MapError(Exception ex, int index, Binding?[] bindings)
	{
		if (index >= 0 && index < bindings.Length && bindings[index] is { } b)
		{
			string message = b.Parameter is { } parameter
				? $"Unable to pass value of column '{b.Column}' to constructor parameter '{parameter}' ({Shape.TypeName(b.Type)}) of '{Shape.TypeName(typeof(T))}'."
				: $"Unable to set value of property '{b.Property}' ({Shape.TypeName(b.Type)}) from column '{b.Column}'.";

			return new ValueMappingException(message, b.Property, b.Column, b.Type, ex);
		}

		return ex is InvalidOperationException ? ex : new InvalidOperationException($"Unable to create '{typeof(T)}' from the current row.", ex);
	}

	#endregion
}

/// <summary>
/// Holds the typed getter of each type a column can be read as without boxing.
/// </summary>
/// <remarks>
/// It does not depend on the model type, so it lives here, not in <see cref="RowMap{T}"/>,
/// where a static field is created once per model type.
/// </remarks>
static class TypedGetters
{
	internal static readonly FrozenDictionary<Type, string> ByType = new Dictionary<Type, string>()
	{
		[typeof(bool)] = nameof(IDataRecord.GetBoolean),
		[typeof(byte)] = nameof(IDataRecord.GetByte),
		[typeof(char)] = nameof(IDataRecord.GetChar),
		[typeof(short)] = nameof(IDataRecord.GetInt16),
		[typeof(int)] = nameof(IDataRecord.GetInt32),
		[typeof(long)] = nameof(IDataRecord.GetInt64),
		[typeof(float)] = nameof(IDataRecord.GetFloat),
		[typeof(double)] = nameof(IDataRecord.GetDouble),
		[typeof(decimal)] = nameof(IDataRecord.GetDecimal),
		[typeof(DateTime)] = nameof(IDataRecord.GetDateTime),
		[typeof(Guid)] = nameof(IDataRecord.GetGuid),
		[typeof(string)] = nameof(IDataRecord.GetString),
	}.ToFrozenDictionary();
}

/// <summary>
/// The row source of a single-value model type (<c>Shape.IsScalar</c>):
/// column 0 of every row, read as a property of type <typeparamref name="T"/> is,
/// whatever the column is named, other columns ignored.
/// </summary>
/// <remarks>
/// There is one instance per <typeparamref name="T"/> (<see langword="null"/> for any other <typeparamref name="T"/>, so <c>Map&lt;T&gt;()</c> branches on it).
/// Like a property, the reader is chosen once per result set,
/// from the column's reported type;
/// the cache is keyed by that type alone,
/// so there is no name matching to pay for.
/// </remarks>
/// <typeparam name="T">The type of the single value.</typeparam>
internal sealed class FirstColumn<T> : IRowSource<T>
{
	internal static readonly FirstColumn<T>? Instance
		= Shape.IsScalar(typeof(T)) ? new() : null;

	sealed class Entry(Type columnType)
	{
		public readonly Type ColumnType = columnType;
		public Func<DbDataReader, T>? Db;
		public Func<IDataRecord, T>? Rec;
	}

	readonly ConcurrentDictionary<Type, Entry> _entries = new();
	Entry? _last;

	Entry Resolve(IDataRecord reader)
	{
		Entry? last = _last;
		Type columnType = reader.GetFieldType(0) ?? typeof(object); // Tolerant of a provider that reports no type, as the layout of a model is.
		return last is not null && last.ColumnType == columnType
			? last
			: _last = _entries.GetOrAdd(columnType, static t => new Entry(t));
	}

	public Func<DbDataReader, T> Mapper(DbDataReader reader)
	{
		Entry e = Resolve(reader);
		return e.Db ??= RowMap<T>.FirstColumnReader<DbDataReader>(e.ColumnType);
	}

	public Func<IDataRecord, T> Mapper(IDataRecord reader)
	{
		Entry e = Resolve(reader);
		return e.Rec ??= RowMap<T>.FirstColumnReader<IDataRecord>(e.ColumnType);
	}
}

/// <summary>
/// Represents what one column can feed: a constructor parameter, or a settable public instance property of the model.
/// </summary>
/// <remarks>
/// <para>
/// Slots are held once per model type in an <see cref="ImmutableArray{T}"/> in place of <see cref="PropertyInfo"/>,
/// so the matcher and the compiled readers see only a name, the type a value must become and where it goes.
/// It does not depend on the model type either.
/// </para>
/// <para>
/// A slot fed through the constructor has a <c>Position</c> and no <c>Setter</c>;
/// a parameter and a property of the same name (ignoring case) are one such slot, so the property is never set again.
/// </para>
/// </remarks>
sealed class Slot
{
	/// <summary>Initializes a slot for a settable property.</summary>
	public Slot(string name, Type type, MethodInfo setter, bool required, bool nonNullableReference, string? columnName = null, int order = -1, bool notMapped = false)
	{
		Name = name;
		ColumnName = columnName;
		Order = order;
		NotMapped = notMapped;
		Required = required;
		Type = type;
		Setter = setter;
		Position = -1;
		IsProperty = true;
		KeepsValueOnNull = KeepsOnNull(type, nonNullableReference);
	}

	/// <summary>Initializes a slot for the one value of a single-value model type (<c>FirstColumn&lt;T&gt;</c>): no name, nothing to set.</summary>
	public Slot(Type type) : this(type, false) { }

	/// <summary>
	/// Initializes a slot for a value that is not a member (the column a converter is given):
	/// a reference type of it is declared non-nullable when told so.
	/// </summary>
	public Slot(Type type, bool nonNullableReference)
	{
		Name = "";
		Type = type;
		Position = -1;
		KeepsValueOnNull = KeepsOnNull(type, nonNullableReference);
	}

	/// <summary>Initializes a slot for a constructor parameter, named after the property of that name when there is one.</summary>
	public Slot(string name, Type type, int position, string parameter, bool isProperty, Expression fallback, bool required, bool nonNullableReference, string? columnName = null, int order = -1)
	{
		Name = name;
		ColumnName = columnName;
		Order = order;
		Required = required;
		Type = type;
		Position = position;
		Parameter = parameter;
		IsProperty = isProperty;
		Fallback = fallback;
		KeepsValueOnNull = KeepsOnNull(type, nonNullableReference);
	}

	/// <summary>
	/// Returns whether a NULL leaves a member of this type alone.
	/// </summary>
	/// <remarks>
	/// A NULL is not a value for a member the C# type declares non-nullable:
	/// a value type other than <see cref="Nullable{T}"/> (an enum counts), or a reference type declared non-nullable (<c>NullAnnotations</c>).
	/// A property then keeps what the constructor or its initializer gave it, and a constructor parameter gets its fallback.
	/// It is decided once per model type, here, never per row.
	/// </remarks>
	static bool KeepsOnNull(Type type, bool nonNullableReference)
		=> type.IsValueType ? Nullable.GetUnderlyingType(type) is null : nonNullableReference;

	/// <summary>
	/// Whether a NULL cell leaves the member alone (see <c>KeepsOnNull</c>) rather than assigning <see langword="null"/>;
	/// the compiled readers test the cell only for such a slot.
	/// </summary>
	public readonly bool KeepsValueOnNull;

	public readonly string Name;
	public readonly Type Type;

	/// <summary>
	/// The column the member declares it reads (<c>[Column(Name)]</c>; see <c>Decoration</c>), or <see langword="null"/> for none.
	/// </summary>
	/// <remarks>
	/// A slot that declares a column or an <see cref="Order"/> is a hard instruction: the result must satisfy it, or the row reader is not built.
	/// </remarks>
	public readonly string? ColumnName;

	/// <summary>
	/// The ordinal the member declares its column must have (<c>[Column(Order = n)]</c>; see <c>Decoration</c>), or -1 for none.
	/// </summary>
	public readonly int Order = -1;

	/// <summary>
	/// Whether the member is <c>[NotMapped]</c>: it reads no column on its own (no tier matches it).
	/// </summary>
	/// <remarks>
	/// It still has a slot, so that a pair can name it and map it all the same.
	/// </remarks>
	public readonly bool NotMapped;

	/// <summary>The method that stores the value; <see langword="null"/> for a slot fed through the constructor.</summary>
	public readonly MethodInfo? Setter;

	/// <summary>The position of the constructor parameter; -1 for a property that is set.</summary>
	public readonly int Position;
	/// <summary>The name of the constructor parameter; <see langword="null"/> for a property that is set.</summary>
	public readonly string? Parameter;

	/// <summary>Whether a public property of this name exists (it may be get-only, as in a readonly struct), for the messages.</summary>
	public readonly bool IsProperty;

	/// <summary>What the constructor receives when no column feeds the parameter: its declared default, else <c>default(TParam)</c>.</summary>
	public readonly Expression? Fallback;

	/// <summary>
	/// Whether a column must feed it in every result shape:
	/// a required property, unless the constructor sets the required members.
	/// A constructor parameter's default does not count as a feed.
	/// </summary>
	public readonly bool Required;

	/// <summary>
	/// Returns the expression of what the constructor receives for a parameter no column feeds.
	/// </summary>
	/// <remarks>
	/// <see cref="ParameterInfo"/> reports a <see langword="null"/> default for <c>DateTime d = default</c> (and .NET Framework throws <see cref="FormatException"/> reading it),
	/// and the underlying integer for a nullable enum,
	/// so a <see langword="null"/> stands for <c>default(TParam)</c>, and an enum is converted back.
	/// </remarks>
	public static Expression FallbackOf(ParameterInfo parameter)
	{
		Type type = parameter.ParameterType;
		object? value = null;
		try
		{
			if (parameter.HasDefaultValue) value = parameter.DefaultValue;
		}
		catch (FormatException)
		{
			// .NET Framework cannot read the default of a struct parameter set to `default`: that is default(TParam).
		}

		if (value is null or DBNull or Missing) return Expression.Default(type);

		Type underlying = Nullable.GetUnderlyingType(type) ?? type;
		if (underlying.IsEnum && value.GetType() != underlying) value = Enum.ToObject(underlying, value);
		return Expression.Constant(value, type);
	}
}

/// <summary>
/// Represents the converter of one slot: the delegate a caller gave <c>RowMap&lt;T&gt;.Convert</c>, as the compiled row readers call it.
/// </summary>
/// <remarks>
/// There is one per <c>Convert</c> call, shared by every row, layout and thread of its map (the delegate is only called, never changed).
/// It does not depend on the model type, so it is not generic.
/// </remarks>
/// <param name="function">The delegate.</param>
/// <param name="functionType">The type the delegate was declared as.</param>
/// <param name="column">The type the delegate receives.</param>
/// <param name="member">The type the delegate returns.</param>
sealed class Converter(
	Delegate function,
	Type functionType,
	Type column,
	Type member)
{
	/// <summary>The column as a slot of the type the delegate receives, so the readers read it and judge its NULL as they do a member of that type.</summary>
	/// <remarks>
	/// A NULL is a value only for a <see cref="Nullable{T}"/>:
	/// a reference type is declared non-nullable here, as its annotations do not exist at run time
	/// (a <c>Func&lt;string?, int&gt;</c> is a <c>Func&lt;string, int&gt;</c>).
	/// </remarks>
	public readonly Slot Column = new(column, true);

	/// <summary>
	/// Returns the call of the delegate, typed as the delegate was declared and converted to the type of the slot when that is not the one the delegate returns.
	/// </summary>
	/// <remarks>
	/// A delegate converted by variance keeps its own type.
	/// The delegate is a constant of the expression tree: it is held once by the compiled row reader, and calling it allocates nothing.
	/// </remarks>
	public Expression Invoke(Expression value, Type slotType)
	{
		Expression call = Expression.Invoke(Expression.Constant(function, functionType), value);
		return member == slotType ? call : Expression.Convert(call, slotType);
	}
}

/// <summary>
/// Decides whether a property or a constructor parameter is a reference type the C# source declares non-nullable,
/// which is when a NULL must not overwrite it (<c>Slot.KeepsValueOnNull</c>).
/// </summary>
/// <remarks>
/// <para>
/// The compiler records nullable annotations in metadata:
/// <c>NullableAttribute</c> on a member (or a parameter) whose annotation differs from its context,
/// else <c>NullableContextAttribute</c> on the nearest enclosing method or type that has one.
/// Flag 1 is not-null, 2 is nullable, 0 or none is oblivious (nullable annotations off): only 1 counts.
/// </para>
/// <para>
/// It reads those attributes as data, by full name, so it works on every target
/// (<c>NullabilityInfoContext</c> does not exist on netstandard) and no attribute type is referenced.
/// </para>
/// <para>
/// A property marked <c>[AllowNull]</c> (on the property or on its setter's value parameter) says <see langword="null"/> may be written to it, so it is nullable.
/// A member whose declared type is a type parameter (<c>T Value</c> in <c>Box&lt;T&gt;</c>) is not judged: it is as oblivious as it was.
/// </para>
/// <para>
/// This runs once per model type, when the slots are built, never per row.
/// </para>
/// </remarks>
static class NullAnnotations
{
	const string NullableAttributeName = "System.Runtime.CompilerServices.NullableAttribute";
	const string NullableContextAttributeName = "System.Runtime.CompilerServices.NullableContextAttribute";
	const string AllowNullAttributeName = "System.Diagnostics.CodeAnalysis.AllowNullAttribute";
	const BindingFlags AnyDeclared = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

	public static bool IsNonNullableReference(PropertyInfo property)
	{
		if (!IsReference(property.PropertyType) || DeclaredAsTypeParameter(property)) return false;

		IList<CustomAttributeData> own = CustomAttributeData.GetCustomAttributes(property);
		return !Has(own, AllowNullAttributeName) && !AllowsNullOnSetter(property) && (Flag(own, NullableAttributeName) ?? ContextFlag(property)) == 1;
	}

	/// <summary>
	/// Returns whether the setter of the property allows <see langword="null"/> through <c>[AllowNull]</c>.
	/// </summary>
	/// <remarks>
	/// The compiler puts <c>[AllowNull]</c> written on a property on the value parameter of its setter (an auto-property has it nowhere else), which is also where the runtime reads it.
	/// </remarks>
	static bool AllowsNullOnSetter(PropertyInfo property)
		=> property.SetMethod?.GetParameters() is { Length: > 0 } parameters && Has(CustomAttributeData.GetCustomAttributes(parameters[^1]), AllowNullAttributeName);

	public static bool IsNonNullableReference(ParameterInfo parameter)
	{
		if (!IsReference(parameter.ParameterType) || DeclaredAsTypeParameter(parameter)) return false;

		return (Flag(CustomAttributeData.GetCustomAttributes(parameter), NullableAttributeName) ?? ContextFlag(parameter.Member)) == 1;
	}

	static bool IsReference(Type type) => !type.IsValueType && !type.IsGenericParameter && !type.IsByRef && !type.IsPointer;

	/// <summary>
	/// Returns whether the member is declared with a type parameter as its type.
	/// </summary>
	/// <remarks>
	/// The members of a closed generic type report the closed type (<c>Box&lt;string&gt;.Value</c> is a <see cref="string"/>); the declaration says <c>T</c>.
	/// The declaration is found by its metadata token.
	/// </remarks>
	static bool DeclaredAsTypeParameter(PropertyInfo property)
	{
		if (property.DeclaringType is not { IsGenericType: true, IsGenericTypeDefinition: false } closed) return false;

		foreach (PropertyInfo candidate in closed.GetGenericTypeDefinition().GetProperties(AnyDeclared))
			if (candidate.MetadataToken == property.MetadataToken) return candidate.PropertyType.IsGenericParameter;

		return false;
	}

	static bool DeclaredAsTypeParameter(ParameterInfo parameter)
	{
		if (parameter.Member is not { DeclaringType: { IsGenericType: true, IsGenericTypeDefinition: false } closed } constructor) return false;

		foreach (ConstructorInfo candidate in closed.GetGenericTypeDefinition().GetConstructors(AnyDeclared))
			if (candidate.MetadataToken == constructor.MetadataToken) return candidate.GetParameters()[parameter.Position].ParameterType.IsGenericParameter;

		return false;
	}

	/// <summary>
	/// Returns the flag of the nearest <c>NullableContextAttribute</c> from the member outward through its declaring types, or <see langword="null"/> when there is none: oblivious.
	/// </summary>
	/// <remarks>
	/// A nested type's context applies to what it contains.
	/// </remarks>
	static byte? ContextFlag(MemberInfo? member)
	{
		for (; member is not null; member = member.DeclaringType)
		{
			if (Flag(CustomAttributeData.GetCustomAttributes(member), NullableContextAttributeName) is { } flag) return flag;
		}

		return null;
	}

	/// <summary>
	/// Returns the flag of the attribute by that name.
	/// </summary>
	/// <remarks>
	/// Its one argument is a <see cref="byte"/>, or a <c>byte[]</c> with one flag per type in a generic or array type, whose first is the type itself.
	/// </remarks>
	static byte? Flag(IList<CustomAttributeData> attributes, string attributeName)
	{
		foreach (CustomAttributeData attribute in attributes)
		{
			if (attribute.AttributeType.FullName != attributeName || attribute.ConstructorArguments.Count != 1) continue;

			switch (attribute.ConstructorArguments[0].Value)
			{
				case byte flag:
					return flag;
				case IList<CustomAttributeTypedArgument> { Count: > 0 } flags when flags[0].Value is byte first:
					return first;
			}
		}

		return null;
	}

	static bool Has(IList<CustomAttributeData> attributes, string attributeName)
	{
		foreach (CustomAttributeData attribute in attributes)
			if (attribute.AttributeType.FullName == attributeName) return true;

		return false;
	}
}

/// <summary>
/// Represents what a model property declares for the map.
/// </summary>
/// <remarks>
/// <para>
/// The declarations are <c>[Column(Name)]</c> and <c>[Column(Order = n)]</c>
/// (<c>System.ComponentModel.DataAnnotations.Schema.ColumnAttribute</c>; <c>TypeName</c> is ignored) and <c>[NotMapped]</c>.
/// </para>
/// <para>
/// They are read as data by the full name of the attributes (a consumer's own copy counts, and no package is needed).
/// This runs once per model type, when the slots are built, never per row.
/// </para>
/// </remarks>
/// <param name="column">The name the column attribute gives, or <see langword="null"/> for none.</param>
/// <param name="order">The ordinal the column attribute gives, or -1 for none.</param>
/// <param name="hasColumnAttribute"><see langword="true"/> if a column attribute is there at all; otherwise, <see langword="false"/>.</param>
/// <param name="notMapped"><see langword="true"/> if the property is <c>[NotMapped]</c>; otherwise, <see langword="false"/>.</param>
/// <param name="invalid">Why a declaration is not valid, or <see langword="null"/>.</param>
readonly struct Decoration(string? column, int order, bool hasColumnAttribute, bool notMapped, string? invalid)
{
	const string ColumnAttributeName = "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute";
	const string NotMappedAttributeName = "System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute";

	/// <summary>Gets the name the column attribute gives, or <see langword="null"/> for none.</summary>
	public string? Column => column;

	/// <summary>Gets the ordinal the column attribute gives, or -1 for none.</summary>
	public int Order => order;

	/// <summary>Gets whether a column attribute is there at all.</summary>
	public bool HasColumnAttribute => hasColumnAttribute;

	/// <summary>Gets whether the property is <c>[NotMapped]</c>.</summary>
	public bool NotMapped => notMapped;

	/// <summary>
	/// Gets why a declaration is not valid, or <see langword="null"/>.
	/// </summary>
	/// <remarks>
	/// A declaration is not valid when it has an empty or white-space name or a negative ordinal: values the attribute itself refuses.
	/// A declaration is a hard rule, so it is never read as no declaration: it makes the type unmappable.
	/// </remarks>
	public string? Invalid => invalid;

	/// <summary>Gets a member that declares nothing (the default value of the struct would claim ordinal 0).</summary>
	public static Decoration None => new(null, -1, false, false, null);

	/// <summary>
	/// Reads what a property declares.
	/// </summary>
	/// <param name="property">The property to read.</param>
	/// <returns>What the property declares.</returns>
	public static Decoration Of(PropertyInfo property)
	{
		string? column = null;
		int order = -1;
		bool hasColumn = false;
		bool notMapped = false;
		string? invalid = null;
		foreach (CustomAttributeData attribute in CustomAttributeData.GetCustomAttributes(property))
		{
			string? name = attribute.AttributeType.FullName;
			if (name == NotMappedAttributeName)
			{
				notMapped = true;
			}
			else if (name == ColumnAttributeName)
			{
				hasColumn = true;
				if (attribute.ConstructorArguments.Count > 0)
				{
					if (attribute.ConstructorArguments[0].Value is string given && !string.IsNullOrWhiteSpace(given))
						column = given;
					else
						invalid ??= "a [Column] with an empty name";
				}

				foreach (CustomAttributeNamedArgument named in attribute.NamedArguments)
				{
					if (named.MemberName != "Order" || named.TypedValue.Value is not int value) continue;

					if (value >= 0)
						order = value;
					else
						invalid ??= $"a negative [Column(Order = {value})]";
				}
			}
		}

		return new(column, order, hasColumn, notMapped, invalid);
	}
}

/// <summary>
/// Decides whether a model type can be mapped at all, and how it is built, once per model type from its shape.
/// </summary>
/// <remarks>
/// <para>
/// A type that cannot be mapped fails at the call, not at the first row.
/// Reflection runs once and never throws: the reason is held as the message of the <see cref="InvalidOperationException"/> that <see cref="RowMap{T}"/> raises where every entry point passes
/// (its constructor and <see cref="RowMap{T}.Default"/>).
/// It does not depend on the model type, so it is not generic.
/// </para>
/// <para>
/// The constructor is chosen from the type alone, never from the columns, so a type is built the same way whatever the query:
/// a class with a public parameterless constructor, or a struct with no public constructor, is built as it always was (<c>new T()</c>, then its settable properties);
/// any other type is built through its one public constructor.
/// Two or more public constructors and none without parameters is an error, and so is a class with none.
/// </para>
/// <para>
/// A <see langword="required"/> property is mapped, and must be fed by a column in every result shape (<c>RowMap&lt;T&gt;.RequireColumns</c>),
/// unless the constructor the model is built through is marked <c>[SetsRequiredMembers]</c>: it promises to set them, so nothing is enforced.
/// A <see langword="required"/> public field cannot be set (fields are not mapped), so it is a type-shape error here.
/// </para>
/// <para>
/// Both attributes are matched by full name, so a consumer's own copy counts and the library names no type that netstandard lacks.
/// </para>
/// </remarks>
sealed class Shape
{
	const string RequiredMemberAttributeName = "System.Runtime.CompilerServices.RequiredMemberAttribute";
	const string SetsRequiredMembersAttributeName = "System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute";

	Shape(string? error, ConstructorInfo? constructor, bool setsRequiredMembers)
	{
		Error = error;
		Constructor = constructor;
		SetsRequiredMembers = setsRequiredMembers;
	}

	/// <summary>Why the type cannot be mapped (the message), or <see langword="null"/> when it can.</summary>
	public readonly string? Error;

	/// <summary>The constructor the model is built through, or <see langword="null"/> when it is built as it always was.</summary>
	public readonly ConstructorInfo? Constructor;

	/// <summary>Whether the constructor the model is built through (the parameterless one too) is marked <c>[SetsRequiredMembers]</c>: the required members are then ordinary properties.</summary>
	public readonly bool SetsRequiredMembers;

	public static Shape Of(Type type)
	{
		string? error = Judge(type, out ConstructorInfo? constructor, out bool setsRequiredMembers);
		return new(error, constructor, setsRequiredMembers);
	}

	public void ThrowIfUnmappable()
	{
		if (Error is not null) throw new InvalidOperationException(Error);
	}

	/// <summary>
	/// Returns the type as the messages name it:
	/// the underlying name and a <c>?</c> for a <see cref="Nullable{T}"/> (<c>Int32?</c>), else the short name (<c>Int32</c>, <c>String</c>).
	/// </summary>
	public static string TypeName(Type type) => Nullable.GetUnderlyingType(type) is { } underlying ? underlying.Name + "?" : type.Name;

	static string? Judge(Type type, out ConstructorInfo? constructor, out bool setsRequiredMembers)
	{
		constructor = null;
		setsRequiredMembers = false;
		string name = TypeName(type);

		if (IsScalar(type)) return $"'{name}' is a single value, not a row model. On a command use Map<T>() or ToListAsync<T>(), which read the first column; else ExecuteScalar<T>().";
		if (NotConcrete(type) is { } kind) return $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
		if (IsDictionaryOrDynamic(type)) return $"'{name}' cannot be mapped: it is a dictionary or a dynamic object, which has no properties to set from the columns. Map to a class or record, or use Map(selector).";

		return Choose(type, name, out constructor, out setsRequiredMembers) ?? (setsRequiredMembers ? null : RequiredField(type, name));
	}

	/// <summary>
	/// Returns whether the type is a single value, not a row: a column of it, not a model with properties.
	/// </summary>
	/// <remarks>
	/// <see cref="Nullable{T}"/> of these is one too.
	/// <c>DateOnly</c> and <c>TimeOnly</c> are matched by name,
	/// so every target framework agrees
	/// (the netstandard builds cannot name them).
	/// </remarks>
	internal static bool IsScalar(Type type)
	{
		type = Nullable.GetUnderlyingType(type) ?? type;
		return type.IsPrimitive || type.IsEnum
			|| type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset)
			|| type == typeof(TimeSpan)
			|| type == typeof(Guid)
			|| type == typeof(byte[])
			|| type.FullName is "System.DateOnly" or "System.TimeOnly";
	}

	/// <summary>
	/// Returns whether the type is a dictionary or a dynamic object, which has no properties to set: it would be built as one empty row.
	/// </summary>
	/// <remarks>
	/// Any of the interfaces that make a type one counts, the closed generic ones through the type's interface list.
	/// </remarks>
	static bool IsDictionaryOrDynamic(Type type)
	{
		if (typeof(IDictionary).IsAssignableFrom(type) || typeof(System.Dynamic.IDynamicMetaObjectProvider).IsAssignableFrom(type)) return true;

		foreach (Type implemented in type.GetInterfaces())
		{
			if (!implemented.IsGenericType) continue;
			Type definition = implemented.GetGenericTypeDefinition();
			if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>)) return true;
		}

		return false;
	}

	/// <summary>Returns what the type is when it cannot be created at all, else <see langword="null"/>.</summary>
	static string? NotConcrete(Type type)
		=> type.IsInterface ? "an interface"
		: type.ContainsGenericParameters ? "an open generic type"
		: typeof(Delegate).IsAssignableFrom(type) ? "a delegate"
		: type.IsAbstract ? "an abstract class"
		: null;

	/// <summary>
	/// Chooses the constructor a model is built through, and returns the reason there is none, or <see langword="null"/>.
	/// </summary>
	/// <remarks>
	/// The constructor is none (<see langword="null"/>) when a public parameterless constructor exists or when a struct has no public constructor
	/// (a struct always can be created, as its default value, whatever non-public constructors it declares), else the one public constructor.
	/// <see cref="Type.GetConstructors()"/> lists the public instance constructors only, and never the implicit parameterless one of a struct.
	/// </remarks>
	static string? Choose(Type type, string name, out ConstructorInfo? constructor, out bool setsRequiredMembers)
	{
		constructor = null;
		setsRequiredMembers = false;
		ConstructorInfo[] constructors = type.GetConstructors();
		if (Array.Find(constructors, c => c.GetParameters().Length == 0) is { } parameterless)
		{
			setsRequiredMembers = Has(parameterless, SetsRequiredMembersAttributeName);
			return null;
		}

		if (type.IsValueType && constructors.Length == 0) return null;
		if (constructors.Length == 0) return $"'{name}' cannot be mapped: it has no public constructor.";

		if (constructors.Length > 1)
		{
			string[] signatures = [.. constructors.Select(c => Signature(name, c)).OrderBy(s => s, StringComparer.Ordinal)];
			return $"'{name}' cannot be mapped: it has {constructors.Length} public constructors, {string.Join(", ", signatures, 0, signatures.Length - 1)} and {signatures[^1]}, and none without parameters. Make one of them non-public, or use Map(selector).";
		}

		foreach (ParameterInfo parameter in constructors[0].GetParameters())
		{
			if (parameter.ParameterType.IsByRef || parameter.ParameterType.IsPointer)
				return $"'{name}' cannot be mapped: its constructor parameter '{parameter.Name}' is passed by reference or is a pointer. Use Map(selector).";
		}

		constructor = constructors[0];
		setsRequiredMembers = Has(constructor, SetsRequiredMembersAttributeName);
		return null;
	}

	static string Signature(string name, ConstructorInfo constructor)
		=> name + "(" + string.Join(", ", constructor.GetParameters().Select(p => TypeName(p.ParameterType) + " " + p.Name)) + ")";

	/// <summary>
	/// Returns the reason a type cannot be mapped because of a public instance field marked required (an inherited one too), which a map cannot set.
	/// The first one is named.
	/// </summary>
	static string? RequiredField(Type type, string name)
	{
		foreach (FieldInfo field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
		{
			if (IsRequired(field)) return $"'{name}' cannot be mapped: its required field '{field.Name}' cannot be set. Make it a property.";
		}

		return null;
	}

	public static bool IsRequired(MemberInfo member) => Has(member, RequiredMemberAttributeName);

	/// <summary>
	/// Returns whether the member has the attribute, matched by the full name of the attribute.
	/// </summary>
	/// <remarks>
	/// A consumer's own copy of the attribute counts.
	/// It is read as data, so no attribute is instantiated.
	/// </remarks>
	static bool Has(MemberInfo member, string attributeName)
	{
		foreach (CustomAttributeData attribute in CustomAttributeData.GetCustomAttributes(member))
			if (attribute.AttributeType.FullName == attributeName) return true;

		return false;
	}
}

/// <summary>The builder behind the collection syntax of <see cref="RowMap{T}"/>: <c>RowMap&lt;Post&gt; map = [new(nameof(Post.Id), "post_id")];</c>.</summary>
public static class RowMap
{
	/// <summary>
	/// Creates and validates a <see cref="RowMap{T}"/> from (property name, column name) pairs, exactly as the constructor does.
	/// </summary>
	/// <remarks>
	/// This is what a collection expression of pairs calls; an empty one maps by name.
	/// </remarks>
	/// <typeparam name="T">The model type whose properties are mapped.</typeparam>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mappings']/*" />
	/// <returns>The new map.</returns>
	/// <include file="RowMapping.docs.xml" path="docs/members[@name='mapping-exceptions']/*" />
	public static RowMap<T> Create<T>(ReadOnlySpan<KeyValuePair<string, string?>> mappings)
		=> new(mappings);
}
