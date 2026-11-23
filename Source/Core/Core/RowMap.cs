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
/// A parameter and a public property whose names differ only in case are one member, fed once, through the constructor, and a pair names it by the property's name (exactly) or by the parameter's name (ignoring case).
/// A parameter no column feeds, or a <see langword="null"/> column ignores, receives its declared default value, else <see langword="default"/>; a parameter a column feeds receives the column's value, under the NULL rules below.
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
/// <item><description>a dictionary or a dynamic object (<c>Dictionary&lt;string, object?&gt;</c>, <see cref="System.Dynamic.ExpandoObject"/>: they have no properties to set, so use <c>AsDictionaryRows()</c> or <c>Map(selector)</c>);</description></item>
/// <item><description>a type with two or more public constructors and none without parameters;</description></item>
/// <item><description>a class with no public constructor;</description></item>
/// <item><description>a type with a <see langword="required"/> public field (a field is never mapped, so it cannot be set), unless the constructor it is built through is marked <c>[SetsRequiredMembers]</c>;</description></item>
/// <item><description>a type whose <c>[Column]</c> and <c>[NotMapped]</c> declarations cannot all be honoured (see below).</description></item>
/// </list>
/// <para>
/// A NULL into a member the C# type declares non-nullable is an error: a value type other than <see cref="Nullable{T}"/> (an enum counts), or a reference type with nullable annotations on (<c>string</c>, not <c>string?</c>).
/// The row throws <see cref="ValueMappingException"/> naming the model type and the 0-based row, with the provider's error, if it raised one, as the inner exception.
/// A member that takes NULL receives <see langword="null"/>: <see cref="Nullable{T}"/>, a reference type declared nullable (<c>string?</c>, or <c>[AllowNull]</c> on a property), a reference type in code with nullable annotations off, and a member declared as a type parameter.
/// A <see cref="NullPolicyAttribute"/> on a member or on <typeparamref name="T"/> says what a NULL does instead, whatever the member's type,
/// and a <see cref="NullValueAttribute"/> on a member replaces a NULL with its value, over any policy;
/// a pair or a converter of this map that names a member overrides the member's own declarations (the type's policy still covers it).
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
/// These are checked once per result set, when the row reader is built, and a result set that fails is checked again the next time it comes back.
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
/// The delegate receives the column's own value when the column is a <c>TColumn</c>,
/// else the value read the way <c>Map&lt;int&gt;()</c> reads a single value: an integer converts only within the range of <c>TColumn</c>, else the row throws <see cref="ValueMappingException"/>.
/// </para>
/// <para>
/// A converter is not called for a NULL cell, and the member follows the NULL rules above:
/// <see langword="null"/> for a nullable member, <see cref="ValueMappingException"/> for a non-nullable one.
/// A converter that must see NULL names a <see cref="Nullable{T}"/> as <c>TColumn</c> (<c>int?</c>, <c>DateTime?</c>): it is called with <see langword="null"/>,
/// and what it returns is the member's value, a non-nullable member's included.
/// A converter of a reference-type column (text included) is never called with <see langword="null"/>, whatever its parameter is annotated as.
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
	// One pair, so that new(...) compiles as a single argument.
	/// <summary>
	/// Initializes a new instance of the <see cref="RowMap{T}"/> class from one (property name, column name) pair.
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

		string? policyError = null;
		NullAction? modelPolicy = ModelPolicy(ref policyError);
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

			bool nonNullableReference = NullAnnotations.IsNonNullableReference(parameter);
			NullRule? onNull = RuleOf(NullDeclaration.Of(parameter), modelPolicy, name, parameter.ParameterType, nonNullableReference, ref policyError);
			NullRule? typeOnNull = RuleOf(NullDeclaration.None, modelPolicy, name, parameter.ParameterType, nonNullableReference, ref policyError);
			list.Add(new Slot(name, parameter.ParameterType, i, name, false, Slot.FallbackOf(parameter), false, nonNullableReference, onNull: onNull, typeOnNull: typeOnNull));
		}

		if (policyError is not null) throw new ArgumentException(policyError, nameof(factory));
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
	/// a factory map's pairs do not hold its function, so that constructor throws for one,
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
	/// <param name="convert">Makes the value of the member of the value of the column. It runs once per row.</param>
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
	/// <param name="convert">Makes the value of the member of the value of the column. It runs once per row.</param>
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

	// The per-layout reader cache is bounded so dynamically generated column lists cannot grow memory forever.
	// Real applications use a handful of layouts; when the bound is hit the cache is simply cleared and refills.
	/// <summary>The number of layouts a map keeps readers for before its cache is cleared.</summary>
	const int MaxLayouts = 128;

	// The statics below are one immutable copy per model type: see TypeShape.

	// Never throws: an unmappable T holds its error, so no static initializer here can fail with it.
	// The per-model-type statics (TypeShape, ModelSlots, ModelSlotIndex, Default) are one immutable copy per T, built once.
	// Any cache of compiled readers keyed by model type lives as long and costs a lookup per call; a static on the generic type is the cheapest place for it.
	// Nothing that does not depend on T belongs here.
	/// <summary>
	/// Holds the shape of <typeparamref name="T"/>: why it cannot be mapped, if it cannot, and the constructor it is built through.
	/// </summary>
	static readonly Shape TypeShape = Shape.Of(typeof(T));

	// Nullable annotations are read from the assembly the model is declared in, once per model type.
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
	/// Why a null policy of <typeparamref name="T"/> cannot be honoured, or <see langword="null"/> when every one can.
	/// </summary>
	static readonly string? PolicyError = Model.PolicyError;

	// Only then does a result set pay the checks of Match for it (once per layout, never per row).
	/// <summary>
	/// Whether any member declares a column or an ordinal.
	/// </summary>
	static readonly bool HasDecorators = Model.Decorated;

	/// <summary>
	/// Represents the slots of a model type, and the verdict on what its members declare.
	/// </summary>
	/// <param name="slots">The slots, in order.</param>
	/// <param name="error">Why the declarations cannot be honoured, or <see langword="null"/> when they can.</param>
	/// <param name="decorated"><see langword="true"/> if any slot declares a column name or an ordinal; otherwise, <see langword="false"/>.</param>
	/// <param name="policyError">Why a null policy cannot be honoured, or <see langword="null"/> when every one can.</param>
	sealed class SlotTable(ImmutableArray<Slot> slots, string? error, bool decorated, string? policyError = null)
	{
		/// <summary>Why a null declaration cannot be honoured (a null value that does not convert or is not a constant, an undefined action), or <see langword="null"/> when every one can.</summary>
		public readonly string? PolicyError = policyError;

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
		string? policyError = null;
		NullAction? modelPolicy = ModelPolicy(ref policyError);
		if (TypeShape.Constructor is { } constructor) AddParameterSlots(constructor, list, seen, ref error, modelPolicy, ref policyError);

		foreach (PropertyInfo p in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
		{
			if (p.GetIndexParameters().Length != 0 || p.GetSetMethod() is not { } setter || !seen.Add(p.Name)) continue;

			var declared = Decoration.Of(p);
			if (declared.NotMapped || declared.Invalid is not null) error ??= DeclarationError(p, declared, null);

			bool nonNullableReference = NullAnnotations.IsNonNullableReference(p);
			NullRule? onNull = RuleOf(NullDeclaration.Of(p), modelPolicy, p.Name, p.PropertyType, nonNullableReference, ref policyError);
			NullRule? typeOnNull = RuleOf(NullDeclaration.None, modelPolicy, p.Name, p.PropertyType, nonNullableReference, ref policyError);
			list.Add(new Slot(p.Name, p.PropertyType, setter, MustHaveColumn(p), nonNullableReference, declared.Column, declared.Order, declared.NotMapped, onNull, typeOnNull));
		}

		bool decorated = list.Exists(s => s.ColumnName is not null || s.Order >= 0);
		if (decorated) error ??= DuplicateDeclaration(list);

		return new([.. list], error, decorated, policyError);
	}

	// CustomAttributeData reads what a type declares itself, so the hierarchy is walked here: the nearest declaration wins.
	/// <summary>
	/// Returns the null policy the model type declares, else the nearest base type's, or <see langword="null"/> for none.
	/// </summary>
	/// <param name="error">The first null declaration error so far; set when the policy is not a <see cref="NullAction"/>.</param>
	static NullAction? ModelPolicy(ref string? error)
	{
		for (Type? type = typeof(T); type is not null && type != typeof(object); type = type.BaseType)
		{
			NullDeclaration declared = NullDeclaration.Of(type);
			if (declared.Invalid is not null)
			{
				error ??= $"'{Shape.TypeName(type)}' declares {declared.Invalid}.";
				return null;
			}

			if (declared.Policy is { } policy) return policy;
		}

		return null;
	}

	// Resolved once per slot, when the slots are built: only a slot with a rule pays for a NULL test in the compiled readers.
	/// <summary>
	/// Returns what a NULL gives a member: its <see cref="NullValueAttribute"/>, else its own policy, else the model type's, or <see langword="null"/> for none.
	/// </summary>
	/// <param name="member">What the member declares; its null value wins over any policy.</param>
	/// <param name="model">The policy the model type declares, or <see langword="null"/>.</param>
	/// <param name="name">The member's name, for the message.</param>
	/// <param name="type">The member's type.</param>
	/// <param name="nonNullableReference"><see langword="true"/> for a reference type declared non-nullable.</param>
	/// <param name="error">The first null declaration error so far; set when a declaration cannot be honoured.</param>
	static NullRule? RuleOf(NullDeclaration member, NullAction? model, string name, Type type, bool nonNullableReference, ref string? error)
	{
		if (member.Invalid is not null)
		{
			error ??= $"'{name}' of '{Shape.TypeName(typeof(T))}' declares {member.Invalid}.";
			return null;
		}

		if (member.HasValue)
		{
			if (NullValueOf(member.Value, type, nonNullableReference) is { } value) return new NullRule(value);

			string shown = member.Value is { } v ? $"{System.Convert.ToString(v, CultureInfo.InvariantCulture)} ({v.GetType().Name})" : "null";
			error ??= $"The null value of '{name}' of '{Shape.TypeName(typeof(T))}', {shown}, does not convert to {Shape.TypeName(type)}.";
			return null;
		}

		return (member.Policy ?? model) switch
		{
			NullAction.Throw => NullRule.Throw,
			NullAction.Default => NullRule.Default,
			_ => null, // None or no policy: no test, and a member's None overrides the type's policy.
		};
	}

	/// <summary>
	/// Returns the constant a NULL gives a member: the value converted to the member's type (its underlying type for a <see cref="Nullable{T}"/>) as a column is,
	/// or <see langword="null"/> when it does not convert.
	/// </summary>
	static ConstantExpression? NullValueOf(object? value, Type type, bool nonNullableReference)
	{
		Type? underlying = Nullable.GetUnderlyingType(type);
		if (value is null)
			return (type.IsValueType && underlying is null) || nonNullableReference ? null : Expression.Constant(null, type);

		try
		{
			object? converted = typeof(RowMap<T>)
				.GetMethod(nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!
				.MakeGenericMethod(underlying ?? type)
				.Invoke(null, [value]);
			return Expression.Constant(converted, type);
		}
		catch (Exception ex) when (ex is TargetInvocationException or ArgumentException)
		{
			return null;
		}
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
	/// <param name="modelPolicy">The null policy the model type declares, or <see langword="null"/>.</param>
	/// <param name="policyError">The first null policy error so far.</param>
	static void AddParameterSlots(ConstructorInfo constructor, List<Slot> list, HashSet<string> seen, ref string? error, NullAction? modelPolicy, ref string? policyError)
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

			// A positional record's parameter takes the declarations on the parameter, else on the property it feeds.
			bool nonNullableReference = NullAnnotations.IsNonNullableReference(parameter);
			NullDeclaration own = NullDeclaration.Of(parameter);
			if (matches[i] is { } fed) own = own.Or(NullDeclaration.Of(fed));
			NullRule? onNull = RuleOf(own, modelPolicy, name, parameter.ParameterType, nonNullableReference, ref policyError);
			NullRule? typeOnNull = RuleOf(NullDeclaration.None, modelPolicy, name, parameter.ParameterType, nonNullableReference, ref policyError);
			list.Add(new Slot(name, parameter.ParameterType, i, parameter.Name ?? name, matches[i] is not null, Slot.FallbackOf(parameter), matches[i] is { } required && MustHaveColumn(required), nonNullableReference, declared.Column, declared.Order, onNull, typeOnNull));
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

	// Static initializers run in textual order; each must follow what it uses.
	/// <summary>The index of <see cref="ModelSlots"/> by name.</summary>
	static readonly FrozenDictionary<string, int> ModelSlotIndex = BuildSlotIndex(ModelSlots);

	static FrozenDictionary<string, int> BuildSlotIndex(ImmutableArray<Slot> slots)
	{
		var index = new Dictionary<string, int>(slots.Length, StringComparer.Ordinal);
		for (int i = 0; i < slots.Length; i++) index.Add(slots[i].Name, i);
		return index.ToFrozenDictionary();
	}

	/// <summary>Gets the map for the model's default mapping (property name == column name), used when no map is passed.</summary>
	/// <value>The default map of <typeparamref name="T"/>.</value>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="ArgumentException">A <see cref="NullValueAttribute"/> of <typeparamref name="T"/> does not convert to its member's type, or is not a supported constant.</exception>
	public static RowMap<T> Default
	{
		// Null when T cannot be mapped, so reading Default throws instead of a static initializer failing.
		get => field ?? throw (TypeError is not null ? new InvalidOperationException(TypeError) : new ArgumentException(PolicyError));
	} = TypeError is null && PolicyError is null ? new() : null; // Declared after TypeShape, ModelSlots and ModelSlotIndex: its constructor reads them.

	/// <summary>
	/// Throws when <typeparamref name="T"/> cannot be mapped.
	/// </summary>
	/// <remarks>
	/// It is for an entry point that runs a command before it maps a row: checking the type first makes an unmappable <typeparamref name="T"/> fail at the call, with nothing executed.
	/// </remarks>
	/// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped (see <see cref="RowMap{T}"/>).</exception>
	/// <exception cref="ArgumentException">A <see cref="NullValueAttribute"/> of <typeparamref name="T"/> cannot be honoured.</exception>
	[SuppressMessage("Roslynator", "RCS1158:Static member in generic type should use a type parameter", Justification = "It checks this T's shape: the type parameter is how a caller names which T to check (RowMap<T>.ThrowIfUnmappable()).")]
	internal static void ThrowIfUnmappable()
	{
		if (TypeError is not null) throw new InvalidOperationException(TypeError);
		if (PolicyError is not null) throw new ArgumentException(PolicyError);
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
	/// The array itself is <see langword="null"/> for a map nothing was converted in.
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

	// The same match, so the pipelines that pick columns before mapping them agree with Map<T>() on which column feeds which property.
	/// <summary>
	/// Finds the columns of the record that feed a property, as their ordinals in ascending order and, aligned with them, their actual names in the record.
	/// </summary>
	/// <remarks>
	/// It is the same match (<c>Match</c>) a row read uses.
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
	/// It returns <see langword="null"/> when there is none.
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

	// No stripped copy of a name is ever made: the hash and the comparison walk the characters.
	/// <summary>
	/// Compares names ignoring case and ignoring every <c>_</c> in either name.
	/// </summary>
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

	// After the first query of a given shape no reflection, expression building or compilation happens again.
	// Values are read with the provider's typed getters whenever the column type matches the property type (or converts numerically), so nothing is boxed.
	// A non-nullable member is read with the typed getter alone: a NULL makes the provider throw, so it costs nothing per cell. Only a nullable member asks IsDBNull.
	// Which reference types are declared non-nullable is read once per model type, when the slots are built (NullAnnotations).
	/// <summary>
	/// Represents one result-set layout (the column names and types) and the compiled readers for it.
	/// </summary>
	/// <remarks>
	/// The map holds the validated column table and the compiled typed row readers for one result-set layout (column names and column types).
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

		// The same check is written twice on purpose: this runs on every query, and virtual calls on DbDataReader beat the interface calls the IDataRecord overload must make.
		/// <summary>
		/// Returns whether the reader has this layout.
		/// </summary>
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

	#region Array rows (the route generated mappers and the Channel and Dataflow pipelines use)

	/// <summary>The layout of the <c>object[]</c> rows most recently used.</summary>
	/// <remarks>
	/// Rows already copied out of a reader as <c>object[]</c> are mapped by a compiled, cached delegate per column list.
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
	/// <remarks>For generated code.</remarks>
	/// <param name="names">The names of the columns, in order.</param>
	/// <returns>The delegate that maps a row to a <typeparamref name="T"/>.</returns>
	/// <exception cref="ValueMappingException">A required property has no column among <paramref name="names"/>.</exception>
	[System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
	public Func<object?[], T> ArrayMapper(ImmutableArray<string> names)
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
		MethodInfo isNullCell = typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!;

		Expression Cell(int i) => Expression.ArrayIndex(row, Expression.Constant(i));

		// A slot with a converter reads its column as the converter's type (see Converted).
		// The caller of an array mapper owns the loop, so the row is not known: the error names none.
		CellRead Read(int i, Slot slot)
		{
			Converter? converter = ConverterOf(slot);
			// A null policy tests the cell first, so the value is converted (and a converter called) only when it is not NULL.
			if (PolicyIn(slot) is { } rule)
			{
				Expression value = ConvertCell(Cell(i), converter?.Column ?? slot, nullChecked: true);
				return new(Expression.Call(isNullCell, Cell(i)), converter is null ? value : Converted(converter, slot, value, null), rule);
			}

			return new(
				null,
				converter is null
					? ConvertCell(Cell(i), slot)
					: Converted(
						converter,
						slot,
						ConvertCell(Cell(i), converter.Column),
						NullSkipsConverter(converter, slot) ? Expression.Call(isNullCell, Cell(i)) : null));
		}

		return Compile<object?[]>(row, layout.Names, Read, rowKnown: false);
	}

	#endregion

	#region Compilation

	Func<TReader, T> Build<TReader>(Layout layout)
		where TReader : class, IDataRecord
	{
		Type readerType = typeof(TReader);
		MethodInfo isDbNull = readerType.GetMethod(nameof(IDataRecord.IsDBNull), [typeof(int)])!;
		MethodInfo getValue = readerType.GetMethod(nameof(IDataRecord.GetValue), [typeof(int)])!;

		ParameterExpression r = Expression.Parameter(readerType, "r");

		// A converter's general path reads its cell once into `cell`, tests it for NULL, and converts that same value.
		ParameterExpression cell = Expression.Variable(typeof(object), "cell");

		// What column i gives a slot: the member's own type, or the one the converter of the slot receives (see Converted).
		CellRead Read(int i, Slot slot)
		{
			Expression ordinal = Expression.Constant(i);
			Type columnType = layout.Types[i];
			Converter? converter = ConverterOf(slot);

			// A null policy tests the cell first (the converter's column, when there is one), and the value is read (and a converter called) only when it is not NULL.
			if (PolicyIn(slot) is { } rule)
			{
				Slot source = converter?.Column ?? slot;
				Expression value = ReadValue(r, ordinal, columnType, source, readerType, isDbNull, getValue, cell, nullChecked: true);
				return new(
					NullTest(r, ordinal, columnType, source, readerType, isDbNull, getValue, cell),
					converter is null ? value : Converted(converter, slot, value, null),
					rule);
			}

			if (converter is null)
				return new(null, ReadValue(r, ordinal, columnType, slot, readerType, isDbNull, getValue));

			if (!NullSkipsConverter(converter, slot))
				return new(null, Converted(converter, slot, ReadValue(r, ordinal, columnType, converter.Column, readerType, isDbNull, getValue), null));

			return new(
				null,
				Converted(
					converter,
					slot,
					ReadValue(r, ordinal, columnType, converter.Column, readerType, isDbNull, getValue, cell),
					NullTest(r, ordinal, columnType, converter.Column, readerType, isDbNull, getValue, cell)));
		}

		return Compile<TReader>(r, layout.Names, Read, rowKnown: true, cell);
	}

	// A pair and a converter are explicit and belong to this map, so they override what the member declares, as a pair overrides [Column];
	// the type's policy still covers the member. A converter whose column type takes NULL receives the NULL itself.
	/// <summary>
	/// Returns the null policy a slot follows in this map, or <see langword="null"/> for none.
	/// </summary>
	NullRule? PolicyIn(Slot slot)
	{
		int s = _slots.IndexOf(slot);
		Converter? converter = _converters?[s];
		if (converter?.Column.RejectsNull == false) return null;
		return _named[s] || converter is not null ? slot.TypeOnNull : slot.OnNull;
	}

	/// <summary>
	/// Returns the slot's converter, or <see langword="null"/> for a slot without one
	/// (every slot of a map nothing was converted in).
	/// </summary>
	Converter? ConverterOf(Slot slot) => _converters?[_slots.IndexOf(slot)];

	/// <summary>
	/// Returns whether a NULL bypasses the converter of a slot: its column type rejects NULL and the member takes it.
	/// </summary>
	/// <remarks>
	/// The member is then <see langword="null"/> on NULL, and the converter is not called.
	/// A member that rejects NULL needs no test: the column read throws.
	/// </remarks>
	static bool NullSkipsConverter(Converter converter, Slot slot) => converter.Column.RejectsNull && !slot.RejectsNull;

	/// <summary>
	/// Returns what a slot with a converter reads:
	/// the delegate applied to the column read as the delegate's own parameter type.
	/// </summary>
	/// <remarks>
	/// The column is a slot of that type (<c>Converter.Column</c>), so it is read, and its NULL judged, as a member of that type is:
	/// only a <see cref="Nullable{T}"/> is given NULL,
	/// and what the delegate returns is the value of the member, a non-nullable one's too.
	/// For anything else the delegate is not called on NULL:
	/// a member that takes NULL is <see langword="null"/> (<paramref name="columnIsNull"/> is the test then),
	/// and a member that rejects it throws, as the column read does.
	/// </remarks>
	static Expression Converted(Converter converter, Slot slot, Expression column, Expression? columnIsNull)
	{
		Expression value = converter.Invoke(column, slot.Type);
		return columnIsNull is null
			? value
			: Expression.Condition(columnIsNull, Expression.Default(slot.Type), value);
	}

	/// <summary>
	/// Builds the reader of a single value (<c>FirstColumn&lt;T&gt;</c>):
	/// column 0 read exactly as a property of type <typeparamref name="T"/> is,
	/// with the same typed getter, conversion and NULL rule.
	/// </summary>
	/// <remarks>
	/// A NULL into a type the C# type declares non-nullable is an error (the read loop names the row);
	/// any other NULL is <see langword="null"/>.
	/// It is built from the helpers below, which the row readers also use.
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
		Expression value = ReadValue(
			r,
			Expression.Constant(0),
			columnType,
			new Slot(typeof(T)),
			readerType,
			isDbNull,
			getValue);

		return Expression
			.Lambda<Func<TReader, T>>(
				Guard(value, [], singleValue: true, rowKnown: true),
				r)
			.Compile();
	}

	// Built from the same helpers (TypedGetters, TryConvert, ConvertValue) as the single-column reader, so the two agree for every provider tested; the parity matrix checks it.
	/// <summary>
	/// Converts the scalar of a command (<c>ExecuteScalar&lt;T&gt;</c>, <c>ExecuteReturn&lt;T&gt;</c>):
	/// a value that has arrived boxed, converted as column 0 of <c>Map&lt;T&gt;()</c> is.
	/// </summary>
	/// <remarks>
	/// No value (no row) is <c>default(T)</c>;
	/// NULL (<see cref="DBNull"/>) is <see langword="null"/> for a <typeparamref name="T"/> that takes NULL, and an error for a non-nullable value type;
	/// a value that is already a <typeparamref name="T"/> is returned as it is;
	/// any other value is converted the way <c>Map&lt;T&gt;()</c> converts a column of its type.
	/// </remarks>
	/// <param name="value">The scalar as it arrived.</param>
	/// <returns>The value as a <typeparamref name="T"/>.</returns>
	/// <exception cref="ValueMappingException">The value is NULL and <typeparamref name="T"/> is a non-nullable value type, or it cannot be converted: the conversion's error is the inner exception.</exception>
	internal static T ConvertScalar(object? value)
	{
		if (value is null) return default!;
		if (value is DBNull)
		{
			return ScalarRejectsNull
				? throw new ValueMappingException(RowErrors.NullScalar(typeof(T)), "", "", typeof(T))
				: default!;
		}

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
	/// Whether a NULL scalar is an error: <typeparamref name="T"/> is a value type other than <see cref="Nullable{T}"/>.
	/// </summary>
	/// <remarks>
	/// A reference type is <see langword="null"/>: a type argument carries no nullable annotation, as a single value read by <c>Map&lt;T&gt;()</c> does not.
	/// </remarks>
	static readonly bool ScalarRejectsNull = typeof(T).IsValueType && Nullable.GetUnderlyingType(typeof(T)) is null;

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

	// A model built as it always was (a class with a public parameterless constructor, a struct with no constructor of its own)
	// is built as new T(), then for each column that maps to a property, item.Property = <cell>.
	// A model built through a constructor reads every fed cell, in column order, into a typed local,
	// then calls new T(arguments), then sets the properties no parameter covers.
	// Only how a cell is read differs (a live reader vs buffered object[] rows).
	// No state is written per cell: a failure is reported for the row (Guard), never for a column, so nothing tracks which cell is being read.
	/// <summary>
	/// Builds the compiled row reader that both mappers share.
	/// </summary>
	/// <param name="arg">The parameter the row is read from.</param>
	/// <param name="names">The names of the columns, in order.</param>
	/// <param name="readCell">What column i gives a slot.</param>
	/// <param name="rowKnown"><see langword="true"/> when a read loop of the library calls the reader and names the row of a failure; otherwise, <see langword="false"/>.</param>
	/// <param name="cell">The variable a read may keep a raw cell in, or <see langword="null"/> for none.</param>
	Func<TArg, T> Compile<TArg>(ParameterExpression arg, string[] names, Func<int, Slot, CellRead> readCell, bool rowKnown, ParameterExpression? cell = null)
	{
		ParameterExpression item = Expression.Variable(typeof(T), "item");

		var locals = new List<ParameterExpression> { item };
		if (cell is not null) locals.Add(cell);
		Slot?[] feeds = Match(names);
		RequireColumns(feeds);

		List<Expression> statements = _factory is { } factory
			? BindConstructor(_slots.Length, arguments => CallFactory(factory, arguments), item, names, feeds, readCell, locals)
			: TypeShape.Constructor is { } constructor
				? BindConstructor(constructor.GetParameters().Length, arguments => Expression.New(constructor, arguments), item, names, feeds, readCell, locals)
				: CreateThenSet(item, names, feeds, readCell);

		return Expression
			.Lambda<Func<TArg, T>>(
				Guard(Expression.Block(typeof(T), statements), locals, singleValue: false, rowKnown),
				arg)
			.Compile();
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
			if (!slot.Required && (slot.ColumnName is null || _named[s]) || Array.IndexOf(feeds, slot) >= 0) continue;

			string column = _columns[s]!; // A required slot is never mapped to null (the pairs are validated), and a declared one is not either.
			throw new ValueMappingException(
				slot.Required
					? $"No column feeds the required property '{slot.Name}' ({Shape.TypeName(slot.Type)}) of '{Shape.TypeName(typeof(T))}'; expected a column named '{column}'."
					: $"No column feeds the {Describe(slot)} of '{Shape.TypeName(typeof(T))}', which is declared [Column(\"{column}\")]; expected a column named '{column}'.",
				slot.Name, column, slot.Type);
		}
	}

	// The factory is held once by the compiled row reader, and calling it allocates nothing
	// (FactoryMapTests.TheFactoryRoute_AllocatesNothingPerRow_BeyondTheConstructorRoute).
	// The factory is a constant of the expression tree, as a converter's delegate is.
	/// <summary>
	/// Builds the call of the factory with the arguments.
	/// </summary>
	/// <remarks>
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
	static List<Expression> CreateThenSet(ParameterExpression item, string[] names, Slot?[] feeds, Func<int, Slot, CellRead> readCell)
	{
		var statements = new List<Expression>(names.Length + 2)
		{
			Expression.Assign(item, Expression.New(typeof(T))),
		};

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			CellRead read = readCell(i, slot);
			statements.Add(KeepsOnNull(read, slot) is { } isNull
				? Expression.IfThen(Expression.Not(isNull), Expression.Call(item, slot.Setter!, read.Value))
				: Expression.Call(item, slot.Setter!, OnNull(read, slot, names[i])));
		}

		statements.Add(item);
		return statements;
	}

	// A constructor needs its arguments before anything can be set, and a SequentialAccess reader allows each cell to be read once, in ascending order:
	// so every fed cell is read, in column order, into a typed local (nothing is boxed on the typed path),
	// the constructor is called with the locals (the fallback for a parameter no column feeds),
	// and then the properties the constructor does not cover are set from their locals.
	/// <summary>
	/// Builds the statements of a model created through a constructor.
	/// </summary>
	List<Expression> BindConstructor(int parameterCount, Func<Expression[], Expression> create, ParameterExpression item, string[] names, Slot?[] feeds,
		Func<int, Slot, CellRead> readCell, List<ParameterExpression> locals)
	{
		var arguments = new Expression[parameterCount];
		var statements = new List<Expression>(names.Length + 3);
		var sets = new List<Expression>();

		for (int i = 0; i < names.Length; i++)
		{
			if (feeds[i] is not { } slot) continue; // Not mapped: ignore the column.

			ParameterExpression local = Expression.Variable(slot.Type, "value" + i);
			locals.Add(local);
			CellRead read = readCell(i, slot);

			if (slot.Setter is not null && KeepsOnNull(read, slot) is { } isNull)
			{
				// A property that keeps its value on NULL is not set then: the NULL is remembered, as the set comes after the constructor.
				ParameterExpression wasNull = Expression.Variable(typeof(bool), "null" + i);
				locals.Add(wasNull);
				statements.Add(Expression.Assign(local, Expression.Condition(Expression.Assign(wasNull, isNull), Expression.Default(slot.Type), read.Value)));
				sets.Add(Expression.IfThen(Expression.Not(wasNull), Expression.Call(item, slot.Setter, local)));
				continue;
			}

			statements.Add(Expression.Assign(local, OnNull(read, slot, names[i])));

			if (slot.Setter is null)
			{
				Debug.Assert(arguments[slot.Position] is null);
				arguments[slot.Position] = local;
			}
			else
			{
				sets.Add(Expression.Call(item, slot.Setter, local));
			}
		}

		foreach (Slot slot in _slots)
		{
			// Only a parameter no column feeds receives its declared default: a NULL in a column that feeds one follows the NULL rules.
			if (slot.Setter is null && arguments[slot.Position] is null) arguments[slot.Position] = slot.Fallback!;
		}

		statements.Add(Expression.Assign(item, create(arguments)));
		statements.AddRange(sets);
		statements.Add(item);
		return statements;
	}

	/// <summary>
	/// Returns the NULL test of a property whose null policy is <see cref="NullAction.Default"/> (it is not set on NULL), or <see langword="null"/> for any other read.
	/// </summary>
	static Expression? KeepsOnNull(CellRead read, Slot slot)
		=> slot.Setter is not null && read.Rule?.Action == NullAction.Default ? read.IsNull : null;

	/// <summary>
	/// Returns what a slot receives from its read, under its null policy:
	/// the value, else on NULL a <c>NullPolicyViolation</c> (<see cref="NullAction.Throw"/>), the parameter's fallback (<see cref="NullAction.Default"/>), or the member's <see cref="NullValueAttribute"/> constant.
	/// </summary>
	/// <param name="read">The read of the cell.</param>
	/// <param name="slot">The slot.</param>
	/// <param name="column">The name of the column, for the error.</param>
	static Expression OnNull(CellRead read, Slot slot, string column)
	{
		if (read.IsNull is not { } isNull) return read.Value;

		NullRule rule = read.Rule!;
		Expression whenNull = rule.Action switch
		{
			NullAction.Throw => Expression.Throw(
				Expression.New(
					typeof(NullPolicyViolation).GetConstructor([typeof(string), typeof(string), typeof(Type)])!,
					Expression.Constant(slot.Name),
					Expression.Constant(column),
					Expression.Constant(slot.Type, typeof(Type))),
				slot.Type),
			NullAction.Default => slot.Fallback!, // Only a parameter reaches here: a property's Default is a set left out (KeepsOnNull).
			_ => rule.Value!,
		};

		return Expression.Condition(isNull, whenNull, read.Value);
	}

	// One try region per row and nothing written per cell: the region costs the row nothing until it throws.
	// The reader is not touched after a failure (no name is read for the message): the row is what is reported.
	/// <summary>
	/// Wraps the read of a row so that a failure is reported for the row.
	/// </summary>
	/// <remarks>
	/// The provider's own failures (<see cref="DbException"/>) and cancellation propagate untouched.
	/// Anything else is handed to the read loop that called the reader, which knows the row (<c>RowMappingFailure</c>),
	/// or, for a reader whose caller owns the loop (<c>ArrayMapper</c>), thrown at once as a <see cref="ValueMappingException"/> that names no row.
	/// </remarks>
	/// <param name="body">The read of the row.</param>
	/// <param name="variables">The variables of the read.</param>
	/// <param name="singleValue"><see langword="true"/> for the reader of a single value; otherwise, <see langword="false"/> for a model.</param>
	/// <param name="rowKnown"><see langword="true"/> when a read loop of the library calls the reader; otherwise, <see langword="false"/>.</param>
	/// <returns>The guarded read.</returns>
	static BlockExpression Guard(Expression body, List<ParameterExpression> variables, bool singleValue, bool rowKnown)
	{
		ParameterExpression ex = Expression.Parameter(typeof(Exception), "ex");
		MethodInfo fail = rowKnown
			? typeof(RowMappingFailure).GetMethod(nameof(RowMappingFailure.Defer))!
			: typeof(RowErrors).GetMethod(nameof(RowErrors.Create), [typeof(Exception), typeof(Type), typeof(bool)])!;

		return Expression.Block(
			typeof(T),
			variables,
			Expression.TryCatch(
				body,
				Expression.Catch(
					ex,
					Expression.Condition(
						Expression.OrElse(
							Expression.TypeIs(ex, typeof(DbException)),
							Expression.TypeIs(ex, typeof(OperationCanceledException))),
						Expression.Rethrow(typeof(T)),
						Expression.Throw(
							Expression.Call(
								fail,
								ex,
								Expression.Constant(typeof(T)),
								Expression.Constant(singleValue)),
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
	/// Returns the expression that is <see langword="true"/> when the cell is NULL: for a slot with a null policy, and for a converter whose column type rejects NULL and whose member takes it (<c>NullSkipsConverter</c>).
	/// </summary>
	/// <remarks>
	/// On the typed path the reader answers; on the general path the cell is read once into <c>cell</c> and <c>ReadValue</c> converts that same value.
	/// </remarks>
	static MethodCallExpression NullTest(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue, ParameterExpression cell)
	{
		if (TypedRead(r, ordinal, columnType, Nullable.GetUnderlyingType(slot.Type) ?? slot.Type, readerType) is not null)
			return Expression.Call(r, isDbNull, ordinal);

		return Expression.Call(
			typeof(RowMap<T>).GetMethod(nameof(IsNullCell), BindingFlags.Static | BindingFlags.NonPublic)!,
			Expression.Assign(cell, Expression.Call(r, getValue, ordinal)));
	}

	// A slot that rejects NULL is read with no IsDBNull: the typed getter throws on NULL (SqlClient: SqlNullValueException; Microsoft.Data.Sqlite: InvalidOperationException),
	// and the general path's conversion throws SqlNullValueException itself (ConvertCell), so both report the same way.
	/// <summary>
	/// Returns the read of a cell as the type of a slot.
	/// </summary>
	/// <param name="r">The reader.</param>
	/// <param name="ordinal">The ordinal of the column.</param>
	/// <param name="columnType">The type the reader reports for the column.</param>
	/// <param name="slot">The slot the value is for.</param>
	/// <param name="readerType">The type of the reader.</param>
	/// <param name="isDbNull">The reader's <c>IsDBNull</c>.</param>
	/// <param name="getValue">The reader's <c>GetValue</c>.</param>
	/// <param name="cell">The variable <c>NullTest</c> has read the cell into on the general path, or <see langword="null"/> to read the cell here.</param>
	/// <param name="nullChecked"><see langword="true"/> when <c>NullTest</c> has found the cell not NULL (a null policy), so the read makes no NULL test of its own.</param>
	/// <returns>The read.</returns>
	static Expression ReadValue(
		ParameterExpression r, Expression ordinal,
		Type columnType, Slot slot,
		Type readerType, MethodInfo isDbNull, MethodInfo getValue,
		ParameterExpression? cell = null, bool nullChecked = false)
	{
		Type propertyType = slot.Type;
		Type? underlying = Nullable.GetUnderlyingType(propertyType);
		Type target = underlying ?? propertyType;

		if (TypedRead(r, ordinal, columnType, target, readerType) is { } typed)
		{
			// The typed value, lifted to Nullable<> when needed.
			Expression present = underlying is null ? typed : Expression.Convert(typed, propertyType);
			if (slot.RejectsNull || nullChecked) return present;

			// NULL → null.
			return Expression.Condition(
				Expression.Call(r, isDbNull, ordinal),
				Expression.Default(propertyType),
				present);
		}

		// General path: the boxed value, converted. Handles enums, strings → Guid, mismatched types, unknown column types.
		return ConvertCell(cell ?? (Expression)Expression.Call(r, getValue, ordinal), slot, nullChecked);
	}

	/// <summary>
	/// Returns the conversion of a boxed cell to the type of a slot: NULL is <see langword="null"/> for a slot that takes it, and an error for one that rejects it.
	/// </summary>
	/// <param name="value">The boxed cell.</param>
	/// <param name="slot">The slot.</param>
	/// <param name="nullChecked"><see langword="true"/> when the cell is known not to be NULL (a null policy tested it).</param>
	static MethodCallExpression ConvertCell(Expression value, Slot slot, bool nullChecked = false)
		=> Expression.Call(
			typeof(RowMap<T>)
				.GetMethod(slot.RejectsNull && !nullChecked ? nameof(ConvertRequired) : nameof(ConvertValue), BindingFlags.Static | BindingFlags.NonPublic)!
				.MakeGenericMethod(slot.Type),
			value);

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
	/// Three more are the conversions the general path makes:
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

	// The provider's typed getters throw SqlNullValueException on NULL (SqlClient), so the library's own NULL check throws the same:
	// one exception type for "a NULL into a non-nullable member", whichever path read the cell.
	static TTarget ConvertRequired<TTarget>(object? value)
		=> value is null or DBNull
			? throw new System.Data.SqlTypes.SqlNullValueException()
			: ConvertValue<TTarget>(value);

	static TTarget ConvertValue<TTarget>(object? value)
	{
		// Only a slot that takes NULL reaches here with one (ConvertRequired guards the others).
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

	#endregion
}

// Not in RowMap<T>: a static field there is created once per model type, and this does not depend on the model type.
/// <summary>
/// Holds the typed getter of each type a column can be read as without boxing.
/// </summary>
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

// The cache is keyed by the column's reported type alone, so there is no name matching to pay for.
/// <summary>
/// The row source of a single-value model type (<c>Shape.IsScalar</c>):
/// column 0 of every row, read as a property of type <typeparamref name="T"/> is,
/// whatever the column is named, other columns ignored.
/// </summary>
/// <remarks>
/// There is one instance per <typeparamref name="T"/> (<see langword="null"/> for any other <typeparamref name="T"/>).
/// Like a property, the reader is chosen once per result set, from the column's reported type.
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

// Slots are held once per model type in an ImmutableArray in place of PropertyInfo,
// so the matcher and the compiled readers see only a name, the type a value must become and where it goes.
// A slot does not depend on the model type either.
/// <summary>
/// Represents what one column can feed: a constructor parameter, or a settable public instance property of the model.
/// </summary>
/// <remarks>
/// A slot fed through the constructor has a <c>Position</c> and no <c>Setter</c>;
/// a parameter and a property of the same name (ignoring case) are one such slot, so the property is never set again.
/// </remarks>
sealed class Slot
{
	/// <summary>Initializes a slot for a settable property.</summary>
	public Slot(string name, Type type, MethodInfo setter, bool required, bool nonNullableReference, string? columnName = null, int order = -1, bool notMapped = false, NullRule? onNull = null, NullRule? typeOnNull = null)
	{
		Name = name;
		OnNull = onNull;
		TypeOnNull = typeOnNull;
		ColumnName = columnName;
		Order = order;
		NotMapped = notMapped;
		Required = required;
		Type = type;
		Setter = setter;
		Position = -1;
		IsProperty = true;
		RejectsNull = IsNonNullable(type, nonNullableReference);
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
		RejectsNull = IsNonNullable(type, nonNullableReference);
	}

	/// <summary>Initializes a slot for a constructor parameter, named after the property of that name when there is one.</summary>
	public Slot(string name, Type type, int position, string parameter, bool isProperty, Expression fallback, bool required, bool nonNullableReference, string? columnName = null, int order = -1, NullRule? onNull = null, NullRule? typeOnNull = null)
	{
		Name = name;
		OnNull = onNull;
		TypeOnNull = typeOnNull;
		ColumnName = columnName;
		Order = order;
		Required = required;
		Type = type;
		Position = position;
		Parameter = parameter;
		IsProperty = isProperty;
		Fallback = fallback;
		RejectsNull = IsNonNullable(type, nonNullableReference);
	}

	/// <summary>
	/// Returns whether a NULL is an error for a member of this type.
	/// </summary>
	/// <remarks>
	/// A NULL is not a value for a member the C# type declares non-nullable:
	/// a value type other than <see cref="Nullable{T}"/> (an enum counts), or a reference type declared non-nullable (<c>NullAnnotations</c>).
	/// </remarks>
	static bool IsNonNullable(Type type, bool nonNullableReference)
		=> type.IsValueType ? Nullable.GetUnderlyingType(type) is null : nonNullableReference;

	/// <summary>
	/// Whether a NULL cell is an error for the member (see <c>IsNonNullable</c>) rather than <see langword="null"/>;
	/// the compiled readers test the cell only for a slot that takes NULL.
	/// </summary>
	public readonly bool RejectsNull;

	/// <summary>
	/// What a NULL gives the member (<see cref="NullPolicyAttribute"/>), or <see langword="null"/> for no policy: the cell is then not tested, and the NULL rules of the type apply.
	/// </summary>
	public readonly NullRule? OnNull;

	/// <summary>
	/// What a NULL gives the member under the model type's policy alone: the rule of a member a pair or a converter names, whose own policy the map overrides.
	/// </summary>
	public readonly NullRule? TypeOnNull;

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

	// ParameterInfo reports a null default for DateTime d = default (and .NET Framework throws FormatException reading it),
	// and the underlying integer for a nullable enum.
	/// <summary>
	/// Returns the expression of what the constructor receives for a parameter no column feeds.
	/// </summary>
	/// <remarks>
	/// A <see langword="null"/> default stands for <c>default(TParam)</c>, and an enum default is converted back to the enum.
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
/// Represents what a NULL gives a member as the compiled readers apply it: a policy's action, or a null value's constant.
/// </summary>
sealed class NullRule
{
	/// <summary>A NULL throws (<see cref="NullAction.Throw"/>).</summary>
	public static readonly NullRule Throw = new(NullAction.Throw, null);

	/// <summary>A NULL leaves the member as it is, or gives a parameter its fallback (<see cref="NullAction.Default"/>).</summary>
	public static readonly NullRule Default = new(NullAction.Default, null);

	/// <summary>Initializes a rule by which a NULL gives the constant.</summary>
	public NullRule(ConstantExpression value) : this(null, value) { }

	NullRule(NullAction? action, ConstantExpression? value)
	{
		Action = action;
		Value = value;
	}

	/// <summary>What a NULL does, or <see langword="null"/> when it gives <see cref="Value"/>.</summary>
	public readonly NullAction? Action;

	/// <summary>The constant a NULL gives, typed as the member; <see langword="null"/> for an action.</summary>
	public readonly ConstantExpression? Value;
}

/// <summary>
/// Represents the read of one cell for a slot: the value, and for a slot with a null policy the test that comes first.
/// </summary>
/// <param name="isNull">The test of the cell for NULL, evaluated before <paramref name="value"/>; <see langword="null"/> for a slot with no null policy.</param>
/// <param name="value">The read of the value; when <paramref name="isNull"/> is given, it is evaluated only for a cell that is not NULL.</param>
/// <param name="rule">The null policy the slot follows in this map; given with <paramref name="isNull"/>.</param>
readonly struct CellRead(Expression? isNull, Expression value, NullRule? rule = null)
{
	public readonly Expression? IsNull = isNull;
	public readonly Expression Value = value;
	public readonly NullRule? Rule = rule;
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
	// A reference type is declared non-nullable here, as its annotations do not exist at run time
	// (a Func<string?, int> is a Func<string, int>).
	/// <summary>The column as a slot of the type the delegate receives, so a NULL is given to the delegate only for a <see cref="Nullable{T}"/>.</summary>
	public readonly Slot Column = new(column, true);

	// The delegate is a constant of the expression tree: it is held once by the compiled row reader, and calling it allocates nothing.
	/// <summary>
	/// Returns the call of the delegate, typed as the delegate was declared and converted to the type of the slot when that is not the one the delegate returns.
	/// </summary>
	/// <remarks>
	/// A delegate converted by variance keeps its own type.
	/// </remarks>
	public Expression Invoke(Expression value, Type slotType)
	{
		Expression call = Expression.Invoke(Expression.Constant(function, functionType), value);
		return member == slotType ? call : Expression.Convert(call, slotType);
	}
}

// The compiler records nullable annotations in metadata:
// NullableAttribute on a member (or a parameter) whose annotation differs from its context,
// else NullableContextAttribute on the nearest enclosing method or type that has one.
// Flag 1 is not-null, 2 is nullable, 0 or none is oblivious (nullable annotations off): only 1 counts.
// The attributes are read as data, by full name, so this works on every target
// (NullabilityInfoContext does not exist on netstandard) and no attribute type is referenced.
// A property marked [AllowNull] (on the property or on its setter's value parameter) says null may be written to it, so it is nullable.
// A member whose declared type is a type parameter (T Value in Box<T>) is not judged: it is as oblivious as it was.
// This runs once per model type, when the slots are built, never per row.
/// <summary>
/// Decides whether a property or a constructor parameter is a reference type the C# source declares non-nullable,
/// which is when a NULL into it is an error (<c>Slot.RejectsNull</c>).
/// </summary>
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

// They are read as data by the full name of the attributes (a consumer's own copy counts, and no package is needed).
// This runs once per model type, when the slots are built, never per row.
/// <summary>
/// Represents what a model property declares for the map.
/// </summary>
/// <remarks>
/// The declarations are <c>[Column(Name)]</c> and <c>[Column(Order = n)]</c>
/// (<c>System.ComponentModel.DataAnnotations.Schema.ColumnAttribute</c>; <c>TypeName</c> is ignored) and <c>[NotMapped]</c>.
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

	// The default value of the struct is not this one: it would claim ordinal 0.
	/// <summary>Gets a <see cref="Decoration"/> that declares nothing.</summary>
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

// Read by full name through CustomAttributeData, as [Column] and [NotMapped] are: no attribute is instantiated,
// and a generator reading the same declarations from symbols sees the same constructor arguments.
/// <summary>
/// What a member, a parameter or a model type declares for a NULL: a <see cref="NullPolicyAttribute"/> and a <see cref="NullValueAttribute"/>.
/// </summary>
/// <param name="policy">The declared policy, or <see langword="null"/> for none.</param>
/// <param name="hasValue"><see langword="true"/> if a null value is declared, <see langword="null"/> included.</param>
/// <param name="value">The declared null value: <see langword="null"/>, a string, a bool, a char, a number or an enum value.</param>
/// <param name="invalid">Why a declaration cannot be honoured, or <see langword="null"/>.</param>
readonly struct NullDeclaration(NullAction? policy, bool hasValue, object? value, string? invalid)
{
	const string PolicyAttributeName = "Open.Database.Extensions.NullPolicyAttribute";
	const string ValueAttributeName = "Open.Database.Extensions.NullValueAttribute";

	/// <summary>Gets the declared policy, or <see langword="null"/> for none.</summary>
	public NullAction? Policy => policy;

	/// <summary>Gets whether a null value is declared.</summary>
	public bool HasValue => hasValue;

	/// <summary>Gets the declared null value.</summary>
	public object? Value => value;

	/// <summary>Gets why a declaration cannot be honoured, or <see langword="null"/>.</summary>
	public string? Invalid => invalid;

	/// <summary>Gets a <see cref="NullDeclaration"/> that declares nothing.</summary>
	public static NullDeclaration None => default;

	/// <summary>Reads what a member or a type declares.</summary>
	public static NullDeclaration Of(MemberInfo member) => Of(CustomAttributeData.GetCustomAttributes(member));

	/// <summary>Reads what a parameter declares.</summary>
	public static NullDeclaration Of(ParameterInfo parameter) => Of(CustomAttributeData.GetCustomAttributes(parameter));

	/// <summary>
	/// Returns this declaration, with what it leaves out taken from <paramref name="other"/>.
	/// </summary>
	/// <param name="other">The declaration to fall back on: a positional record's property, for its parameter.</param>
	public NullDeclaration Or(NullDeclaration other)
		=> new(
			policy ?? other.Policy,
			hasValue || other.HasValue,
			hasValue ? value : other.Value,
			invalid ?? other.Invalid);

	static NullDeclaration Of(IList<CustomAttributeData> attributes)
	{
		NullAction? policy = null;
		bool hasValue = false;
		object? value = null;
		string? invalid = null;
		foreach (CustomAttributeData attribute in attributes)
		{
			string? name = attribute.AttributeType.FullName;
			if (name is not (PolicyAttributeName or ValueAttributeName) || attribute.ConstructorArguments.Count != 1) continue;

			CustomAttributeTypedArgument argument = attribute.ConstructorArguments[0];
			if (name == PolicyAttributeName)
			{
				// An enum argument arrives as its underlying value; a cast such as (NullAction)7 is not an action.
				if (argument.Value is int action && Enum.IsDefined(typeof(NullAction), action))
					policy = (NullAction)action;
				else
					invalid ??= $"[NullPolicy({argument.Value})], which is not a NullAction";
			}
			else
			{
				hasValue = true;
				value = Constant(argument, ref invalid);
			}
		}

		return new(policy, hasValue, value, invalid);
	}

	// A constant of an object parameter arrives as itself, an enum as its underlying value and its type;
	// a Type or an array cannot be a member's value.
	static object? Constant(CustomAttributeTypedArgument argument, ref string? invalid)
	{
		object? raw = argument.Value;
		Type type = argument.ArgumentType;
		if (raw is null) return null;
		if (type.IsEnum) return Enum.ToObject(type, raw);
		if (raw is string || type.IsPrimitive) return raw;

		invalid ??= $"a [NullValue] of type {type.Name}: a null value is null, a string, a bool, a char, a number or an enum value";
		return null;
	}
}

// Reflection runs once per model type and never throws: the reason is held as the message of the exception RowMap<T> raises.
// It does not depend on the model type, so it is not generic.
// Both attributes are matched by full name, so a consumer's own copy counts and the library names no type that netstandard lacks.
/// <summary>
/// Decides whether a model type can be mapped at all, and how it is built, once per model type from its shape.
/// </summary>
/// <remarks>
/// A type that cannot be mapped fails at the call, not at the first row: <see cref="RowMap{T}.Default"/> and the constructors throw the reason.
/// The constructor is chosen from the type alone, never from the columns, so a type is built the same way whatever the query:
/// a class with a public parameterless constructor, or a struct with no public constructor, is built as it always was (<c>new T()</c>, then its settable properties);
/// any other type is built through its one public constructor.
/// Two or more public constructors and none without parameters is an error, and so is a class with none.
/// A <see langword="required"/> property must be fed by a column in every result shape, unless the constructor is marked <c>[SetsRequiredMembers]</c>, which sets them.
/// A <see langword="required"/> public field cannot be set (fields are not mapped), so it is a type error.
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

		if (IsScalar(type)) return $"'{name}' is a single value, not a row model. On a command use Map<T>(), which reads the first column; else ExecuteScalar<T>().";
		if (NotConcrete(type) is { } kind) return $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
		if (IsDictionaryOrDynamic(type)) return $"'{name}' cannot be mapped: it is a dictionary or a dynamic object, which has no properties to set from the columns. Map to a class or record, use AsDictionaryRows() for the values with their column names, or use Map(selector).";

		return Choose(type, name, out constructor, out setsRequiredMembers) ?? (setsRequiredMembers ? null : RequiredField(type, name));
	}

	/// <summary>
	/// Returns whether the type is a single value, not a row: a column of it, not a model with properties.
	/// </summary>
	/// <remarks>
	/// <see cref="Nullable{T}"/> of these is one too.
	/// </remarks>
	internal static bool IsScalar(Type type)
	{
		type = Nullable.GetUnderlyingType(type) ?? type;
		// DateOnly and TimeOnly are matched by name, so every target framework agrees (the netstandard builds cannot name them).
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
	/// The constructor is none (<see langword="null"/>) when a public parameterless constructor exists or when a struct has no public constructor, else the one public constructor.
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

		// A struct always can be created, as its default value, whatever non-public constructors it declares.
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
