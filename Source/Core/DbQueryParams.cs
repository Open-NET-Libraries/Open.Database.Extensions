using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// The parameters of a call: one shared array of entries and how many of
/// them this value sees.
/// </summary>
/// <remarks>
/// <para>
/// An entry is two slots. The first holds how the parameter is named: the
/// name the caller wrote (a <see cref="string"/>), the argument's text
/// resolved when the query runs (a <c>ParameterNames.DerivedExpression</c>),
/// or a marker for the members of an object. The second holds the value,
/// boxed once, a stand-in for a <see langword="null"/> that remembers its
/// type, or the object whose members are the parameters.
/// </para>
/// <para>
/// Adding a parameter writes the next entry in place and returns a new value
/// that sees one more, so a chain of <c>AddParam</c> allocates the array once.
/// An entry is claimed with one compare-and-swap on its first slot, so a value
/// that is kept and added to twice cannot lose either: the second writer finds
/// the slot taken and copies the entries it sees into an array of its own.
/// Nothing a value sees is ever written again, so no lock is needed and a
/// value can be read by any number of threads while others add to it.
/// </para>
/// <para>
/// The array is sized from the definition's parameter count, so a call of that
/// many parameters allocates exactly one array. A call that is nothing but one
/// <c>AddParams</c> holds that object itself and no array.
/// </para>
/// </remarks>
readonly struct ParamList
{
	/// <summary>
	/// The first slot of an entry that holds the members of an object.
	/// </summary>
	static readonly object MembersMarker = new();

	/// <summary>
	/// The entries: <see langword="null"/> for none, an array of
	/// (name, value) slot pairs, or the <see cref="MemberValues"/> of a call
	/// that is one <c>AddParams</c>.
	/// </summary>
	readonly object? _items;

	/// <summary>
	/// Initializes a new instance that sees the first <paramref name="count"/> entries.
	/// </summary>
	/// <param name="items">The entries.</param>
	/// <param name="count">How many entries are seen.</param>
	ParamList(object? items, int count)
	{
		_items = items;
		Count = count;
	}

	/// <summary>Gets how many entries are seen.</summary>
	public int Count { get; }

	/// <summary>
	/// Adds a parameter whose name the caller wrote.
	/// </summary>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <param name="definition">The query definition, which sizes the array.</param>
	/// <param name="name">The name of the parameter.</param>
	/// <param name="value">The value.</param>
	/// <returns>A value that sees one more entry.</returns>
	public ParamList AddNamed<T>(DbQuery definition, string name, T value)
		=> Append(definition, name, Box(value));

	/// <summary>
	/// Adds a parameter whose name is derived from the argument's text.
	/// </summary>
	/// <typeparam name="T">The type of the value.</typeparam>
	/// <param name="definition">The query definition, which sizes the array.</param>
	/// <param name="expression">The facts of the argument's text.</param>
	/// <param name="value">The value.</param>
	/// <returns>A value that sees one more entry.</returns>
	public ParamList AddDerived<T>(
		DbQuery definition,
		ParameterNames.DerivedExpression expression,
		T value)
		=> Append(definition, expression, Box(value));

	/// <summary>
	/// Adds the members of an object as parameters.
	/// </summary>
	/// <typeparam name="T">The type of the object.</typeparam>
	/// <param name="definition">The query definition, which sizes the array.</param>
	/// <param name="values">The object.</param>
	/// <returns>A value that sees one more entry.</returns>
	public ParamList AddMembers<T>(DbQuery definition, T values)
		where T : notnull
	{
		var holder = new MemberValues<T>(values);
		return Count == 0
			? new(holder, 1)
			: Append(definition, MembersMarker, holder);
	}

	static object Box<T>(T value)
		=> value is null ? (object)NullOf<T>.Instance : value;

	ParamList Append(DbQuery definition, object key, object value)
	{
		object?[] items;
		int slot = Count * 2;
		switch (_items)
		{
			case null:
				items = new object?[2 * definition.ParamCapacity];
				break;

			case MemberValues single:
				// A call that holds only an AddParams object gets its array here.
				items = new object?[2 * Math.Max(definition.ParamCapacity, 2)];
				items[0] = MembersMarker;
				items[1] = single;
				break;

			default:
				var shared = (object?[])_items!;
				if (slot < shared.Length
					&& Interlocked.CompareExchange(ref shared[slot], key, null) is null)
				{
					// The entry is claimed: nothing else will write it.
					shared[slot + 1] = value;
					return new(shared, Count + 1);
				}

				// Full, or the slot is taken by another call that is kept and added to:
				// this one carries on in an array of its own.
				items = new object?[slot < shared.Length ? shared.Length : Math.Max(8, shared.Length * 2)];
				Array.Copy(shared, items, slot);
				break;
		}

		items[slot] = key;
		items[slot + 1] = value;
		return new(items, Count + 1);
	}

	/// <summary>
	/// Gets the entry at an index.
	/// </summary>
	/// <param name="index">The index, below <see cref="Count"/>.</param>
	/// <param name="key">What names the entry.</param>
	/// <param name="value">What the entry holds.</param>
	void Get(int index, out object key, out object value)
	{
		if (_items is object?[] items)
		{
			key = items[index * 2]!;
			value = items[(index * 2) + 1]!;
			return;
		}

		Debug.Assert(index == 0 && _items is MemberValues);
		key = MembersMarker;
		value = _items!;
	}

	/// <summary>
	/// Determines whether any entry needs the real names of the command (a
	/// derived name or members).
	/// </summary>
	public bool NeedsNames
	{
		get
		{
			for (int i = 0; i < Count; i++)
			{
				Get(i, out object key, out _);
				if (key is not string) return true;
			}

			return false;
		}
	}

	/// <summary>
	/// Determines whether the remembered names fit the entries.
	/// </summary>
	/// <remarks>
	/// <see langword="false"/> when an entry that needs a name matches none of
	/// the remembered names.
	/// </remarks>
	/// <param name="names">The remembered names.</param>
	/// <param name="command">The command text or procedure name.</param>
	/// <returns><see langword="true"/> if the names fit; otherwise, <see langword="false"/>.</returns>
	public bool Fits(NameSet names, string command)
	{
		if (names.Names.Length == 0) return true;

		for (int i = 0; i < Count; i++)
		{
			Get(i, out object key, out object value);
			switch (key)
			{
				case ParameterNames.DerivedExpression derived:
					if (names.Match(derived.Expression, command) is null) return false;
					break;

				case string:
					break;

				default:
					// A second look drops a member the command does not take
					// without reading again.
					if (!names.Rechecked
						&& !((MemberValues)value).Fits(names, command))
					{
						return false;
					}

					break;
			}
		}

		return true;
	}

	/// <summary>
	/// Determines whether an entry claims a name with a name of its own.
	/// </summary>
	/// <remarks>
	/// A name of its own is one written or derived, not a member of an
	/// <c>AddParams</c> object: such a parameter wins over a member that
	/// resolves to the same name, whichever was added first, as on a command.
	/// </remarks>
	/// <param name="name">The real name.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="definition">The query definition.</param>
	/// <returns><see langword="true"/> if an entry claims the name; otherwise, <see langword="false"/>.</returns>
	public bool Claims(string name, NameSet? names, DbQuery definition)
	{
		for (int i = 0; i < Count; i++)
		{
			Get(i, out object key, out _);
			switch (key)
			{
				case string written:
					if (DbQueryBinding.SameName(written, name)) return true;
					break;

				case ParameterNames.DerivedExpression derived:
					if (DbQueryBinding.SameName(
						DbQueryBinding.ResolveDerived(derived, names, definition),
						name))
					{
						return true;
					}

					break;
			}
		}

		return false;
	}

	/// <summary>
	/// Adds the parameters to the command, in the order they were added.
	/// </summary>
	/// <param name="command">The command to add the parameters to.</param>
	/// <param name="definition">The query definition.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	public void BindAll(DbCommand command, DbQuery definition, NameSet? names)
	{
		definition.NoteParams(Count);
		for (int i = 0; i < Count; i++)
		{
			Get(i, out object key, out object value);
			switch (key)
			{
				case string written:
					DbQueryBinding.AddValue(command, written, value, definition, names);
					break;

				case ParameterNames.DerivedExpression derived:
					DbQueryBinding.AddValue(
						command,
						DbQueryBinding.ResolveDerived(derived, names, definition),
						value,
						definition,
						names);
					break;

				default:
					((MemberValues)value).Bind(command, definition, names, this);
					break;
			}
		}
	}

	/// <summary>
	/// Determines whether two values hold the same entries.
	/// </summary>
	/// <param name="other">The other value.</param>
	/// <returns><see langword="true"/> if the entries are equal; otherwise, <see langword="false"/>.</returns>
	public bool SameEntries(ParamList other)
	{
		if (Count != other.Count) return false;
		for (int i = 0; i < Count; i++)
		{
			Get(i, out object k1, out object v1);
			other.Get(i, out object k2, out object v2);
			if (!KeyEquals(k1, k2) || !ValueEquals(v1, v2)) return false;
		}

		return true;
	}

	/// <summary>
	/// Gets a hash code of the entries.
	/// </summary>
	/// <returns>The hash code.</returns>
	public int EntriesHash()
	{
		unchecked
		{
			int h = Count;
			for (int i = 0; i < Count; i++)
			{
				Get(i, out object key, out object value);
				h = (h * 31) + (key switch
				{
					string s => s.GetHashCode(),
					ParameterNames.DerivedExpression d => d.Expression.GetHashCode(),
					_ => 0,
				});
				h = (h * 31) + (value is MemberValues m ? m.ValueHash() : value.GetHashCode());
			}

			return h;
		}
	}

	static bool KeyEquals(object a, object b)
		=> (a, b) switch
		{
			(string x, string y) => string.Equals(x, y, StringComparison.Ordinal),
			(ParameterNames.DerivedExpression x, ParameterNames.DerivedExpression y)
				=> string.Equals(x.Expression, y.Expression, StringComparison.Ordinal),
			_ => ReferenceEquals(a, b),
		};

	static bool ValueEquals(object a, object b)
		=> a is MemberValues m ? m.ValueEquals(b) : a.Equals(b);
}

/// <summary>
/// Stands in for a <see langword="null"/> value and remembers its type, so a
/// database null is sent with the type of its argument.
/// </summary>
/// <param name="type">The type of the argument.</param>
sealed class NullValue(Type type)
{
	/// <summary>Gets the type of the argument.</summary>
	public Type ClrType { get; } = type;
}

/// <summary>
/// The one <see cref="NullValue"/> of a type.
/// </summary>
/// <typeparam name="T">The type of the argument.</typeparam>
static class NullOf<T>
{
	internal static readonly NullValue Instance = new(typeof(T));
}

/// <summary>
/// The object whose public members are parameters.
/// </summary>
abstract class MemberValues
{
	/// <summary>
	/// Determines whether every member is one the remembered names match.
	/// </summary>
	/// <param name="names">The remembered names.</param>
	/// <param name="command">The command text or procedure name.</param>
	/// <returns><see langword="true"/> if every member matches; otherwise, <see langword="false"/>.</returns>
	public abstract bool Fits(NameSet names, string command);

	/// <summary>
	/// Adds the members as parameters.
	/// </summary>
	/// <param name="command">The command to add the parameters to.</param>
	/// <param name="definition">The query definition.</param>
	/// <param name="names">The remembered names, or <see langword="null"/> when they are not known.</param>
	/// <param name="whole">The entries of the whole call.</param>
	public abstract void Bind(
		DbCommand command,
		DbQuery definition,
		NameSet? names,
		ParamList whole);

	public abstract bool ValueEquals(object other);

	public abstract int ValueHash();
}

/// <summary>
/// The object whose public members are parameters, typed.
/// </summary>
/// <typeparam name="T">The type of the object.</typeparam>
/// <param name="value">The object.</param>
sealed class MemberValues<T>(T value) : MemberValues
{
	internal T Value => value;

	public override bool Fits(NameSet names, string command)
	{
		foreach (string member in DbQueryBinding.MemberNames(value))
		{
			if (names.Match(member, command) is null) return false;
		}

		return true;
	}

	public override void Bind(
		DbCommand command,
		DbQuery definition,
		NameSet? names,
		ParamList whole)
		=> DbQueryBinding.BindMembers(command, value, definition, names, whole);

	public override bool ValueEquals(object other)
		=> other is MemberValues<T> o
		&& EqualityComparer<T>.Default.Equals(value, o.Value);

	public override int ValueHash()
		=> EqualityComparer<T>.Default.GetHashCode(value!);
}
