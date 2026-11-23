using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Open.Database.Extensions.Generator;

// What the pipeline carries between its steps: plain, equatable data (strings, numbers, flags), never a symbol,
// so an edit that does not change a model or a call site leaves every step cached.

/// <summary>An immutable array compared by its elements, so a record holding one stays equatable.</summary>
readonly struct EquatableArray<T>(ImmutableArray<T> items) : IEquatable<EquatableArray<T>>, IEnumerable<T>
	where T : IEquatable<T>
{
	public ImmutableArray<T> Items => items.IsDefault ? ImmutableArray<T>.Empty : items;

	public int Length => Items.Length;

	public T this[int index] => Items[index];

	public bool Equals(EquatableArray<T> other)
	{
		ImmutableArray<T> a = Items, b = other.Items;
		if (a.Length != b.Length) return false;
		for (int i = 0; i < a.Length; i++)
			if (!a[i].Equals(b[i])) return false;

		return true;
	}

	public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

	public override int GetHashCode()
	{
		int h = 17;
		foreach (T item in Items) h = unchecked(h * 31 + item.GetHashCode());
		return h;
	}

	public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)Items).GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// One way to read a column into a slot, for one reported column type.
/// <see cref="Expression"/> holds <c>{0}</c> for the ordinal; a general read (<see cref="General"/>) is a cast of <c>GetValue</c> (for a nullable member its NULL is tested on the value).
/// </summary>
sealed record ReadCase(string ColumnType, string Expression, bool General);

/// <summary>What one column can feed (RowMap's Slot): a constructor parameter, or a settable public instance property, in slot order.</summary>
sealed record SlotSpec(
	string Name,
	// The constructor parameter's name, for a slot fed through the constructor; null for a property that is set.
	string? Parameter,
	// The constructor parameter's position, -1 for a property that is set.
	int Position,
	// The member's declared type, and the type a value of it is read as (the underlying type of a Nullable<T>).
	string Type,
	string Underlying,
	bool NullableValue,
	// Declared non-nullable (a value type other than Nullable<T>, or a reference annotated non-nullable): read without a NULL test.
	bool NonNullable,
	// A property that can only be set in an object initializer: init-only, or required while the constructor does not set the required members.
	bool InitBound,
	// A column must feed it in every result set.
	bool Required,
	// What the member declares: [Column(Name)], [Column(Order = n)], [NotMapped].
	string? Column,
	int Order,
	bool NotMapped,
	// The C# expression a constructor parameter receives when no column feeds it; null for a property.
	string? Fallback,
	// The reads, the exact one first (the one Full mode needs).
	EquatableArray<ReadCase> Cases);

sealed record ModelSpec(
	string Type,
	// Built through its one public constructor (true) or created then set (false).
	bool UseConstructor,
	// How a created model starts: "new T()" or "default(T)" (a struct with no public parameterless constructor).
	string Create,
	int ParameterCount,
	bool Decorated,
	// Every mappable slot declares its ordinal, so a passing result set is read at constant ordinals.
	bool AllOrdered,
	EquatableArray<SlotSpec> Slots);

/// <summary>Which overload of <c>Map&lt;T&gt;()</c> the call binds to.</summary>
enum Receiver
{
	ExecuteReader,
	Command,
	DataReader,
}

/// <summary>One intercepted call: where it is, what it binds to, and the model it maps.</summary>
sealed record Site(int Version, string Data, Receiver Receiver, string ReceiverType, string ReturnType, ModelSpec Model);

/// <summary>What the consuming compilation allows and asks for.</summary>
sealed record Options(bool Enabled, bool Counters);
