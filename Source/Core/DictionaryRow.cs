using System.Collections;
using System.Collections.ObjectModel;

namespace Open.Database.Extensions;

/// <summary>
/// One row of a result set read without a model: the values of the row and, by name, the columns they came from.
/// </summary>
/// <remarks>
/// <para>
/// Made by <c>AsDictionaryRows()</c>. It is a <see langword="readonly"/> <see langword="struct"/> over one
/// <c>object?[]</c> (one slot for each column, <see cref="DBNull"/> read as <see langword="null"/>) and the column
/// names of the result set it came from, which every row of that result set shares: reading a million rows builds
/// the names once and no dictionary per row.
/// </para>
/// <para>
/// The dictionary side (<see cref="Count"/>, <see cref="Keys"/>, <see cref="Values"/>, the enumeration,
/// <see cref="ContainsKey(string)"/>, <see cref="TryGetValue(string, out object?)"/>, the string indexer and
/// <see cref="ToDictionary"/>) is over the <em>distinct</em> column names, and the first column with a name wins.
/// A name is found exactly, else ignoring case. The ordinal indexer and <see cref="FieldCount"/> reach every
/// column, duplicates included.
/// A missing name throws <see cref="KeyNotFoundException"/>; a cell that holds a database NULL is present
/// with the value <see langword="null"/>.
/// </para>
/// <para>
/// Reading a value (the indexers, <see cref="TryGetValue(string, out object?)"/>, <see cref="ContainsKey(string)"/>, <see cref="Count"/>,
/// <see cref="Keys"/> and the enumeration) allocates nothing; the first name that is not found exactly builds the
/// ignoring-case lookup, once for the result set. <see cref="Values"/> and <see cref="ToDictionary"/> build a collection each time,
/// and handing the row to an <see cref="IReadOnlyDictionary{TKey, TValue}"/> boxes it.
/// A result set with no rows gives no row, so no column names: an empty result exposes none.
/// <c>default(DictionaryRow)</c> is an empty row.
/// It is not a model: <c>Map&lt;DictionaryRow&gt;()</c> is not supported.
/// </para>
/// </remarks>
public readonly struct DictionaryRow : IReadOnlyDictionary<string, object?>
{
	readonly object?[]? _values;
	readonly ColumnIndex? _index;

	internal DictionaryRow(object?[] values, ColumnIndex index)
	{
		_values = values;
		_index = index;
	}

	/// <summary>Gets the number of columns of the row, duplicate names included.</summary>
	/// <value>The number of columns.</value>
	public int FieldCount => _values?.Length ?? 0;

	/// <summary>Gets the value of a column by its position.</summary>
	/// <param name="ordinal">The zero-based position of the column.</param>
	/// <value>The value; <see langword="null"/> for a database NULL.</value>
	/// <exception cref="ArgumentOutOfRangeException"><paramref name="ordinal"/> is negative or not less than <see cref="FieldCount"/>.</exception>
	public object? this[int ordinal]
		=> _values is { } values && (uint)ordinal < (uint)values.Length
			? values[ordinal]
			: throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, "There is no column at this position.");

	/// <summary>Gets the value of the first column with a name.</summary>
	/// <param name="key">The name, found exactly, else ignoring case.</param>
	/// <value>The value; <see langword="null"/> for a database NULL.</value>
	/// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
	/// <exception cref="KeyNotFoundException">No column has the name.</exception>
	public object? this[string key]
		=> TryGetValue(key, out object? value)
			? value
			: throw new KeyNotFoundException($"The row has no column named '{key}'.");

	/// <summary>Gets the number of distinct column names.</summary>
	/// <value>The number of distinct names: fewer than <see cref="FieldCount"/> when a name repeats.</value>
	public int Count => _index?.Distinct.Length ?? 0;

	/// <summary>Gets the distinct column names, in the order of their first column.</summary>
	/// <value>The names.</value>
	public IEnumerable<string> Keys => _index?.DistinctNames ?? (IEnumerable<string>)Array.Empty<string>();

	/// <summary>Gets the values of the first column of each distinct name, in the same order as <see cref="Keys"/>.</summary>
	/// <value>The values.</value>
	public IEnumerable<object?> Values
	{
		get
		{
			if (_values is null || _index is null) return [];

			int[] ordinals = _index.DistinctOrdinals;
			var values = new object?[ordinals.Length];
			for (int i = 0; i < ordinals.Length; i++) values[i] = _values[ordinals[i]];

			return values;
		}
	}

	/// <summary>Determines whether a column has the name: among the distinct names, exactly, else ignoring case.</summary>
	/// <param name="key">The name, found exactly, else ignoring case.</param>
	/// <returns><see langword="true"/> if a column has the name; otherwise, <see langword="false"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
	public bool ContainsKey(string key)
		=> TryGetValue(key, out _);

	/// <summary>Gets the value of the first column with a name.</summary>
	/// <param name="key">The name, found exactly, else ignoring case.</param>
	/// <param name="value">The value; <see langword="null"/> for a database NULL or when no column has the name.</param>
	/// <returns><see langword="true"/> if a column has the name (a NULL cell included); otherwise, <see langword="false"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
	public bool TryGetValue(string key, out object? value)
	{
		if (key is null) throw new ArgumentNullException(nameof(key));
		Contract.EndContractBlock();

		if (_index is not null && _values is not null && _index.TryGetOrdinal(key, out int ordinal))
		{
			value = _values[ordinal];
			return true;
		}

		value = null;
		return false;
	}

	/// <summary>Copies the row to a new dictionary of the distinct names, which the caller owns.</summary>
	/// <returns>A dictionary with an entry for the first column of each distinct name (names compared exactly).</returns>
	public Dictionary<string, object?> ToDictionary()
	{
		var copy = new Dictionary<string, object?>(Count, StringComparer.Ordinal);
		if (_values is null || _index is null) return copy;

		string[] names = _index.Distinct;
		int[] ordinals = _index.DistinctOrdinals;
		for (int i = 0; i < names.Length; i++) copy.Add(names[i], _values[ordinals[i]]);

		return copy;
	}

	/// <summary>Gets an enumerator over the distinct names and their values.</summary>
	/// <returns>The enumerator, a struct: a <see langword="foreach"/> over the row allocates nothing.</returns>
	public Enumerator GetEnumerator() => new(_values, _index);

	IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	/// <summary>Enumerates the distinct names of a <see cref="DictionaryRow"/> with their values.</summary>
	public struct Enumerator : IEnumerator<KeyValuePair<string, object?>>
	{
		readonly object?[]? _values;
		readonly ColumnIndex? _index;
		int _position;

		internal Enumerator(object?[]? values, ColumnIndex? index)
		{
			_values = values;
			_index = index;
			_position = -1;
		}

		/// <summary>Gets the current name and value.</summary>
		/// <value>The current pair, or the default pair before the first <see cref="MoveNext"/> and after the last.</value>
		public readonly KeyValuePair<string, object?> Current
			=> _index is not null && _values is not null && (uint)_position < (uint)_index.Distinct.Length
				? new(_index.Distinct[_position], _values[_index.DistinctOrdinals[_position]])
				: default;

		readonly object? IEnumerator.Current => Current;

		/// <summary>Moves to the next distinct name.</summary>
		/// <returns><see langword="true"/> if there is a next name; otherwise, <see langword="false"/>.</returns>
		public bool MoveNext()
			=> _index is not null && ++_position < _index.Distinct.Length;

		/// <summary>Does nothing: there is nothing to release.</summary>
		public readonly void Dispose() { }

		/// <summary>Not supported.</summary>
		/// <exception cref="NotSupportedException">Always.</exception>
		public readonly void Reset() => throw new NotSupportedException();
	}
}

/// <summary>
/// The column names of one result set, built once when its first row is read and shared by every row of that set.
/// </summary>
/// <remarks>
/// Never cached anywhere but in the mapper of the result set it was built for, so ad hoc column lists cannot make
/// anything grow: it lives as long as the query's rows do. The ignore-case lookup is built on the first miss of an exact one.
/// </remarks>
internal sealed class ColumnIndex
{
	readonly Dictionary<string, int> _exact;
	Dictionary<string, int>? _ignoreCase;

	ColumnIndex(string[] names)
	{
		_exact = new(names.Length, StringComparer.Ordinal);
		var distinct = new List<string>(names.Length);
		var ordinals = new List<int>(names.Length);
		for (int i = 0; i < names.Length; i++)
		{
			if (_exact.ContainsKey(names[i])) continue;

			_exact.Add(names[i], i);

			distinct.Add(names[i]);
			ordinals.Add(i);
		}

		Distinct = [.. distinct];
		DistinctNames = new ReadOnlyCollection<string>(Distinct);
		DistinctOrdinals = [.. ordinals];
	}

	/// <summary>Gets the distinct names (exact comparison), first column first.</summary>
	public string[] Distinct { get; }

	/// <summary>Gets the same names as a read-only collection, which is what the public <c>Keys</c> hands out: a cast cannot reach the array.</summary>
	public ReadOnlyCollection<string> DistinctNames { get; }

	/// <summary>Gets the position of the first column of each name in <see cref="Distinct"/>.</summary>
	public int[] DistinctOrdinals { get; }

	/// <summary>Reads the names of the result set a record is positioned on.</summary>
	/// <param name="record">The record.</param>
	/// <returns>The index.</returns>
	public static ColumnIndex Create(IDataRecord record)
	{
		var names = new string[record.FieldCount];
		for (int i = 0; i < names.Length; i++) names[i] = record.GetName(i) ?? string.Empty;

		return new(names);
	}

	/// <summary>Finds the first column with a name: exactly, else ignoring case.</summary>
	/// <param name="name">The name.</param>
	/// <param name="ordinal">The position of the column.</param>
	/// <returns><see langword="true"/> if there is one; otherwise, <see langword="false"/>.</returns>
	public bool TryGetOrdinal(string name, out int ordinal)
	{
		if (_exact.TryGetValue(name, out ordinal)) return true;

		Dictionary<string, int> ignoreCase = _ignoreCase ??= BuildIgnoreCase();
		return ignoreCase.TryGetValue(name, out ordinal);
	}

	Dictionary<string, int> BuildIgnoreCase()
	{
		var map = new Dictionary<string, int>(Distinct.Length, StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < Distinct.Length; i++)
		{
			if (!map.ContainsKey(Distinct[i])) map.Add(Distinct[i], DistinctOrdinals[i]);
		}

		return map;
	}
}
