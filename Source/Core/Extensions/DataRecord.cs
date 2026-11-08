namespace Open.Database.Extensions;

/// <summary>
/// Extension methods for reading column names, data type names, ordinals, and values from an <see cref="IDataRecord"/>, such as a data reader positioned on a row.
/// </summary>
/// <remarks>
/// These methods read the record's current row and result-set metadata; they never advance the reader. Methods that return a lazy <see cref="IEnumerable{T}"/> read from the record while they are enumerated, so enumerate them before the reader advances or is disposed. Methods that return an array, <see cref="ImmutableArray{T}"/>, or dictionary read everything before they return. Null database values are represented as <see cref="DBNull"/>, except where noted.
/// </remarks>
public static partial class DataRecordExtensions
{
	/// <summary>
	/// Reads the values of the current row into a new array sized to the record's <see cref="IDataRecord.FieldCount"/>.
	/// </summary>
	/// <param name="record">The record (typically a data reader positioned on a row) to read the values from.</param>
	/// <returns>A new array containing the value of each column in ordinal order.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <seealso cref="GetValues(IDataRecord, ArrayPool{object})"/>
	public static object[] GetValues(this IDataRecord record)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		object[] result = new object[record.FieldCount];
		record.GetValues(result);
		return result;
	}

	/// <summary>
	/// Reads the values of the current row into a new array of the specified length.
	/// </summary>
	/// <param name="record">The record (typically a data reader positioned on a row) to read the values from.</param>
	/// <param name="arrayLength">The size of the resultant array. If it is smaller than the field count, only that many leading values are copied; if it is larger, the extra elements are <see langword="null"/>.</param>
	/// <returns>A new array containing the values in ordinal order.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	public static object[] GetValues(this IDataRecord record, int arrayLength)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		object[] result = new object[arrayLength];
		record.GetValues(result);
		return result;
	}

	/// <summary>
	/// Reads the values of the current row into an array rented from <paramref name="arrayPool"/>.
	/// </summary>
	/// <param name="record">The record (typically a data reader positioned on a row) to read the values from.</param>
	/// <param name="minimumArrayLength">The minimum size of the rented array. If it is smaller than the field count, only that many leading values are copied.</param>
	/// <param name="arrayPool">The array pool to rent the buffer from.</param>
	/// <returns>The rented array containing the values in ordinal order.</returns>
	/// <remarks>
	/// The caller owns the rented array and must return it to <paramref name="arrayPool"/>. The array can be longer than requested, and elements beyond the copied values are not cleared, so they can hold values from earlier use. Because the array holds database values, consider returning it with its contents cleared.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="arrayPool"/> is <see langword="null"/>.</exception>
	public static object[] GetValues(this IDataRecord record, int minimumArrayLength, ArrayPool<object> arrayPool)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));

		Contract.EndContractBlock();
		object[] result = arrayPool.Rent(minimumArrayLength);
		record.GetValues(result);
		return result;
	}
	/// <summary>
	/// Reads the values of the current row into an array rented from <paramref name="arrayPool"/>, sized for the record's <see cref="IDataRecord.FieldCount"/>.
	/// </summary>
	/// <param name="record">The record (typically a data reader positioned on a row) to read the values from.</param>
	/// <param name="arrayPool">The array pool to rent the buffer from.</param>
	/// <returns>The rented array containing the values in ordinal order.</returns>
	/// <remarks>
	/// The caller owns the rented array and must return it to <paramref name="arrayPool"/>. The array can be longer than requested, and elements beyond the copied values are not cleared, so they can hold values from earlier use. Because the array holds database values, consider returning it with its contents cleared.
	/// </remarks>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="arrayPool"/> is <see langword="null"/>.</exception>
	public static object[] GetValues(this IDataRecord record, ArrayPool<object> arrayPool)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));

		Contract.EndContractBlock();
		object[] result = arrayPool.Rent(record.FieldCount);
		record.GetValues(result);
		return result;
	}

	/// <summary>
	/// Lazily enumerates the column names of the current result set in ordinal order.
	/// </summary>
	/// <param name="record">The record to get the column names from.</param>
	/// <returns>An enumerable of column names. The names are read from the record as the enumerable is enumerated.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <seealso cref="GetNames(IDataRecord)"/>
	public static IEnumerable<string> ColumnNames(this IDataRecord record)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: ColumnNamesCore(record);

		static IEnumerable<string> ColumnNamesCore(IDataRecord record)
		{
			int fieldCount = record.FieldCount;
			for (int i = 0; i < fieldCount; i++)
				yield return record.GetName(i);
		}
	}

	/// <summary>
	/// Gets all the column names of the current result set in ordinal order.
	/// </summary>
	/// <param name="record">The record to get the column names from.</param>
	/// <returns>An immutable array of column names.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <seealso cref="ColumnNames(IDataRecord)"/>
	public static ImmutableArray<string> GetNames(this IDataRecord record)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int fieldCount = record.FieldCount;
		ImmutableArray<string>.Builder columnNames = ImmutableArray.CreateBuilder<string>(fieldCount);
		columnNames.Count = fieldCount;
		for (int i = 0; i < fieldCount; i++)
			columnNames[i] = record.GetName(i);
		return columnNames.MoveToImmutable();
	}

	/// <summary>
	/// Gets the column names at the specified ordinals.
	/// </summary>
	/// <param name="record">The record to get the column names from.</param>
	/// <param name="ordinals">The ordinals to look up. The names are returned in this order.</param>
	/// <returns>An immutable array of column names, one for each ordinal.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <exception cref="IndexOutOfRangeException">An ordinal is not a valid column index for the record.</exception>
	public static ImmutableArray<string> GetNames(this IDataRecord record, IEnumerable<int> ordinals)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		return ordinals.Select(o => record.GetName(o)).ToImmutableArray();
	}

	/// <summary>
	/// Lazily enumerates the (name, ordinal) pairs of every column in the current result set.
	/// </summary>
	/// <param name="record">The record to get the column names from.</param>
	/// <returns>An enumerable of the mappings in ordinal order.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record)
		=> record.ColumnNames().Select((n, o) => (Name: n, Ordinal: o));

	/// <summary>
	/// Lazily looks up the ordinal of each requested column.
	/// </summary>
	/// <param name="record">The record to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names. The lookup uses <see cref="IDataRecord.GetOrdinal(string)"/>, which tries a case-sensitive match before a case-insensitive one.</param>
	/// <returns>An enumerable of mappings, in the order requested. Each name is the column's actual name in the record, which can differ in casing from the requested name.</returns>
	/// <exception cref="IndexOutOfRangeException">A requested column does not exist. It is thrown while the result is enumerated.</exception>
	/// <seealso cref="GetOrdinalMapping(IDataRecord, IEnumerable{string}, bool)"/>
	/// <seealso cref="MatchingOrdinals(IDataRecord, IEnumerable{string}, bool)"/>
	public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames)
		=> columnNames.Select(n =>
		{
			// Does do a case-insensitive search after a case-sensitive one.
			// https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getordinal
			int ordinal = record.GetOrdinal(n);
			string name = record.GetName(ordinal); // Get actual casing.
			return (name, ordinal);
		});

	/// <summary>
	/// Lazily looks up the ordinal of each requested column, optionally ordered by ordinal.
	/// </summary>
	/// <param name="record">The record to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort"><see langword="true"/> to order the results by ordinal ascending; <see langword="false"/> to keep the requested order.</param>
	/// <returns>An enumerable of mappings.</returns>
	/// <exception cref="IndexOutOfRangeException">A requested column does not exist. It is thrown while the result is enumerated.</exception>
	/// <seealso cref="GetOrdinalMapping(IDataRecord, IEnumerable{string}, bool)"/>
	/// <seealso cref="MatchingOrdinals(IDataRecord, IEnumerable{string}, bool)"/>
	public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort)
		=> sort
		? OrdinalMapping(record, columnNames).OrderBy(m => m.Ordinal)
		: OrdinalMapping(record, columnNames);

	/// <summary>
	/// Looks up the ordinal of each requested column and returns the mappings as an array.
	/// </summary>
	/// <param name="record">The record to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort"><see langword="true"/> to order the results by ordinal ascending; <see langword="false"/> to keep the requested order.</param>
	/// <returns>An array of mappings. It is empty if <paramref name="columnNames"/> is an empty collection.</returns>
	/// <exception cref="IndexOutOfRangeException">One or more requested columns do not exist. The message lists all the invalid names.</exception>
	/// <seealso cref="GetMatchingOrdinals(IDataRecord, IEnumerable{string}, bool)"/>
	public static (string Name, int Ordinal)[] GetOrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
	{
		if (columnNames is ICollection<string> cn && cn.Count == 0)
			return [];

		try
		{
			return record
				.OrdinalMapping(columnNames, sort)
				.ToArray();
		}
		catch (IndexOutOfRangeException ex)
		{
			var mismatch = new HashSet<string>(columnNames);
			mismatch.ExceptWith(record.GetNames());

			// Columns not mapped correctly.  Report all columns that are mismatched/missing.
			throw new IndexOutOfRangeException($"Invalid columns: {string.Join(", ", mismatch.OrderBy(c => c))}", ex);
		}
	}

	/// <summary>
	/// Lazily enumerates the value of every column of the current row in ordinal order.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <returns>An enumerable of values. The values are read from the record as the enumerable is enumerated.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	public static IEnumerable<object> EnumerateValues(this IDataRecord record)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: EnumerateValuesCore(record);

		static IEnumerable<object> EnumerateValuesCore(IDataRecord record)
		{
			int count = record.FieldCount;
			for (int i = 0; i < count; i++)
				yield return record.GetValue(i);
		}
	}

	/// <summary>
	/// Lazily enumerates the values of the columns at the specified ordinals.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinals">The ordinals to read. The values are returned in this order.</param>
	/// <returns>An enumerable of values matching the ordinals requested. The values are read from the record as the enumerable is enumerated.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="ordinals"/> is <see langword="null"/>.</exception>
	/// <exception cref="IndexOutOfRangeException">An ordinal is not a valid column index for the record. It is thrown while the result is enumerated.</exception>
	public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IEnumerable<int> ordinals)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: EnumerateValuesFromOrdinalsCore(record, ordinals);

		static IEnumerable<object> EnumerateValuesFromOrdinalsCore(IDataRecord record, IEnumerable<int> ordinals)
		{
			foreach (int i in ordinals)
				yield return record.GetValue(i);
		}
	}

	/// <inheritdoc cref="EnumerateValuesFromOrdinals(IDataRecord, IEnumerable{int})"/>
	public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IReadOnlyList<int> ordinals)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: EnumerateValuesFromOrdinalsCore(record, ordinals);

		static IEnumerable<object> EnumerateValuesFromOrdinalsCore(IDataRecord record, IReadOnlyList<int> ordinals)
		{
			// Avoid creating an another enumerator if possible.
			int count = ordinals.Count;
			for (int i = 0; i < count; i++)
				yield return record.GetValue(ordinals[i]);
		}
	}

	/// <summary>
	/// Lazily enumerates the values of the columns at the specified ordinals.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="firstOrdinal">The first ordinal to read.</param>
	/// <param name="remainingOrdinals">The remaining ordinals to read, in order.</param>
	/// <returns>An enumerable of values matching the ordinals requested. The values are read from the record as the enumerable is enumerated.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <exception cref="IndexOutOfRangeException">An ordinal is not a valid column index for the record. It is thrown while the result is enumerated.</exception>
	public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, int firstOrdinal, params IEnumerable<int> remainingOrdinals)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: EnumerateValuesFromOrdinalsCore(record, firstOrdinal, remainingOrdinals);

		static IEnumerable<object> EnumerateValuesFromOrdinalsCore(IDataRecord record, int firstOrdinal, IEnumerable<int> remainingOrdinals)
		{
			yield return record.GetValue(firstOrdinal);
			foreach (int i in remainingOrdinals)
				yield return record.GetValue(i);
		}
	}

	/// <summary>
	/// Reads the values of the columns at the specified ordinals into the provided span.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinals">The ordinals to read.</param>
	/// <param name="values">The span that receives the values. It must be at least as long as <paramref name="ordinals"/>.</param>
	/// <returns>The provided span, updated with values matching the ordinal positions requested.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <exception cref="IndexOutOfRangeException"><paramref name="values"/> is shorter than <paramref name="ordinals"/>, or an ordinal is not a valid column index for the record.</exception>
	public static Span<object> GetValuesFromOrdinals(this IDataRecord record, ReadOnlySpan<int> ordinals, Span<object> values)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int len = ordinals.Length;
		for (int i = 0; i < len; i++)
			values[i] = record.GetValue(ordinals[i]);
		return values;
	}

	/// <summary>
	/// Reads the values of the columns at the specified ordinals into the provided list.
	/// </summary>
	/// <typeparam name="TList">The type of the list that receives the values.</typeparam>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinals">The ordinals to read.</param>
	/// <param name="values">The list that receives the values. It must already have at least as many elements as <paramref name="ordinals"/>.</param>
	/// <returns>The provided list, updated with values matching the ordinal positions requested.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="ordinals"/> is <see langword="null"/>.</exception>
	public static TList GetValuesFromOrdinals<TList>(this IDataRecord record, IReadOnlyList<int> ordinals, TList values)
		where TList : IList<object>
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
		Contract.EndContractBlock();

		int count = ordinals.Count;
		for (int i = 0; i < count; i++)
			values[i] = record.GetValue(ordinals[i]);
		return values;
	}

	/// <summary>
	/// Reads the values of the columns at the specified ordinals into a new array.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinals">The ordinals to read. The values are returned in this order.</param>
	/// <returns>A new array of values matching the ordinal positions requested. It is empty if <paramref name="ordinals"/> is empty.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="ordinals"/> is <see langword="null"/>.</exception>
	/// <exception cref="IndexOutOfRangeException">An ordinal is not a valid column index for the record.</exception>
	public static object[] GetValuesFromOrdinals(this IDataRecord record, IReadOnlyList<int> ordinals)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
		Contract.EndContractBlock();

		int count = ordinals.Count;
		if (count == 0) return [];

		object[] values = new object[count];
		for (int i = 0; i < count; i++)
			values[i] = record.GetValue(ordinals[i]);
		return values;
	}

	/// <summary>
	/// Finds the requested columns that exist in the record, matching names case-insensitively.
	/// </summary>
	/// <param name="record">The record to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names. Names that do not exist in the record are skipped rather than reported.</param>
	/// <param name="sort"><see langword="true"/> to return the mappings in the record's column order; <see langword="false"/> to return them in the requested order.</param>
	/// <returns>An enumerable of (name, ordinal) mappings, where each name is the column's actual name in the record.</returns>
	/// <remarks>
	/// Use <see cref="OrdinalMapping(IDataRecord, IEnumerable{string})"/> or <see cref="GetOrdinalMapping(IDataRecord, IEnumerable{string}, bool)"/> instead when a missing column should be an error.
	/// </remarks>
	/// <exception cref="ArgumentException">A requested name is <see langword="null"/>, empty, or white space. It can be thrown when the result is enumerated rather than when this method is called.</exception>
	public static IEnumerable<(string Name, int Ordinal)> MatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
	{
		// Match case-insensitively via an OrdinalIgnoreCase comparer instead of allocating an
		// upper-cased copy of every requested and every actual column name.
		IEnumerable<string> requestedNames = ValidateColumnNames(columnNames);
		IEnumerable<(string Name, int Ordinal)> actual = record.OrdinalMapping();

		if (sort)
		{
			var requested = new HashSet<string>(requestedNames, StringComparer.OrdinalIgnoreCase);
			// Return the actual columns, in record order, whose names were requested.
			return actual.Where(m => requested.Contains(m.Name));
		}
		else
		{
			// Map actual columns case-insensitively so the requested order can be preserved.
			var actualColumns = new Dictionary<string, (string Name, int Ordinal)>(StringComparer.OrdinalIgnoreCase);
			foreach ((string Name, int Ordinal) m in actual)
				actualColumns[m.Name] = m;
			return requestedNames
				.Where(actualColumns.ContainsKey)
				.Select(c => actualColumns[c]);
		}

		static IEnumerable<string> ValidateColumnNames(IEnumerable<string> names)
		{
			foreach (string c in names)
			{
				yield return string.IsNullOrWhiteSpace(c)
					? throw new ArgumentException("Column names cannot be null or whitespace only.")
					: c;
			}
		}
	}

	/// <summary>
	/// Finds the requested columns that exist in the record, matching names case-insensitively, and returns the mappings as an array.
	/// </summary>
	/// <param name="record">The record to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names. Names that do not exist in the record are skipped rather than reported.</param>
	/// <param name="sort"><see langword="true"/> to return the mappings in the record's column order; <see langword="false"/> to return them in the requested order.</param>
	/// <returns>An array of (name, ordinal) mappings, where each name is the column's actual name in the record.</returns>
	/// <remarks>
	/// Use <see cref="OrdinalMapping(IDataRecord, IEnumerable{string})"/> or <see cref="GetOrdinalMapping(IDataRecord, IEnumerable{string}, bool)"/> instead when a missing column should be an error.
	/// </remarks>
	/// <exception cref="ArgumentException">A requested name is <see langword="null"/>, empty, or white space. It can be thrown when the result is enumerated rather than when this method is called.</exception>
	public static (string Name, int Ordinal)[] GetMatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		=> MatchingOrdinals(record, columnNames, sort).ToArray();

	/// <summary>
	/// Lazily enumerates the provider's data type name of every column of the current result set, in ordinal order.
	/// </summary>
	/// <param name="record">The record to get the data type names from.</param>
	/// <returns>An enumerable of data type names, such as <c>nvarchar</c> for SQL Server. The names are read from the record as the enumerable is enumerated.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	public static IEnumerable<string> DataTypeNames(this IDataRecord record)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: DataTypeNamesCore(record);

		static IEnumerable<string> DataTypeNamesCore(IDataRecord record)
		{
			int fieldCount = record.FieldCount;
			for (int i = 0; i < fieldCount; i++)
				yield return record.GetDataTypeName(i);
		}
	}

	/// <summary>
	/// Gets the provider's data type name of every column of the current result set, in ordinal order.
	/// </summary>
	/// <param name="record">The record to get the data type names from.</param>
	/// <returns>An array of data type names, such as <c>nvarchar</c> for SQL Server.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	public static string[] GetDataTypeNames(this IDataRecord record)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int fieldCount = record.FieldCount;
		string[] results = new string[fieldCount];
		for (int i = 0; i < fieldCount; i++)
			results[i] = record.GetDataTypeName(i);
		return results;
	}

	/// <summary>
	/// Reads the specified columns of the current row into a dictionary keyed by the supplied names.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="columnMap">The ordinals to read, paired with the key to store each value under.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentException">Two entries have the same key.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<KeyValuePair<int, string>> columnMap)
	{
		if (columnMap is null) throw new ArgumentNullException(nameof(columnMap));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>(columnMap is ICollection<KeyValuePair<int, string>> collection ? collection.Count : 0);
		foreach (KeyValuePair<int, string> c in columnMap)
			e.Add(c.Value, CoreExtensions.DBNullValueToNull(record.GetValue(c.Key)));
		return e;
	}

	/// <summary>
	/// Reads the specified columns of the current row into a dictionary keyed by column name.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinalMapping">The (name, ordinal) pairs to read, as produced by <see cref="OrdinalMapping(IDataRecord)"/>.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentException">Two entries have the same name.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<(string Name, int Ordinal)> ordinalMapping)
	{
		if (ordinalMapping is null) throw new ArgumentNullException(nameof(ordinalMapping));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>(ordinalMapping is ICollection<(string Name, int Ordinal)> collection ? collection.Count : 0);
		foreach ((string name, int ordinal) in ordinalMapping)
			e.Add(name, CoreExtensions.DBNullValueToNull(record.GetValue(ordinal)));
		return e;
	}

	/// <summary>
	/// Reads the specified columns of the current row into a dictionary keyed by column name.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinalMapping">The (name, ordinal) pairs to read, as produced by <see cref="GetOrdinalMapping(IDataRecord, IEnumerable{string}, bool)"/>.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="ordinalMapping"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two entries have the same name.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IReadOnlyList<(string Name, int Ordinal)> ordinalMapping)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (ordinalMapping is null) throw new ArgumentNullException(nameof(ordinalMapping));
		Contract.EndContractBlock();

		int count = ordinalMapping.Count;
		var e = new Dictionary<string, object?>(count);
		for (int i = 0; i < count; i++)
		{
			(string name, int ordinal) = ordinalMapping[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
		}

		return e;
	}

	/// <summary>
	/// Reads the specified columns of the current row into a dictionary keyed by column name.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="ordinalMapping">The (name, ordinal) pairs to read.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">Two entries have the same name.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, ReadOnlySpan<(string Name, int Ordinal)> ordinalMapping)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int count = ordinalMapping.Length;
		var e = new Dictionary<string, object?>(count);
		for (int i = 0; i < count; i++)
		{
			(string name, int ordinal) = ordinalMapping[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
		}

		return e;
	}

	/// <summary>
	/// Reads the named columns of the current row into a dictionary keyed by column name.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="columnNames">The names of the columns to read. Each name is looked up with the record's string indexer.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> or <paramref name="columnNames"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">A name is repeated.</exception>
	/// <exception cref="IndexOutOfRangeException">A name is not a column of the record.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>(columnNames is ICollection<string> collection ? collection.Count : 0);
		foreach (string name in columnNames)
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		return e;
	}

	/// <inheritdoc cref="ToDictionary(IDataRecord, IEnumerable{string})"/>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IReadOnlyList<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		int count = columnNames.Count;
		var e = new Dictionary<string, object?>(count);
		for (int i = 0; i < count; i++)
		{
			string name = columnNames[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		}

		return e;
	}

	/// <summary>
	/// Reads the named columns of the current row into a dictionary keyed by column name.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <param name="columnNames">The names of the columns to read. Each name is looked up with the record's string indexer.</param>
	/// <returns>A new dictionary of values. <see cref="DBNull"/> values are converted to <see langword="null"/>.</returns>
	/// <exception cref="ArgumentNullException"><paramref name="record"/> is <see langword="null"/>.</exception>
	/// <exception cref="ArgumentException">A name is repeated.</exception>
	/// <exception cref="IndexOutOfRangeException">A name is not a column of the record.</exception>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, ReadOnlySpan<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int count = columnNames.Length;
		var e = new Dictionary<string, object?>(count);
		for (int i = 0; i < count; i++)
		{
			string name = columnNames[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		}

		return e;
	}
}
