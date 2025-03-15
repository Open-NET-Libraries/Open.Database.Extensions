namespace Open.Database.Extensions;

/// <summary>
/// Extension methods for IDataRecord access.
/// </summary>
public static partial class DataRecordExtensions
{
	/// <inheritdoc cref="GetValues(IDataRecord, int)"/>
	public static object[] GetValues(this IDataRecord record)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		object[] result = new object[record.FieldCount];
		record.GetValues(result);
		return result;
	}

	/// <summary>
	/// Returns an array of values with the specified field count.
	/// </summary>
	/// <param name="record">The reader to get column names from.</param>
	/// <param name="arrayLength">The size of the resultant array.</param>
	/// <returns>The array of values.</returns>
	public static object[] GetValues(this IDataRecord record, int arrayLength)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		object[] result = new object[arrayLength];
		record.GetValues(result);
		return result;
	}

	/// <param name="record">The reader to get column names from.</param>
	/// <param name="minimumArrayLength">The minimum size of the resultant array.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <inheritdoc cref="GetValues(IDataRecord, int)"/>
	public static object[] GetValues(this IDataRecord record, int minimumArrayLength, ArrayPool<object> arrayPool)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (arrayPool is null) throw new ArgumentNullException(nameof(arrayPool));

		Contract.EndContractBlock();
		object[] result = arrayPool.Rent(minimumArrayLength);
		record.GetValues(result);
		return result;
	}
	/// <param name="record">The reader to get column names from.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <inheritdoc cref="GetValues(IDataRecord, int)"/>
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
	/// Returns all the column names for the current result set.
	/// </summary>
	/// <param name="record">The reader to get column names from.</param>
	/// <returns>The enumerable of column names.</returns>
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
	/// Returns all the column names for the current result set.
	/// </summary>
	/// <param name="record">The reader to get column names from.</param>
	/// <returns>The array of column names.</returns>
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
	/// Returns all the column names for the current result set by index provided by the ordinals.
	/// </summary>
	/// <param name="record">The reader to get column names from.</param>
	/// <param name="ordinals">The list (and order) of ordinals to look up.</param>
	/// <returns>The array of column names.</returns>
	public static ImmutableArray<string> GetNames(this IDataRecord record, IEnumerable<int> ordinals)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		return ordinals.Select(o => record.GetName(o)).ToImmutableArray();
	}

	/// <summary>
	/// Returns the (name,ordinal) mapping for current result set.
	/// </summary>
	/// <param name="record">The reader to get column names from.</param>
	/// <returns>An enumerable of the mappings.</returns>
	public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record)
		=> record.ColumnNames().Select((n, o) => (Name: n, Ordinal: o));

	/// <summary>
	/// Returns an array of name to ordinal mappings.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <returns>An enumerable of the mappings.</returns>
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
	/// Returns an array of name to ordinal mappings.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort">If true, will order the results by ordinal ascending.</param>
	/// <returns>An enumerable of the mappings.</returns>
	public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort)
		=> sort
		? OrdinalMapping(record, columnNames).OrderBy(m => m.Ordinal)
		: OrdinalMapping(record, columnNames);

	/// <summary>
	/// Returns an array of name to ordinal mappings.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort">If true, will order the results by ordinal ascending.</param>
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
	/// Produces an array of values based upon their ordinal positions.
	/// </summary>
	/// <inheritdoc cref="EnumerateValuesFromOrdinals(IDataRecord, IEnumerable{int})"/>
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
	/// Produces a selective set of column values based upon the desired ordinal positions.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query.</param>
	/// <param name="ordinals">The set of ordinals to query.</param>
	/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
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
	public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IList<int> ordinals)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: EnumerateValuesFromOrdinalsCore(record, ordinals);

		static IEnumerable<object> EnumerateValuesFromOrdinalsCore(IDataRecord record, IList<int> ordinals)
		{
			// Avoid creating an another enumerator if possible.
			int count = ordinals.Count;
			for (int i = 0; i < count; i++)
				yield return record.GetValue(ordinals[i]);
		}
	}

	/// <param name="record">The <see cref="IDataRecord"/> to query.</param>
	/// <param name="firstOrdinal">The first ordinal to query.</param>
	/// <param name="remainingOrdinals">The remaining set of ordinals to query.</param>
	/// <inheritdoc cref="EnumerateValuesFromOrdinals(IDataRecord, IEnumerable{int})"/>
	public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, int firstOrdinal, params int[] remainingOrdinals)
	{
		return record is null
			? throw new ArgumentNullException(nameof(record))
			: EnumerateValuesFromOrdinalsCore(record, firstOrdinal, remainingOrdinals);

		static IEnumerable<object> EnumerateValuesFromOrdinalsCore(IDataRecord record, int firstOrdinal, int[] remainingOrdinals)
		{
			yield return record.GetValue(firstOrdinal);
			int len = remainingOrdinals.Length;
			for (int i = 0; i < len; i++)
				yield return record.GetValue(remainingOrdinals[i]);
		}
	}

	/// <param name="record">The <see cref="IDataRecord"/> to query.</param>
	/// <param name="ordinals">The list of ordinals to query.</param>
	/// <param name="values">The target to store the values.</param>
	/// <returns>The provided span, updated with values matching the ordinal positions requested.</returns>
	/// <inheritdoc cref="EnumerateValuesFromOrdinals(IDataRecord, IEnumerable{int})"/>
	public static Span<object> GetValuesFromOrdinals(this IDataRecord record, ReadOnlySpan<int> ordinals, Span<object> values)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		int len = ordinals.Length;
		for (int i = 0; i < len; i++)
			values[i] = record.GetValue(ordinals[i]);
		return values;
	}

	/// <returns>The provided list, updated with values matching the ordinal positions requested.</returns>
	/// <inheritdoc cref="GetValuesFromOrdinals(IDataRecord, ReadOnlySpan{int}, Span{object})"/>
	public static TList GetValuesFromOrdinals<TList>(this IDataRecord record, IList<int> ordinals, TList values)
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

	/// <returns>An array of values matching the ordinal positions requested.</returns>
	/// <inheritdoc cref="GetValuesFromOrdinals(IDataRecord, ReadOnlySpan{int}, Span{object})"/>
	public static object[] GetValuesFromOrdinals(this IDataRecord record, IList<int> ordinals)
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
	/// Returns an enumerable of name to ordinal mappings.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort">If true, will order the results by ordinal ascending.</param>
	/// <returns>The enumerable of name to ordinal mappings.</returns>
	public static IEnumerable<(string Name, int Ordinal)> MatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
	{
		// Normalize the requested column names to be lowercase.
		columnNames = columnNames.Select(c
			=> string.IsNullOrWhiteSpace(c)
				? throw new ArgumentException("Column names cannot be null or whitespace only.")
				: c.ToUpperInvariant());

		IEnumerable<(string Name, int Ordinal)> actual = record.OrdinalMapping();
		if (sort)
		{
			var requested = new HashSet<string>(columnNames);
			// Return actual values based upon if their lower-case counterparts exist in the requested.
			return actual
				.Where(m => requested.Contains(m.Name.ToUpperInvariant()));
		}
		else
		{
			// Create a map of lower-case keys to actual.
			var actualColumns = actual.ToDictionary(m => m.Name.ToUpperInvariant(), m => m);
			return columnNames
				.Where(c => actualColumns.ContainsKey(c)) // Select lower case column names if they exist in the dictionary.
				.Select(c => actualColumns[c]); // Then select the actual values based upon that key.
		}
	}

	/// <summary>
	/// Returns an array of name to ordinal mappings.
	/// </summary>
	/// <param name="record">The <see cref="IDataRecord"/> to query the ordinals from.</param>
	/// <param name="columnNames">The requested column names.</param>
	/// <param name="sort">If true, will order the results by ordinal ascending.</param>
	/// <returns>The array of name to ordinal mappings.</returns>
	public static (string Name, int Ordinal)[] GetMatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		=> MatchingOrdinals(record, columnNames, sort).ToArray();

	/// <summary>
	/// Returns all the data type names for the columns of current result set.
	/// </summary>
	/// <param name="record">The reader to get data type names from.</param>
	/// <returns>The enumerable of data type names.</returns>
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
	/// Returns all the data type names for the columns of current result set.
	/// </summary>
	/// <param name="record">The reader to get data type names from.</param>
	/// <returns>The array of data type names.</returns>
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
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="columnMap">The column ids and resultant names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<KeyValuePair<int, string>> columnMap)
		=> columnMap
		.ToDictionary(
			c => c.Value,
			c => CoreExtensions.DBNullValueToNull(record.GetValue(c.Key)));

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<(string Name, int Ordinal)> ordinalMapping)
		=> ordinalMapping
		.ToDictionary(
			c => c.Name,
			c => CoreExtensions.DBNullValueToNull(record.GetValue(c.Ordinal)));

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IList<(string Name, int Ordinal)> ordinalMapping)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (ordinalMapping is null) throw new ArgumentNullException(nameof(ordinalMapping));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>();
		int count = ordinalMapping.Count;
		for (int i = 0; i < count; i++)
		{
			(string name, int ordinal) = ordinalMapping[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
		}

		return e;
	}

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, ReadOnlySpan<(string Name, int Ordinal)> ordinalMapping)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>();
		int count = ordinalMapping.Length;
		for (int i = 0; i < count; i++)
		{
			(string name, int ordinal) = ordinalMapping[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
		}

		return e;
	}

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="columnNames">The column names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>();
		foreach (string name in columnNames)
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		return e;
	}

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="columnNames">The column names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IList<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>();
		int count = columnNames.Count;
		for (int i = 0; i < count; i++)
		{
			string name = columnNames[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		}

		return e;
	}

	/// <summary>
	/// Returns the specified column data of <see cref="IDataRecord"/> as a Dictionary.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are converted to null.</remarks>
	/// <param name="record">The <see cref="IDataRecord"/> to extract values from.</param>
	/// <param name="columnNames">The column names to query.</param>
	/// <returns>The resultant Dictionary of values.</returns>
	public static Dictionary<string, object?> ToDictionary(this IDataRecord record, ReadOnlySpan<string> columnNames)
	{
		if (record is null) throw new ArgumentNullException(nameof(record));
		Contract.EndContractBlock();

		var e = new Dictionary<string, object?>();
		int count = columnNames.Length;
		for (int i = 0; i < count; i++)
		{
			string name = columnNames[i];
			e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
		}

		return e;
	}
}
