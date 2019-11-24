using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Extension methods for IDataRecord access.
	/// </summary>
	public static partial class DataRecordExtensions
	{
		/// <summary>
		/// Returns all the column names for the current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>The enumerable of column names.</returns>
		public static IEnumerable<string> ColumnNames(this IDataRecord record)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var fieldCount = record.FieldCount;
			for (var i = 0; i < fieldCount; i++)
				yield return record.GetName(i);
		}

		/// <summary>
		/// Returns all the column names for the current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>The array of column names.</returns>
		public static string[] GetNames(this IDataRecord record)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var fieldCount = record.FieldCount;
			var columnNames = new string[fieldCount];
			for (var i = 0; i < fieldCount; i++)
				columnNames[i] = record.GetName(i);
			return columnNames;
		}

		/// <summary>
		/// Returns the (name,ordinal) mapping for current result set.
		/// </summary>
		/// <param name="record">The reader to get column names from.</param>
		/// <returns>An enumerable of the mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record)
			=> record.GetNames().Select((n, o) => (Name: n, Ordinal: o));

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <returns>An enumerable of the mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> OrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames)
			=> columnNames.Select(n =>
			{
				// Does do a case-insensitive search after a case-sensitive one.
				// https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getordinal
				var ordinal = record.GetOrdinal(n);
				var name = record.GetName(ordinal); // Get actual casing.
				return (name, ordinal);
			});

		/// <summary>
		/// Returns an array of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
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
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns></returns>
		public static (string Name, int Ordinal)[] GetOrdinalMapping(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		{
			if (columnNames is ICollection<string> cn && cn.Count == 0)
				return Array.Empty<(string Name, int Ordinal)>();

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
				throw new IndexOutOfRangeException($"Invalid columns: {string.Join(", ", mismatch.OrderBy(c => c).ToArray())}", ex);
			}
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <returns>An enumerable for iterating the values of a record.</returns>
		public static IEnumerable<object> EnumerateValues(this IDataRecord record)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var count = record.FieldCount;
			for (var i = 0; i < count; i++)
				yield return record.GetValue(i);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
		public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IEnumerable<int> ordinals)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			foreach (var i in ordinals)
				yield return record.GetValue(i);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
		public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, IList<int> ordinals)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			// Avoid creating an another enumerator if possible.
			var count = ordinals.Count;
			for (var i = 0; i < count; i++)
				yield return record.GetValue(ordinals[i]);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="firstOrdinal">The first ordinal to query.</param>
		/// <param name="remainingOrdinals">The remaining set of ordinals to query.</param>
		/// <returns>An enumerable of values matching the ordinal positions requested.</returns>
		public static IEnumerable<object> EnumerateValuesFromOrdinals(this IDataRecord record, int firstOrdinal, params int[] remainingOrdinals)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			yield return record.GetValue(firstOrdinal);
			var len = remainingOrdinals.Length;
			for (var i = 0; i < len; i++)
				yield return record.GetValue(remainingOrdinals[i]);
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <param name="values">The target span to store the values.</param>
		/// <returns>An array of values matching the ordinal positions requested.</returns>
		public static Span<object> GetValuesFromOrdinals(this IDataRecord record, in ReadOnlySpan<int> ordinals, Span<object> values)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var len = ordinals.Length;
			for (var i = 0; i < len; i++)
				values[i] = record.GetValue(ordinals[i]);
			return values;
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <param name="values">The target list to store the values.</param>
		/// <returns>An array of values matching the ordinal positions requested.</returns>
		public static TList GetValuesFromOrdinals<TList>(this IDataRecord record, IList<int> ordinals, TList values)
			where TList : IList<object>
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			var count = ordinals.Count;
			for (var i = 0; i < count; i++)
				values[i] = record.GetValue(ordinals[i]);
			return values;
		}

		/// <summary>
		/// Produces a selective set of column values based upon the desired ordinal positions.
		/// </summary>
		/// <param name="record">The IDataRecord to query.</param>
		/// <param name="ordinals">The set of ordinals to query.</param>
		/// <returns>An array of values matching the ordinal positions requested.</returns>
		public static object[] GetValuesFromOrdinals(this IDataRecord record, IList<int> ordinals)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			var count = ordinals.Count;
			if (count == 0) return Array.Empty<object>();

			var values = new object[count];
			for (var i = 0; i < count; i++)
				values[i] = record.GetValue(ordinals[i]);
			return values;
		}

		/// <summary>
		/// Returns an enumerable of name to ordinal mappings.
		/// </summary>
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
		/// <param name="columnNames">The requested column names.</param>
		/// <param name="sort">If true, will order the results by ordinal ascending.</param>
		/// <returns>The enumerable of name to ordinal mappings.</returns>
		public static IEnumerable<(string Name, int Ordinal)> MatchingOrdinals(this IDataRecord record, IEnumerable<string> columnNames, bool sort = false)
		{
			// Normalize the requested column names to be lowercase.
			columnNames = columnNames.Select(c =>
			{
				if (string.IsNullOrWhiteSpace(c))
					throw new ArgumentException("Column names cannot be null or whitespace only.");
				return c.ToUpperInvariant();
			});

			var actual = record.OrdinalMapping();
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
		/// <param name="record">The IDataRecord to query the ordinals from.</param>
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
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var fieldCount = record.FieldCount;
			for (var i = 0; i < fieldCount; i++)
				yield return record.GetDataTypeName(i);
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

			var fieldCount = record.FieldCount;
			var results = new string[fieldCount];
			for (var i = 0; i < fieldCount; i++)
				results[i] = record.GetDataTypeName(i);
			return results;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnMap">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<KeyValuePair<int, string>> columnMap)
			=> columnMap
			.ToDictionary(
				c => c.Value,
				c => CoreExtensions.DBNullValueToNull(record.GetValue(c.Key)));

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<(string Name, int Ordinal)> ordinalMapping)
			=> ordinalMapping
			.ToDictionary(
				c => c.Name,
				c => CoreExtensions.DBNullValueToNull(record.GetValue(c.Ordinal)));

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IList<(string Name, int Ordinal)> ordinalMapping)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (ordinalMapping is null) throw new ArgumentNullException(nameof(ordinalMapping));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			var count = ordinalMapping.Count;
			for (var i = 0; i < count; i++)
			{
				var (name, ordinal) = ordinalMapping[i];
				e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="ordinalMapping">The column ids and resultant names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, in ReadOnlySpan<(string Name, int Ordinal)> ordinalMapping)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			var count = ordinalMapping.Length;
			for (var i = 0; i < count; i++)
			{
				var (name, ordinal) = ordinalMapping[i];
				e.Add(name, CoreExtensions.DBNullValueToNull(record[ordinal]));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IEnumerable<string> columnNames)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			foreach (var name in columnNames)
				e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, IList<string> columnNames)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			var count = columnNames.Count;
			for (var i = 0; i < count; i++)
			{
				var name = columnNames[i];
				e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
			}
			return e;
		}

		/// <summary>
		/// Returns the specified column data of IDataRecord as a Dictionary.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="record">The IDataRecord to extract values from.</param>
		/// <param name="columnNames">The column names to query.</param>
		/// <returns>The resultant Dictionary of values.</returns>
		public static Dictionary<string, object?> ToDictionary(this IDataRecord record, in ReadOnlySpan<string> columnNames)
		{
			if (record is null) throw new ArgumentNullException(nameof(record));
			Contract.EndContractBlock();

			var e = new Dictionary<string, object?>();
			var count = columnNames.Length;
			for (var i = 0; i < count; i++)
			{
				var name = columnNames[i];
				e.Add(name, CoreExtensions.DBNullValueToNull(record[name]));
			}
			return e;
		}
	}
}
