using System.Data.Common;

namespace Open.Database.Extensions.Core;

/// <summary>
/// Reads a row without a model: one slot for each column, <see cref="DBNull"/> read as <see langword="null"/>.
/// </summary>
/// <remarks>
/// <see cref="IDataRecord.GetValues(object[])"/> reads each column once, in order, so a provider may stream the row
/// (the read mode of a generated mapping applies: SequentialAccess).
/// </remarks>
static class ObjectRow
{
	/// <summary>Reads the current row of a record.</summary>
	/// <param name="record">The record, positioned on a row.</param>
	/// <returns>A new array of the values; a shared empty array when the result set has no columns.</returns>
	public static object?[] Read(IDataRecord record)
	{
		int count = record.FieldCount;
		if (count == 0) return [];

		var values = new object?[count];
		record.GetValues(values!);
		for (int i = 0; i < values.Length; i++)
		{
			if (values[i] is DBNull) values[i] = null;
		}

		return values;
	}
}

/// <summary>The source of <c>AsObjectArrays()</c>: the same static delegate for every result set.</summary>
sealed class ObjectArraySource : IRowSource<object?[]>
{
	public static readonly ObjectArraySource Instance = new();

	static readonly Func<IDataRecord, object?[]> Read = static record => ObjectRow.Read(record);

	ObjectArraySource() { }

	/// <inheritdoc />
	public Func<DbDataReader, object?[]> Mapper(DbDataReader reader) => Read;

	/// <inheritdoc />
	public Func<IDataRecord, object?[]> Mapper(IDataRecord reader) => Read;
}

/// <summary>
/// The source of <c>AsDictionaryRows()</c>: the column names are read once for each result set, when its mapper is
/// made, and every row of the set shares them.
/// </summary>
sealed class DictionaryRowSource : IRowSource<DictionaryRow>
{
	public static readonly DictionaryRowSource Instance = new();

	DictionaryRowSource() { }

	/// <inheritdoc />
	public Func<DbDataReader, DictionaryRow> Mapper(DbDataReader reader) => Create(reader);

	/// <inheritdoc />
	public Func<IDataRecord, DictionaryRow> Mapper(IDataRecord reader) => Create(reader);

	static Func<IDataRecord, DictionaryRow> Create(IDataRecord record)
	{
		ColumnIndex index = ColumnIndex.Create(record);
		return row => new DictionaryRow(ObjectRow.Read(row), index);
	}
}
