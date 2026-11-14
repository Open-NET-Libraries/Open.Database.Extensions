using System.Collections;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A <see cref="DbDataReader"/> over typed column arrays (<c>int[]</c>, <c>long[]</c>, <c>string[]</c>).
/// Its typed getters read straight from the arrays, so serving a row allocates nothing:
/// unlike <see cref="DataTableReader"/>, whose <c>GetInt32</c> boxes the value, the bytes a test
/// measures are the library's own and do not change when the JIT promotes the code.
/// <see cref="GetValue"/> throws, so a mapper that falls back to a boxed read fails loudly.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class ArrayReader(string[] names, Array[] columns) : DbDataReader
{
	int _row = -1;

	int Count => columns.Length == 0 ? 0 : columns[0].Length;

	public override bool Read() => ++_row < Count;

	public override int FieldCount => columns.Length;
	public override string GetName(int ordinal) => names[ordinal];
	public override Type GetFieldType(int ordinal)
		=> columns[ordinal].GetType().GetElementType()!;
	public override string GetDataTypeName(int ordinal)
		=> GetFieldType(ordinal).Name;

	public override int GetOrdinal(string name)
	{
		for (int i = 0; i < names.Length; i++)
			if (string.Equals(names[i], name, StringComparison.OrdinalIgnoreCase)) return i;
		throw new IndexOutOfRangeException(name);
	}

	public override bool IsDBNull(int ordinal)
		=> columns[ordinal] is string[] strings && strings[_row] is null;

	public override int GetInt32(int ordinal) => ((int[])columns[ordinal])[_row];
	public override long GetInt64(int ordinal) => ((long[])columns[ordinal])[_row];
	public override string GetString(int ordinal) => ((string[])columns[ordinal])[_row];

	public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
	public override byte GetByte(int ordinal) => throw new NotSupportedException();
	public override char GetChar(int ordinal) => throw new NotSupportedException();
	public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
	public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
	public override double GetDouble(int ordinal) => throw new NotSupportedException();
	public override float GetFloat(int ordinal) => throw new NotSupportedException();
	public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
	public override short GetInt16(int ordinal) => throw new NotSupportedException();
	public override object GetValue(int ordinal) => throw new NotSupportedException();
	public override int GetValues(object[] values) => throw new NotSupportedException();

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
		=> throw new NotSupportedException();

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
		=> throw new NotSupportedException();

	public override object this[int ordinal] => throw new NotSupportedException();
	public override object this[string name] => throw new NotSupportedException();

	public override int Depth => 0;
	public override bool HasRows => Count > 0;
	public override bool IsClosed => false;
	public override int RecordsAffected => -1;
	public override bool NextResult() => false;
	public override IEnumerator GetEnumerator() => throw new NotSupportedException();
}
