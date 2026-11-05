using System.Collections;
using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// A minimal in-memory <see cref="DbDataReader"/> that, unlike the NSubstitute fakes, reports real column
/// types and serves values through the typed getters (<c>GetInt32</c>, <c>GetString</c>, …) the way a
/// provider does. That is what lets the generated row mappers take their typed (unboxed) path.
/// </summary>
[ExcludeFromCodeCoverage]
internal class TypedReader(IReadOnlyList<(string Name, Type Type)> columns, object?[][] rows) : DbDataReader
{
	/// <summary>How many times <see cref="Read"/> has returned a row.</summary>
	public int RowsRead { get; private set; }

	int _row = -1;

	public static TypedReader Create((string Name, Type Type)[] columns, params object?[][] rows) => new(columns, rows);

	object? Cell(int ordinal) => rows[_row][ordinal];

	public override bool Read()
	{
		if (++_row >= rows.Length) return false;
		RowsRead++;
		return true;
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return Task.FromResult(Read());
	}

	public override int FieldCount => columns.Count;
	public override string GetName(int ordinal) => columns[ordinal].Name;
	public override Type GetFieldType(int ordinal) => columns[ordinal].Type;
	public override string GetDataTypeName(int ordinal) => columns[ordinal].Type.Name;
	public override bool IsDBNull(int ordinal) => Cell(ordinal) is null or DBNull;
	public override object GetValue(int ordinal) => Cell(ordinal) ?? DBNull.Value;

	public override int GetOrdinal(string name)
	{
		for (int i = 0; i < columns.Count; i++)
			if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
		throw new IndexOutOfRangeException(name);
	}

	// The typed getters cast directly (like a provider), so a NULL cell raises InvalidCastException/NullReferenceException.
	public override bool GetBoolean(int ordinal) => (bool)Cell(ordinal)!;
	public override byte GetByte(int ordinal) => (byte)Cell(ordinal)!;
	public override char GetChar(int ordinal) => (char)Cell(ordinal)!;
	public override short GetInt16(int ordinal) => (short)Cell(ordinal)!;
	public override int GetInt32(int ordinal) => (int)Cell(ordinal)!;
	public override long GetInt64(int ordinal) => (long)Cell(ordinal)!;
	public override float GetFloat(int ordinal) => (float)Cell(ordinal)!;
	public override double GetDouble(int ordinal) => (double)Cell(ordinal)!;
	public override decimal GetDecimal(int ordinal) => (decimal)Cell(ordinal)!;
	public override DateTime GetDateTime(int ordinal) => (DateTime)Cell(ordinal)!;
	public override Guid GetGuid(int ordinal) => (Guid)Cell(ordinal)!;
	public override string GetString(int ordinal) => (string)Cell(ordinal)!;

	public override int GetValues(object[] values)
	{
		int n = Math.Min(values.Length, FieldCount);
		for (int i = 0; i < n; i++) values[i] = GetValue(i);
		return n;
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();

	public override object this[int ordinal] => GetValue(ordinal);
	public override object this[string name] => GetValue(GetOrdinal(name));

	public override int Depth => 0;
	public override bool HasRows => rows.Length > 0;
	public override bool IsClosed => false;
	public override int RecordsAffected => -1;
	public override bool NextResult() => false;
	public override IEnumerator GetEnumerator() => throw new NotSupportedException();
}
