using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// Builds stateful <see cref="IDataReader"/> / <see cref="DbDataReader"/> substitutes
/// backed by an in-memory column list + rows, so extension methods can be exercised
/// end-to-end. A C# <see langword="null"/> in a row cell is surfaced as
/// <see cref="DBNull.Value"/> to mirror real ADO.NET semantics.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class FakeReader
{
	public static IDataReader Create(IReadOnlyList<string> columns, params object?[][] rows)
	{
		IDataReader reader = Substitute.For<IDataReader>();
		Configure(reader, columns, rows, out _);
		return reader;
	}

	public static DbDataReader CreateDb(IReadOnlyList<string> columns, params object?[][] rows)
	{
		DbDataReader reader = Substitute.For<DbDataReader>();
		Configure(reader, columns, rows, out RowCursor cursor);

		// Async read shares the same cursor as the sync path.
		reader.ReadAsync(Arg.Any<CancellationToken>())
			.Returns(_ => Task.FromResult(cursor.MoveNext()));

		return reader;
	}

	sealed class RowCursor(object?[][] rows)
	{
		int _index = -1;
		public bool MoveNext() => ++_index < rows.Length;
		public object?[] Current => rows[_index];
	}

	static void Configure(IDataReader reader, IReadOnlyList<string> columns, object?[][] rows, out RowCursor cursor)
	{
		int fieldCount = columns.Count;
		var c = new RowCursor(rows);
		cursor = c;

		reader.FieldCount.Returns(fieldCount);

		for (int i = 0; i < fieldCount; i++)
		{
			int idx = i;
			reader.GetName(idx).Returns(columns[idx]);
			reader.GetDataTypeName(idx).Returns(columns[idx] + "_type");
		}

		// Mirrors IDataRecord.GetOrdinal: case-sensitive match first, then case-insensitive.
		reader.GetOrdinal(Arg.Any<string>()).Returns(ci => OrdinalOf(columns, ci.Arg<string>()));

		reader.Read().Returns(_ => c.MoveNext());

		reader.GetValue(Arg.Any<int>()).Returns(ci => AsDbValue(c.Current[ci.Arg<int>()]));
		reader[Arg.Any<int>()].Returns(ci => AsDbValue(c.Current[ci.Arg<int>()]));
		reader[Arg.Any<string>()].Returns(ci => AsDbValue(c.Current[OrdinalOf(columns, ci.Arg<string>())]));

		reader.When(x => x.GetValues(Arg.Any<object[]>())).Do(ci =>
		{
			object[] target = ci.Arg<object[]>();
			object?[] row = c.Current;
			int n = Math.Min(target.Length, row.Length);
			for (int i = 0; i < n; i++) target[i] = AsDbValue(row[i]);
		});
	}

	static int OrdinalOf(IReadOnlyList<string> columns, string name)
	{
		for (int i = 0; i < columns.Count; i++)
			if (string.Equals(columns[i], name, StringComparison.Ordinal)) return i;
		for (int i = 0; i < columns.Count; i++)
			if (string.Equals(columns[i], name, StringComparison.OrdinalIgnoreCase)) return i;
		throw new IndexOutOfRangeException(name);
	}

	static object AsDbValue(object? value) => value ?? DBNull.Value;
}
