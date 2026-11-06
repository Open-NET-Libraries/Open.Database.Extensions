using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Open.Database.Extensions.Benchmarks;

// A tiny in-memory ADO.NET provider. It reports correctly typed columns (an int column is an int, a nullable
// column reports its underlying type) and serves typed values without boxing, like a real provider does.
// Both libraries run through the same provider, so every measured difference is the library's own cost.

public sealed class BenchTable
{
	public required string[] Names { get; init; }
	public required Type[] Types { get; init; }
	public required Array[] Columns { get; init; }
	public required bool[]?[] Nulls { get; init; }
	public required int Rows { get; init; }

	readonly Dictionary<string, int> _ordinals = new(StringComparer.OrdinalIgnoreCase);

	public int Ordinal(string name)
	{
		if (_ordinals.Count == 0)
			for (int i = 0; i < Names.Length; i++) _ordinals[Names[i]] = i;
		return _ordinals.TryGetValue(name, out int o) ? o : throw new IndexOutOfRangeException(name);
	}

	/// <summary>The same rows under other column names.</summary>
	public BenchTable WithNames(string[] names) => new() { Names = names, Types = Types, Columns = Columns, Nulls = Nulls, Rows = Rows };

	public static BenchTable Posts(int rows)
	{
		var id = new int[rows]; var title = new string[rows]; var body = new string[rows]; var created = new DateTime[rows];
		var price = new decimal[rows]; var published = new bool[rows]; var views = new int[rows]; var score = new double[rows];
		var key = new Guid[rows]; var rank = new short[rows]; var big = new long[rows]; var tag = new string[rows];
		var bodyNull = new bool[rows]; var viewsNull = new bool[rows]; var tagNull = new bool[rows];
		var start = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		for (int i = 0; i < rows; i++)
		{
			id[i] = i + 1; title[i] = "Post number " + i; body[i] = "Lorem ipsum dolor sit amet " + i; created[i] = start.AddMinutes(i);
			price[i] = 10m + i; published[i] = i % 2 == 0; views[i] = i * 3; score[i] = i * 1.5; key[i] = new Guid(i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
			rank[i] = (short)(i % 100); big[i] = (long)i << 20; tag[i] = "t" + (i % 7);
			bodyNull[i] = i % 3 == 0; viewsNull[i] = i % 4 == 0; tagNull[i] = i % 2 == 0;
		}

		return new BenchTable
		{
			Rows = rows,
			Names = ["Id", "Title", "Body", "Created", "Price", "IsPublished", "ViewCount", "Score", "Key", "Rank", "Big", "Tag"],
			Types = [typeof(int), typeof(string), typeof(string), typeof(DateTime), typeof(decimal), typeof(bool), typeof(int), typeof(double), typeof(Guid), typeof(short), typeof(long), typeof(string)],
			Columns = [id, title, body, created, price, published, views, score, key, rank, big, tag],
			Nulls = [null, null, bodyNull, null, null, null, viewsNull, null, null, null, null, tagNull],
		};
	}
}

public sealed class BenchConnection(BenchTable table) : DbConnection
{
	public BenchTable Table { get; } = table;

	[AllowNull]
	public override string ConnectionString { get; set; } = "bench";
	public override string Database => "bench";
	public override string DataSource => "bench";
	public override string ServerVersion => "1.0";
	public override ConnectionState State => ConnectionState.Open;
	public override void ChangeDatabase(string databaseName) { }
	public override void Close() { }
	public override void Open() { }
	protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
	protected override DbCommand CreateDbCommand() => new BenchCommand(this);
}

public sealed class BenchCommand(BenchConnection connection) : DbCommand
{
	static readonly object BoxedScalar = 42;
	BenchParameterCollection? _parameters;

	[AllowNull]
	public override string CommandText { get; set; } = "";
	public override int CommandTimeout { get; set; } = 30;
	public override CommandType CommandType { get; set; }
	public override bool DesignTimeVisible { get; set; }
	public override UpdateRowSource UpdatedRowSource { get; set; }
	protected override DbConnection? DbConnection { get => connection; set { } }
	protected override DbParameterCollection DbParameterCollection => _parameters ??= [];
	protected override DbTransaction? DbTransaction { get; set; }
	public override void Cancel() { }
	public override int ExecuteNonQuery() => 1;
	public override object? ExecuteScalar() => BoxedScalar;
	public override void Prepare() { }
	protected override DbParameter CreateDbParameter() => new BenchParameter();
	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new BenchReader(connection.Table);
}

public sealed class BenchParameter : DbParameter
{
	public override DbType DbType { get; set; }
	public override ParameterDirection Direction { get; set; }
	public override bool IsNullable { get; set; }
	[AllowNull]
	public override string ParameterName { get; set; } = "";
	public override int Size { get; set; }
	[AllowNull]
	public override string SourceColumn { get; set; } = "";
	public override bool SourceColumnNullMapping { get; set; }
	public override object? Value { get; set; }
	public override void ResetDbType() { }
}

public sealed class BenchParameterCollection : DbParameterCollection, IEnumerable<DbParameter>
{
	readonly List<DbParameter> _list = [];
	public override int Count => _list.Count;
	public override object SyncRoot => _list;
	public void Add(DbParameter value) => _list.Add(value);
	public override int Add(object value) { _list.Add((DbParameter)value); return _list.Count - 1; }
	public override void AddRange(Array values) { foreach (DbParameter p in values) _list.Add(p); }
	public override void Clear() => _list.Clear();
	public override bool Contains(object value) => _list.Contains((DbParameter)value);
	public override bool Contains(string value) => IndexOf(value) >= 0;
	public override void CopyTo(Array array, int index) => ((ICollection)_list).CopyTo(array, index);
	public override IEnumerator GetEnumerator() => _list.GetEnumerator();
	IEnumerator<DbParameter> IEnumerable<DbParameter>.GetEnumerator() => _list.GetEnumerator();
	public override int IndexOf(object value) => _list.IndexOf((DbParameter)value);
	public override int IndexOf(string parameterName) => _list.FindIndex(p => p.ParameterName == parameterName);
	public override void Insert(int index, object value) => _list.Insert(index, (DbParameter)value);
	public override void Remove(object value) => _list.Remove((DbParameter)value);
	public override void RemoveAt(int index) => _list.RemoveAt(index);
	public override void RemoveAt(string parameterName) => _list.RemoveAt(IndexOf(parameterName));
	protected override DbParameter GetParameter(int index) => _list[index];
	protected override DbParameter GetParameter(string parameterName) => _list[IndexOf(parameterName)];
	protected override void SetParameter(int index, DbParameter value) => _list[index] = value;
	protected override void SetParameter(string parameterName, DbParameter value) => _list[IndexOf(parameterName)] = value;
}

public sealed class BenchReader(BenchTable table) : DbDataReader
{
	int _row = -1;
	bool _closed;

	public override int Depth => 0;
	public override int FieldCount => table.Names.Length;
	public override bool HasRows => table.Rows > 0;
	public override bool IsClosed => _closed;
	public override int RecordsAffected => -1;
	public override object this[int ordinal] => GetValue(ordinal);
	public override object this[string name] => GetValue(table.Ordinal(name));

	public override bool Read() => ++_row < table.Rows;
	public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Read() ? TrueTask : FalseTask;
	static readonly Task<bool> TrueTask = Task.FromResult(true), FalseTask = Task.FromResult(false);
	public override bool NextResult() => false;
	public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => FalseTask;
	public override void Close() => _closed = true;

	public override string GetName(int ordinal) => table.Names[ordinal];
	public override int GetOrdinal(string name) => table.Ordinal(name);
	public override Type GetFieldType(int ordinal) => table.Types[ordinal];
	public override string GetDataTypeName(int ordinal) => table.Types[ordinal].Name;
	public override bool IsDBNull(int ordinal) => table.Nulls[ordinal] is { } n && n[_row];
	public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => IsDBNull(ordinal) ? TrueTask : FalseTask;

	public override bool GetBoolean(int ordinal) => ((bool[])table.Columns[ordinal])[_row];
	public override DateTime GetDateTime(int ordinal) => ((DateTime[])table.Columns[ordinal])[_row];
	public override decimal GetDecimal(int ordinal) => ((decimal[])table.Columns[ordinal])[_row];
	public override double GetDouble(int ordinal) => ((double[])table.Columns[ordinal])[_row];
	public override Guid GetGuid(int ordinal) => ((Guid[])table.Columns[ordinal])[_row];
	public override short GetInt16(int ordinal) => ((short[])table.Columns[ordinal])[_row];
	public override int GetInt32(int ordinal) => ((int[])table.Columns[ordinal])[_row];
	public override long GetInt64(int ordinal) => ((long[])table.Columns[ordinal])[_row];
	public override string GetString(int ordinal) => ((string[])table.Columns[ordinal])[_row];

	public override object GetValue(int ordinal)
	{
		if (IsDBNull(ordinal)) return DBNull.Value;
		return table.Columns[ordinal] switch
		{
			int[] a => a[_row],
			string[] a => a[_row],
			DateTime[] a => a[_row],
			decimal[] a => a[_row],
			bool[] a => a[_row],
			double[] a => a[_row],
			Guid[] a => a[_row],
			short[] a => a[_row],
			long[] a => a[_row],
			_ => throw new NotSupportedException(),
		};
	}

	public override int GetValues(object[] values)
	{
		int n = Math.Min(values.Length, FieldCount);
		for (int i = 0; i < n; i++) values[i] = GetValue(i);
		return n;
	}

	public override byte GetByte(int ordinal) => throw new NotSupportedException();
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
	public override char GetChar(int ordinal) => throw new NotSupportedException();
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
	public override float GetFloat(int ordinal) => throw new NotSupportedException();
	public override IEnumerator GetEnumerator() => throw new NotSupportedException();
}
