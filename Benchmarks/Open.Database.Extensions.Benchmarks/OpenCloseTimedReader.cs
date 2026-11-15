using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace Open.Database.Extensions.Benchmarks;

/// <summary>What the timing wrapper stamps (see <see cref="TimedConnection"/>).</summary>
public enum TimedReaderMode
{
	/// <summary>The IN6 and S4 probes: the open and close of the reader, and two timestamps around every Read and every getter (the time inside the calls).</summary>
	PerCallStamps,

	/// <summary>The BenchmarkDotNet real-provider cases: only the open and close of the reader (the ReaderOpen column); Read and every getter are plain forwards, as in the Cut B build.</summary>
	ReaderOpenOnly,
}

/// <summary>
/// The reader of <see cref="TimedReaderMode.ReaderOpenOnly"/>: the clock starts on entry of ExecuteReader (sync or async) and stops when the reader is closed or disposed, exactly as
/// <see cref="TimedReader"/> does, and reports the interval to the stats and the recorder; every other call is one plain forward, so the number of wrapped calls per row cannot skew
/// a comparison between paths that read a row with different numbers of calls. Meant for the synchronous benchmarks: it keeps none of the async counters.
/// </summary>
public sealed class OpenCloseTimedReader(DbDataReader inner, In6Stats stats, long start) : DbDataReader
{
	bool _closed;

	void Stamp()
	{
		if (_closed) return;
		_closed = true;
		long ticks = Stopwatch.GetTimestamp() - start;
		stats.OpenTicks += ticks;
		stats.Readers++;
		stats.Recorder?.ReaderClosed(ticks, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	public override void Close()
	{
		inner.Close();
		Stamp();
	}

	public override async Task CloseAsync()
	{
		await inner.CloseAsync().ConfigureAwait(false);
		Stamp();
	}

	public override async ValueTask DisposeAsync()
	{
		await inner.DisposeAsync().ConfigureAwait(false);
		Stamp();
	}

	protected override void Dispose(bool disposing)
	{
		if (!disposing) return;
		inner.Dispose();
		Stamp();
	}

	public override bool Read() => inner.Read();
	public override Task<bool> ReadAsync(CancellationToken cancellationToken) => inner.ReadAsync(cancellationToken);
	public override bool NextResult() => inner.NextResult();
	public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => inner.NextResultAsync(cancellationToken);
	public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => inner.IsDBNullAsync(ordinal, cancellationToken);
	public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => inner.GetFieldValueAsync<T>(ordinal, cancellationToken);

	public override int Depth => inner.Depth;
	public override int FieldCount => inner.FieldCount;
	public override int VisibleFieldCount => inner.VisibleFieldCount;
	public override bool HasRows => inner.HasRows;
	public override bool IsClosed => inner.IsClosed;
	public override int RecordsAffected => inner.RecordsAffected;
	public override object this[int ordinal] => inner[ordinal];
	public override object this[string name] => inner[name];
	public override string GetName(int ordinal) => inner.GetName(ordinal);
	public override int GetOrdinal(string name) => inner.GetOrdinal(name);
	public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
	public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
	public override Type GetProviderSpecificFieldType(int ordinal) => inner.GetProviderSpecificFieldType(ordinal);
	public override object GetProviderSpecificValue(int ordinal) => inner.GetProviderSpecificValue(ordinal);
	public override int GetProviderSpecificValues(object[] values) => inner.GetProviderSpecificValues(values);
	public override DataTable? GetSchemaTable() => inner.GetSchemaTable();
	public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
	public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
	public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
	public override char GetChar(int ordinal) => inner.GetChar(ordinal);
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
	public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
	public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
	public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
	public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
	public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
	public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
	public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
	public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
	public override string GetString(int ordinal) => inner.GetString(ordinal);
	public override object GetValue(int ordinal) => inner.GetValue(ordinal);
	public override int GetValues(object[] values) => inner.GetValues(values);
	public override T GetFieldValue<T>(int ordinal) => inner.GetFieldValue<T>(ordinal);
	public override Stream GetStream(int ordinal) => inner.GetStream(ordinal);
	public override TextReader GetTextReader(int ordinal) => inner.GetTextReader(ordinal);
	public override IEnumerator GetEnumerator() => inner.GetEnumerator();
}
