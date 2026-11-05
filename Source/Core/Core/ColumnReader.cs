namespace Open.Database.Extensions.Core;

/// <summary>
/// Reads one column as the requested type through the provider's typed getter when there is one, so a
/// value type is never boxed on the way out. <c>NULL</c> reads as <c>default</c>.
/// </summary>
internal static class ColumnReader
{
	public static T Get<T>(DbDataReader reader, int ordinal)
	{
		if (reader.IsDBNull(ordinal)) return default!;

		// Each check is a JIT-time constant for a given T, so only the matching branch survives and no value is boxed.
		if (typeof(T) == typeof(int)) return (T)(object)reader.GetInt32(ordinal);
		if (typeof(T) == typeof(string)) return (T)(object)reader.GetString(ordinal);
		if (typeof(T) == typeof(long)) return (T)(object)reader.GetInt64(ordinal);
		if (typeof(T) == typeof(bool)) return (T)(object)reader.GetBoolean(ordinal);
		if (typeof(T) == typeof(DateTime)) return (T)(object)reader.GetDateTime(ordinal);
		if (typeof(T) == typeof(Guid)) return (T)(object)reader.GetGuid(ordinal);
		if (typeof(T) == typeof(decimal)) return (T)(object)reader.GetDecimal(ordinal);
		if (typeof(T) == typeof(double)) return (T)(object)reader.GetDouble(ordinal);
		if (typeof(T) == typeof(short)) return (T)(object)reader.GetInt16(ordinal);
		if (typeof(T) == typeof(byte)) return (T)(object)reader.GetByte(ordinal);
		if (typeof(T) == typeof(float)) return (T)(object)reader.GetFloat(ordinal);
		if (typeof(T) == typeof(char)) return (T)(object)reader.GetChar(ordinal);

		return reader.GetFieldValue<T>(ordinal);
	}
}
