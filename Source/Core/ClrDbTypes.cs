namespace Open.Database.Extensions;

/// <summary>
/// The database type a CLR type is sent as, for a <c>NULL</c> parameter whose
/// value cannot tell the provider (a <see langword="null"/> carries no type).
/// </summary>
/// <remarks>
/// The same choices Dapper makes.
/// <see langword="null"/> means no choice: the provider infers as it always
/// did.
/// </remarks>
static class ClrDbTypes
{
	/// <summary>Gets the database type a value of a CLR type is sent as.</summary>
	/// <remarks>
	/// A <see cref="Nullable{T}"/> is treated as its underlying type, and an
	/// enum as its underlying integral type.
	/// </remarks>
	/// <param name="type">The CLR type of the parameter's value.</param>
	/// <returns>
	/// The database type, or <see langword="null"/> if there is no choice and
	/// the provider should infer it.
	/// </returns>
	internal static DbType? From(Type type)
	{
		type = Nullable.GetUnderlyingType(type) ?? type;
		if (type.IsEnum) type = Enum.GetUnderlyingType(type);

		return Type.GetTypeCode(type) switch
		{
			TypeCode.Boolean => DbType.Boolean,
			TypeCode.Byte => DbType.Byte,
			TypeCode.SByte => DbType.SByte,
			TypeCode.Int16 => DbType.Int16,
			TypeCode.UInt16 => DbType.UInt16,
			TypeCode.Int32 => DbType.Int32,
			TypeCode.UInt32 => DbType.UInt32,
			TypeCode.Int64 => DbType.Int64,
			TypeCode.UInt64 => DbType.UInt64,
			TypeCode.Single => DbType.Single,
			TypeCode.Double => DbType.Double,
			TypeCode.Decimal => DbType.Decimal,
			TypeCode.DateTime => DbType.DateTime,
			TypeCode.Char => DbType.StringFixedLength,
			TypeCode.String => DbType.String,
			_ when type == typeof(Guid) => DbType.Guid,
			_ when type == typeof(DateTimeOffset) => DbType.DateTimeOffset,
			_ when type == typeof(TimeSpan) => DbType.Time,
			_ when type == typeof(byte[]) => DbType.Binary,
			_ => null,
		};
	}
}
