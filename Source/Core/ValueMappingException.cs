namespace Open.Database.Extensions;

/// <summary>
/// A column value could not be mapped to the requested .NET type: a <c>NULL</c> for a non-nullable value type, a value that does not convert, and so on.
/// </summary>
/// <remarks>
/// The provider's or the conversion's error is the <see cref="Exception.InnerException"/>.
/// This is a <see cref="DataException"/> and deliberately not an <see cref="InvalidOperationException"/>: application code often catches that type as a business-rule failure and reports its message.
/// </remarks>
public sealed class ValueMappingException : DataException
{
	/// <summary>
	/// Creates the exception.
	/// </summary>
	/// <param name="message">The error message.</param>
	/// <param name="propertyName">The name of the property being set, or <see langword="null"/> for a scalar result.</param>
	/// <param name="columnName">The name of the column being read, or <see langword="null"/> for a scalar result.</param>
	/// <param name="targetType">The .NET type the value was being read as.</param>
	/// <param name="innerException">The provider's or the conversion's error, if any.</param>
	public ValueMappingException(string message, string? propertyName, string? columnName, Type targetType, Exception? innerException = null)
		: base(message, innerException)
	{
		PropertyName = propertyName;
		ColumnName = columnName;
		TargetType = targetType;
	}

	/// <summary>
	/// The name of the property being set, or <see langword="null"/> for a scalar result.
	/// </summary>
	public string? PropertyName { get; }

	/// <summary>
	/// The name of the column being read, or <see langword="null"/> for a scalar result.
	/// </summary>
	public string? ColumnName { get; }

	/// <summary>
	/// The .NET type the value was being read as.
	/// </summary>
	public Type TargetType { get; }
}
