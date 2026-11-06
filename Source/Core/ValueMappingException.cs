namespace Open.Database.Extensions;

/// <summary>
/// A column value could not be mapped to the requested .NET type: a value that does not convert, an overflow, and so on.
/// </summary>
/// <remarks>
/// The provider's or the conversion's error is the <see cref="Exception.InnerException"/>.
/// This is a <see cref="DataException"/> and deliberately not an <see cref="InvalidOperationException"/>: application code often catches that type as a business-rule failure and reports its message.
/// </remarks>
/// <remarks>
/// Creates the exception.
/// </remarks>
/// <param name="message">The error message.</param>
/// <param name="propertyName">The name of the property being set.</param>
/// <param name="columnName">The name of the column being read.</param>
/// <param name="targetType">The .NET type the value was being read as.</param>
/// <param name="innerException">The provider's or the conversion's error, if any.</param>
[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors")]
public sealed class ValueMappingException(string message, string propertyName, string columnName, Type targetType, Exception? innerException = null) : DataException(message, innerException)
{
	/// <summary>
	/// The name of the property being set.
	/// </summary>
	public string PropertyName { get; } = propertyName;

	/// <summary>
	/// The name of the column being read.
	/// </summary>
	public string ColumnName { get; } = columnName;

	/// <summary>
	/// The .NET type the value was being read as.
	/// </summary>
	public Type TargetType { get; } = targetType;
}
