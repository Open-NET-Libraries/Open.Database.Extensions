namespace Open.Database.Extensions;

// Deliberately a DataException and not an InvalidOperationException: application code often catches that type as a business-rule failure and reports its message.
/// <summary>
/// The exception that is thrown when a column value cannot be mapped to the
/// requested .NET type: a value that does not convert, an overflow, and so on.
/// </summary>
/// <remarks>
/// <para>
/// The provider's or the conversion's error is the
/// <see cref="Exception.InnerException"/>.
/// </para>
/// <para>
/// A row that fails names the model and the row in the message. A NULL into a non-nullable member names the model and the row only;
/// to have it name the member and the column, declare <see cref="NullPolicyAttribute"/> with <see cref="NullAction.Throw"/>,
/// and to replace a NULL instead, declare <see cref="NullValueAttribute"/>.
/// A <see cref="NullAction.Throw"/> policy and a <see langword="required"/> member no column feeds set <see cref="PropertyName"/> and <see cref="ColumnName"/>.
/// </para>
/// <para>
/// This is a <see cref="DataException"/>.
/// </para>
/// </remarks>
/// <param name="message">The error message.</param>
/// <param name="propertyName">The name of the property being set.</param>
/// <param name="columnName">The name of the column being read.</param>
/// <param name="targetType">The .NET type the value was being read as.</param>
/// <param name="innerException">
/// The provider's or the conversion's error, if any.
/// </param>
[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors")]
public sealed class ValueMappingException(string message, string propertyName, string columnName, Type targetType, Exception? innerException = null) : DataException(message, innerException)
{
	/// <summary>
	/// The name of the property being set; empty when the failure names no member (see the remarks).
	/// </summary>
	/// <value>The name of the property that was being set. Empty when the failure names no member.</value>
	public string PropertyName { get; } = propertyName;

	/// <summary>
	/// The name of the column being read; empty when the failure names no column (see the remarks).
	/// </summary>
	/// <value>The name of the column that was being read. Empty when the failure names no column.</value>
	public string ColumnName { get; } = columnName;

	/// <summary>
	/// The .NET type the value was being read as.
	/// </summary>
	/// <value>The .NET type that the value was being read as.</value>
	public Type TargetType { get; } = targetType;
}
