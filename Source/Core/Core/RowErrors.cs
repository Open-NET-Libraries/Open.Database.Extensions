using System.Data.SqlTypes;
using System.Globalization;

namespace Open.Database.Extensions;

// The message depends only on the type of the error under it: no member and no column is named, so nothing is tracked per cell
// and the reader is never touched after a failure.
// SqlNullValueException is the BCL type Microsoft.Data.SqlClient throws from a typed getter on NULL, and the one the library throws when it sees the NULL itself (RowMap.ConvertRequired),
// so Core needs no provider reference to recognise it.
/// <summary>
/// Makes the <see cref="ValueMappingException"/> a row mapper throws: it names the type being made and the row.
/// </summary>
static class RowErrors
{
	/// <summary>
	/// Creates the exception for a failure mapping a row whose number is not known (a reader whose caller owns the read loop).
	/// </summary>
	/// <param name="inner">The error the read or the conversion raised.</param>
	/// <param name="target">The model type, or the type of the single value.</param>
	/// <param name="singleValue"><see langword="true"/> for a single value; otherwise, <see langword="false"/> for a model.</param>
	/// <returns>The exception to throw.</returns>
	public static ValueMappingException Create(Exception inner, Type target, bool singleValue)
		=> Create(inner, target, singleValue, -1);

	/// <summary>
	/// Creates the exception for a failure mapping a row.
	/// </summary>
	/// <param name="inner">The error the read or the conversion raised.</param>
	/// <param name="target">The model type, or the type of the single value.</param>
	/// <param name="singleValue"><see langword="true"/> for a single value; otherwise, <see langword="false"/> for a model.</param>
	/// <param name="row">The 0-based row in the result set, or -1 when it is not known.</param>
	/// <returns>The exception to throw.</returns>
	public static ValueMappingException Create(Exception inner, Type target, bool singleValue, int row)
	{
		if (inner is NullPolicyViolation violation) return violation.Create(target, row);

		string type = Shape.TypeName(target);
		string at = row < 0 ? "" : " at row " + row.ToString(CultureInfo.InvariantCulture);
		string which = row < 0 ? "a row" : "row " + row.ToString(CultureInfo.InvariantCulture);

		string message = inner switch
		{
			SqlNullValueException when singleValue
				=> $"A NULL was read into a non-nullable {type}{at}. Declare it nullable ({type}?).",
			SqlNullValueException
				=> $"A NULL was read into a non-nullable member of {type}{at}. Declare the member nullable (int?, string?).",
			InvalidCastException when singleValue
				=> $"Unable to map {which} to '{type}': the value is NULL, or its type does not convert to {type}. Declare it nullable ({type}?), or read the column as its own type.",
			InvalidCastException
				=> $"Unable to map {which} to '{type}': a NULL was read into a non-nullable member, or a column's type does not convert to its member's. Declare the member nullable (int?, string?), or match the member's type to the column's.",
			_ => $"Unable to map {which} to '{type}'.",
		};

		return new ValueMappingException(message, "", "", target, inner);
	}

	/// <summary>
	/// Returns the message for a NULL scalar read as a non-nullable value type.
	/// </summary>
	/// <param name="target">The type the scalar was read as.</param>
	/// <returns>The message.</returns>
	public static string NullScalar(Type target)
	{
		string type = Shape.TypeName(target);
		return $"A NULL was read into a non-nullable {type}. Declare it nullable ({type}?).";
	}

	/// <summary>
	/// Returns the message for a scalar query that returned no rows, read as a non-nullable value type.
	/// </summary>
	/// <param name="target">The type the scalar was read as.</param>
	/// <param name="method">The name of the method the caller used.</param>
	/// <returns>The message.</returns>
	public static string NoRowsScalar(Type target, string method)
	{
		string type = Shape.TypeName(target);
		return $"The query returned no rows; {method}<{type}> needs a value. Ask for {type}? and use ?? for a default.";
	}
}

// Internal and never seen by a caller: the row reader's guard hands it to RowErrors.Create, which turns it into the ValueMappingException.
/// <summary>
/// Reports a NULL in a member whose null policy is <see cref="NullAction.Throw"/>; thrown by the compiled row readers before the value is read.
/// </summary>
/// <param name="member">The name of the member.</param>
/// <param name="column">The name of the column.</param>
/// <param name="memberType">The type of the member.</param>
[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Internal wrapper: only the row readers create it.")]
[SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "Internal wrapper: the row reader's guard turns it into a ValueMappingException.")]
[SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "Internal wrapper: only the row readers create it.")]
sealed class NullPolicyViolation(string member, string column, Type memberType) : Exception
{
	/// <summary>
	/// Returns the exception a caller sees: the member, the column and the row, with no inner exception.
	/// </summary>
	/// <param name="model">The model type.</param>
	/// <param name="row">The 0-based row in the result set, or -1 when it is not known.</param>
	/// <returns>The exception to throw.</returns>
	public ValueMappingException Create(Type model, int row)
	{
		string at = row < 0 ? "" : " at row " + row.ToString(CultureInfo.InvariantCulture);
		return new ValueMappingException(
			$"A NULL was read from column '{column}' into '{member}' of {Shape.TypeName(model)}{at}, whose null policy is Throw.",
			member,
			column,
			memberType);
	}
}

// Internal and never seen by a caller: every read loop that calls a row reader catches it (RowReader) and throws At(row).
/// <summary>
/// Carries the failure of a compiled row reader, which does not know the row, to the read loop, which does.
/// </summary>
/// <param name="inner">The error the read or the conversion raised.</param>
/// <param name="target">The model type, or the type of the single value.</param>
/// <param name="singleValue"><see langword="true"/> for a single value; otherwise, <see langword="false"/> for a model.</param>
[SuppressMessage("Roslynator", "RCS1194:Implement exception constructors", Justification = "Internal wrapper: only the row readers create it.")]
[SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "Internal wrapper: every read loop catches it, so it never reaches a caller.")]
[SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "Internal wrapper: only the row readers create it.")]
sealed class RowMappingFailure(Exception inner, Type target, bool singleValue) : Exception(null, inner)
{
	/// <summary>
	/// Creates the wrapper; called by the compiled row readers.
	/// </summary>
	/// <param name="inner">The error the read or the conversion raised.</param>
	/// <param name="target">The model type, or the type of the single value.</param>
	/// <param name="singleValue"><see langword="true"/> for a single value; otherwise, <see langword="false"/> for a model.</param>
	/// <returns>The wrapper to throw.</returns>
	public static RowMappingFailure Defer(Exception inner, Type target, bool singleValue)
		=> new(inner, target, singleValue);

	/// <summary>
	/// Returns the exception the read loop throws, naming the row.
	/// </summary>
	/// <param name="row">The 0-based row in the result set.</param>
	/// <returns>The exception to throw.</returns>
	public ValueMappingException At(int row)
		=> RowErrors.Create(InnerException!, target, singleValue, row);
}
