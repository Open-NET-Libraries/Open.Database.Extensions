using System.Data.SqlTypes;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// Asserts the error of a NULL read into a non-nullable shape: a <see cref="ValueMappingException"/> that names the type and the row, and no member or column.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class NullAssert
{
	/// <summary>The message of a NULL into a non-nullable member of a model.</summary>
	public static string ModelMessage<TModel>(int row)
		=> $"A NULL was read into a non-nullable member of {Shape.TypeName(typeof(TModel))} at row {row}. Declare the member nullable (int?, string?).";

	/// <summary>The message of a NULL into a non-nullable single value.</summary>
	public static string ValueMessage<T>(int row)
		=> $"A NULL was read into a non-nullable {Shape.TypeName(typeof(T))} at row {row}. Declare it nullable ({Shape.TypeName(typeof(T))}?).";

	/// <summary>Asserts that reading a model throws the NULL error for <paramref name="row"/>, with the SqlNullValueException the provider (or the library) raised inside.</summary>
	public static ValueMappingException Model<TModel>(int row, Action read)
		=> Check<TModel, SqlNullValueException>(ModelMessage<TModel>(row), Assert.Throws<ValueMappingException>(read));

	/// <inheritdoc cref="Model{TModel}(int, Action)"/>
	public static async Task<ValueMappingException> ModelAsync<TModel>(int row, Func<Task> read)
		=> Check<TModel, SqlNullValueException>(ModelMessage<TModel>(row), await Assert.ThrowsAsync<ValueMappingException>(read));

	/// <summary>Asserts that reading a single value throws the NULL error for <paramref name="row"/>.</summary>
	public static ValueMappingException Value<T>(int row, Action read)
		=> Check<T, SqlNullValueException>(ValueMessage<T>(row), Assert.Throws<ValueMappingException>(read));

	/// <inheritdoc cref="Value{T}(int, Action)"/>
	public static async Task<ValueMappingException> ValueAsync<T>(int row, Func<Task> read)
		=> Check<T, SqlNullValueException>(ValueMessage<T>(row), await Assert.ThrowsAsync<ValueMappingException>(read));

	/// <summary>Asserts that reading a scalar (or a return value) throws the NULL error: the type, no row, no member, no column, nothing inside.</summary>
	public static ValueMappingException Scalar<T>(Action read)
		=> CheckScalar<T>(Assert.Throws<ValueMappingException>(read));

	/// <inheritdoc cref="Scalar{T}(Action)"/>
	public static async Task<ValueMappingException> ScalarAsync<T>(Func<Task> read)
		=> CheckScalar<T>(await Assert.ThrowsAsync<ValueMappingException>(read));

	static ValueMappingException CheckScalar<T>(ValueMappingException ex)
	{
		Assert.Equal($"A NULL was read into a non-nullable {Shape.TypeName(typeof(T))}. Declare it nullable ({Shape.TypeName(typeof(T))}?).", ex.Message);
		Assert.Equal(typeof(T), ex.TargetType);
		Assert.Equal("", ex.PropertyName);
		Assert.Equal("", ex.ColumnName);
		Assert.Null(ex.InnerException);
		return ex;
	}

	/// <summary>Asserts that a typed scalar query that returned no rows, read as a non-nullable value type, throws the no-rows error: the method, the type, no member, no column, nothing inside.</summary>
	public static ValueMappingException NoRows<T>(string method, Action read)
		=> CheckNoRows<T>(method, Assert.Throws<ValueMappingException>(read));

	/// <inheritdoc cref="NoRows{T}(string, Action)"/>
	public static async Task<ValueMappingException> NoRowsAsync<T>(string method, Func<Task> read)
		=> CheckNoRows<T>(method, await Assert.ThrowsAsync<ValueMappingException>(read));

	static ValueMappingException CheckNoRows<T>(string method, ValueMappingException ex)
	{
		string type = Shape.TypeName(typeof(T));
		Assert.Equal($"The query returned no rows; {method}<{type}> needs a value. Ask for {type}? and use ?? for a default.", ex.Message);
		Assert.Equal(typeof(T), ex.TargetType);
		Assert.Equal("", ex.PropertyName);
		Assert.Equal("", ex.ColumnName);
		Assert.Null(ex.InnerException);
		return ex;
	}

	/// <summary>Asserts what every row error has: the message, the type, no member, no column, and the inner exception's type.</summary>
	public static ValueMappingException Check<TTarget, TInner>(string message, ValueMappingException ex)
		where TInner : Exception
	{
		Assert.Equal(message, ex.Message);
		Assert.Equal(typeof(TTarget), ex.TargetType);
		Assert.Equal("", ex.PropertyName);
		Assert.Equal("", ex.ColumnName);
		Assert.IsType<TInner>(ex.InnerException);
		return ex;
	}
}
