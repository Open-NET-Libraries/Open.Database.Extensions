using System.Linq.Expressions;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for building a command and retrieving data using best practices.
/// </summary>
public static partial class CoreExtensions
{
	internal static IEnumerable<T> Concat<T>(T first, ICollection<T> remaining)
		=> (remaining == null || remaining.Count == 0) ? [first] : remaining.Prepend(first);

	// https://stackoverflow.com/questions/17660097/is-it-possible-to-speed-this-method-up/17669142#17669142
	internal static Action<T, object?> BuildUntypedSetter<T>(this PropertyInfo propertyInfo)
	{
		var targetType = propertyInfo.DeclaringType;
		var exTarget = Expression.Parameter(targetType ?? throw new InvalidOperationException(), "t");
		var exValue = Expression.Parameter(typeof(object), "p");
		var methodInfo = propertyInfo.GetSetMethod();
		var exBody = Expression.Call(exTarget, methodInfo!,
		   Expression.Convert(exValue, propertyInfo.PropertyType));
		var lambda = Expression.Lambda<Action<T, object?>>(exBody, exTarget, exValue);
		return lambda.Compile();
	}

	internal static object? DBNullValueToNull(object? value)
		=> value == DBNull.Value ? null : value;

	/// <summary>
	/// Copies the contents of the <paramref name="values"/> span
	/// to the <paramref name="target"/> span
	/// with any <see cref="DBNull"/> values converted to <see langword="null"/>.
	/// </summary>
	public static Span<object?> CopyToWithDbNullAsNull(this ReadOnlySpan<object?> values, Span<object?> target)
	{
		int len = values.Length;
		object?[] result = new object?[len];
		for (int i = 0; i < len; i++)
			target[i] = DBNullValueToNull(values[i]);

		return result;
	}

	/// <inheritdoc cref="CopyToWithDbNullAsNull(ReadOnlySpan{object?}, Span{object?})"/>
	public static Span<object?> CopyToWithDbNullAsNull(this Span<object?> values, Span<object?> target)
		=> CopyToWithDbNullAsNull((ReadOnlySpan<object?>)values, target);

	/// <summary>
	/// Any <see cref="DBNull"/> values are yielded as null.
	/// </summary>
	/// <param name="values">The source enumerable.</param>
	public static IEnumerable<object?> DBNullToNull(this IEnumerable<object?> values)
	{
		return DBNullToNullCore(values ?? throw new ArgumentNullException(nameof(values)));

		static IEnumerable<object?> DBNullToNullCore(IEnumerable<object?> values)
		{
			foreach (object? v in values)
				yield return DBNullValueToNull(v);
		}
	}

	/// <summary>
	/// Returns a copy of <paramref name="values"/> with any <see cref="DBNull"/> values converted to null.
	/// </summary>
	/// <param name="values">The source values.</param>
	public static object?[] DBNullToNullCopy(this object?[] values)
	{
		if (values is null) throw new ArgumentNullException(nameof(values));
		Contract.EndContractBlock();

		object?[] result = new object?[values.Length];
		CopyToWithDbNullAsNull(values.AsSpan(), result.AsSpan());
		return result;
	}

	/// <summary>
	/// Returns a copy of the contents of this span as an array with any <see cref="DBNull"/> values converted to null.
	/// </summary>
	/// <inheritdoc cref="DBNullToNullCopy(object[])"/>
	public static object?[] DBNullToNullCopy(this ReadOnlySpan<object?> values)
	{
		object?[] result = new object?[values.Length];
		DBNullToNullCopy(values, result.AsSpan());
		return result;
	}

	/// <inheritdoc cref="DBNullToNullCopy(ReadOnlySpan{object?})"/>
	public static object?[] DBNullToNullCopy(this Span<object?> values)
		=> DBNullToNullCopy((ReadOnlySpan<object?>)values);

	/// <summary>
	/// Replaces any <see cref="DBNull"/> in the <paramref name="values"/> with null;
	/// </summary>
	/// <param name="values">The source values.</param>
	public static Span<object?> ReplaceDBNullWithNull(this Span<object?> values)
		=> DBNullToNullCopy(values, values);

	/// <inheritdoc cref="ReplaceDBNullWithNull(Span{object?})"/>
	public static object?[] ReplaceDBNullWithNull(this object?[] values)
	{
		if (values is null) throw new ArgumentNullException(nameof(values));
		Contract.EndContractBlock();

		values.AsSpan().ReplaceDBNullWithNull();
		return values;
	}

	/// <summary>
	/// Generic enumerable extension for <see cref="DataColumnCollection"/>.
	/// </summary>
	/// <param name="columns">The column collection.</param>
	/// <returns>An enumerable of <see cref="DataColumn"/>s.</returns>
	public static IEnumerable<DataColumn> AsEnumerable(this DataColumnCollection columns)
	{
		return columns is null
			? throw new ArgumentNullException(nameof(columns))
			: AsEnumerableCore(columns);

		static IEnumerable<DataColumn> AsEnumerableCore(DataColumnCollection columns)
		{
			foreach (DataColumn c in columns)
				yield return c;
		}
	}

	/// <summary>
	/// Generic enumerable extension for <see cref="DataRowCollection"/>.
	/// </summary>
	/// <param name="rows">The row collection.</param>
	/// <returns>An enumerable of <see cref="DataRow"/>s.</returns>
	public static IEnumerable<DataRow> AsEnumerable(this DataRowCollection rows)
	{
		return rows is null
			? throw new ArgumentNullException(nameof(rows))
			: AsEnumerableCore(rows);

		static IEnumerable<DataRow> AsEnumerableCore(DataRowCollection rows)
		{
			foreach (DataRow r in rows)
				yield return r;
		}
	}

	/// <inheritdoc cref="To{T}(DataTable, IEnumerable{KeyValuePair{string, string?}}?, bool)"/>
	public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
		=> Transformer<T>
		.Create(fieldMappingOverrides)
		.Results(table, clearSourceTable);

	/// <inheritdoc cref="To{T}(DataTable, IEnumerable{KeyValuePair{string, string?}}?, bool)"/>
	public static IEnumerable<T> To<T>(this DataTable table, params (string Field, string? Column)[] fieldMappingOverrides) where T : new()
		=> Transformer<T>
		.Create(fieldMappingOverrides)
		.Results(table, false);

	/// <summary>
	/// Loads all data into a queue before iterating (dequeuing) the results as type <typeparamref name="T"/>.
	/// </summary>
	/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
	/// <param name="table">The <see cref="DataTable"/> to read from.</param>
	/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
	/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
	/// <returns>An enumerable used to iterate the results.</returns>
	public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
		=> To<T>(table, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), clearSourceTable);

	/// <summary>
	/// Useful extension for dequeuing items from a queue.
	/// Not thread safe but queueing/dequeuing items in between items is supported.
	/// </summary>
	/// <typeparam name="T">Return type of the source queue</typeparam>
	/// <returns>An enumerable of the items contained within the queue.</returns>
	public static IEnumerable<T> DequeueEach<T>(this Queue<T> source)
	{
		if (source is null) throw new ArgumentNullException(nameof(source));
		Contract.EndContractBlock();

#if NETSTANDARD2_0
		while (source.Count != 0)
			yield return source.Dequeue();
#else
		while (source.TryDequeue(out T? a))
			yield return a;
#endif
	}
}
