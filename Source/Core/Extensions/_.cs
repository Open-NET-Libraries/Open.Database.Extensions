namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for building a command and retrieving data using best practices.
/// </summary>
public static partial class CoreExtensions
{

	internal static ValueTask<bool> CancelledAsFalse(this ValueTask<bool> task)
	{
		if (task.IsCompletedSuccessfully)
			return task;

		return CancelledAsFalseCore(task);

		static async ValueTask<bool> CancelledAsFalseCore(ValueTask<bool> task)
		{
			try
			{
				return await task.ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				return false;
			}
		}
	}

	internal static async ValueTask<bool> CancelledAsFalse(this Task<bool> task)
	{
		try
		{
			return await task.ConfigureAwait(false);
		}
		catch (OperationCanceledException)
		{
			return false;
		}
	}

	internal static object? DBNullValueToNull(object? value)
		=> value == DBNull.Value ? null : value;

	/// <summary>
	/// Copies the contents of the <paramref name="values"/> span
	/// to the <paramref name="target"/> span
	/// with any <see cref="DBNull"/> values converted to <see langword="null"/>.
	/// </summary>
	/// <param name="values">The source values.</param>
	/// <param name="target">The span that receives the values.</param>
	/// <returns>The <paramref name="target"/> span.</returns>
	/// <exception cref="IndexOutOfRangeException">
	/// <paramref name="target"/> is shorter than <paramref name="values"/>.
	/// </exception>
	public static Span<object?> CopyToDBNullAsNull(this ReadOnlySpan<object?> values, Span<object?> target)
	{
		int len = values.Length;
		for (int i = 0; i < len; i++)
			target[i] = DBNullValueToNull(values[i]);

		return target;
	}

	/// <inheritdoc cref="CopyToDBNullAsNull(ReadOnlySpan{object?}, Span{object?})"/>
	public static Span<object?> CopyToDBNullAsNull(this Span<object?> values, Span<object?> target)
		=> CopyToDBNullAsNull((ReadOnlySpan<object?>)values, target);

	/// <inheritdoc cref="CopyToDBNullAsNull(ReadOnlySpan{object?}, Span{object?})"/>
	public static Span<object?> CopyToDBNullAsNull(this ReadOnlySpan<object?> values, Memory<object?> target)
		=> CopyToDBNullAsNull(values, target.Span);

	/// <inheritdoc cref="CopyToDBNullAsNull(ReadOnlySpan{object?}, Span{object?})"/>
	public static Span<object?> CopyToDBNullAsNull(this Span<object?> values, Memory<object?> target)
		=> CopyToDBNullAsNull((ReadOnlySpan<object?>)values, target.Span);

	/// <summary>
	/// Yields each value of an enumerable, with any <see cref="DBNull"/> converted to <see langword="null"/>.
	/// </summary>
	/// <param name="values">The source enumerable.</param>
	/// <returns>An enumerable of the converted values.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="values"/> is <see langword="null"/>.
	/// </exception>
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
	/// Returns a copy of <paramref name="values"/> with any <see cref="DBNull"/> values converted to <see langword="null"/>.
	/// </summary>
	/// <param name="values">The source values.</param>
	/// <returns>A new array of the converted values.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="values"/> is <see langword="null"/>.
	/// </exception>
	public static object?[] DBNullToNullCopy(this object?[] values)
	{
		if (values is null) throw new ArgumentNullException(nameof(values));
		Contract.EndContractBlock();

		object?[] result = new object?[values.Length];
		CopyToDBNullAsNull(values.AsSpan(), result.AsSpan());
		return result;
	}

	/// <summary>
	/// Returns a copy of the contents of this span as an array with any <see cref="DBNull"/> values converted to <see langword="null"/>.
	/// </summary>
	/// <param name="values">The source values.</param>
	/// <returns>A new array of the converted values.</returns>
	public static object?[] DBNullToNullCopy(this ReadOnlySpan<object?> values)
	{
		object?[] result = new object?[values.Length];
		CopyToDBNullAsNull(values, result.AsSpan());
		return result;
	}

	/// <inheritdoc cref="DBNullToNullCopy(ReadOnlySpan{object?})"/>
	public static object?[] DBNullToNullCopy(this Span<object?> values)
		=> DBNullToNullCopy((ReadOnlySpan<object?>)values);

	/// <summary>
	/// Replaces any <see cref="DBNull"/> in the <paramref name="values"/> with <see langword="null"/>.
	/// </summary>
	/// <param name="values">The values to update in place.</param>
	/// <returns>The <paramref name="values"/>, updated in place.</returns>
	public static Span<object?> ReplaceDBNullWithNull(this Span<object?> values)
		=> CopyToDBNullAsNull(values, values);

	/// <inheritdoc cref="ReplaceDBNullWithNull(Span{object?})"/>
	public static Memory<object?> ReplaceDBNullWithNull(this Memory<object?> values)
	{
		CopyToDBNullAsNull(values.Span, values.Span);
		return values;
	}

	/// <inheritdoc cref="ReplaceDBNullWithNull(Span{object?})"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="values"/> is <see langword="null"/>.
	/// </exception>
	public static object?[] ReplaceDBNullWithNull(this object?[] values)
	{
		if (values is null) throw new ArgumentNullException(nameof(values));
		Contract.EndContractBlock();

		values.AsSpan().ReplaceDBNullWithNull();
		return values;
	}

	/// <inheritdoc cref="ReplaceDBNullWithNull(Span{object?})"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="values"/> is <see langword="null"/>.
	/// </exception>
	public static List<object?> ReplaceDBNullWithNull(this List<object?> values)
	{
		if (values is null) throw new ArgumentNullException(nameof(values));
		Contract.EndContractBlock();

#if NET10_0_OR_GREATER
		var span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(values);
		for (int i = 0; i < span.Length; i++)
		{
			ref object? item = ref span[i];
			if (item == DBNull.Value) item = null;
		}
#else
		int count = values.Count;
		for (int i = 0; i < count; i++)
		{
			if (values[i] == DBNull.Value) values[i] = null;
		}
#endif

		return values;
	}

	/// <summary>
	/// Enumerates a <see cref="DataColumnCollection"/> as a generic <see cref="IEnumerable{T}"/>.
	/// </summary>
	/// <param name="columns">The column collection.</param>
	/// <returns>An enumerable of <see cref="DataColumn"/>s.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="columns"/> is <see langword="null"/>.
	/// </exception>
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
	/// Enumerates a <see cref="DataRowCollection"/> as a generic <see cref="IEnumerable{T}"/>.
	/// </summary>
	/// <param name="rows">The row collection.</param>
	/// <returns>An enumerable of <see cref="DataRow"/>s.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="rows"/> is <see langword="null"/>.
	/// </exception>
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
}
