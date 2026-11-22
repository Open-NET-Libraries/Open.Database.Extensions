using System.Diagnostics;

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
