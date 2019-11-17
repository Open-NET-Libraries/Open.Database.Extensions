using Open.Database.Extensions.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Core non-DB-specific extensions for building a command and retrieving data using best practices.
	/// </summary>
	public static partial class Extensions
	{
		// https://stackoverflow.com/questions/17660097/is-it-possible-to-speed-this-method-up/17669142#17669142
		internal static Action<T, object?> BuildUntypedSetter<T>(this PropertyInfo propertyInfo)
		{
			var targetType = propertyInfo.DeclaringType;
			var exTarget = Expression.Parameter(targetType ?? throw new InvalidOperationException(), "t");
			var exValue = Expression.Parameter(typeof(object), "p");
			var methodInfo = propertyInfo.GetSetMethod();
			var exBody = Expression.Call(exTarget, methodInfo,
			   Expression.Convert(exValue, propertyInfo.PropertyType));
			var lambda = Expression.Lambda<Action<T, object?>>(exBody, exTarget, exValue);
			var action = lambda.Compile();
			return action;
		}

		internal static object? DBNullValueToNull(object? value)
			=> value == DBNull.Value ? null : value;

		/// <summary>
		/// Any DBNull values are converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>The converted enumerable.</returns>
		public static IEnumerable<object?> DBNullToNull(this IEnumerable<object?> values)
		{
			if (values is null) throw new ArgumentNullException(nameof(values));
			Contract.EndContractBlock();

			foreach (var v in values)
				yield return DBNullValueToNull(v);
		}

		/// <summary>
		/// Returns a copy of this array with any DBNull values converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>A new array containing the results with.</returns>
		public static object?[] DBNullToNull(this object?[] values)
		{
			if (values is null) throw new ArgumentNullException(nameof(values));
			Contract.EndContractBlock();

			var len = values.Length;
			var result = new object?[len];
			for (var i = 0; i < len; i++)
				result[i] = DBNullValueToNull(values[i]);
			return result;
		}

		/// <summary>
		/// Returns a copy of this array with any DBNull values converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>A new array containing the results with.</returns>
		public static object?[] DBNullToNull(this in ReadOnlySpan<object?> values)
		{
			var len = values.Length;
			var result = new object?[len];
			for (var i = 0; i < len; i++)
				result[i] = DBNullValueToNull(values[i]);
			return result;
		}

		/// <summary>
		/// Returns a copy of this array with any DBNull values converted to null.
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>A new array containing the results with.</returns>
		public static object?[] DBNullToNull(this in Span<object?> values)
		{
			var len = values.Length;
			var result = new object?[len];
			for (var i = 0; i < len; i++)
				result[i] = DBNullValueToNull(values[i]);
			return result;
		}

		/// <summary>
		/// Replaces any DBNull values in the array with null;
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>The converted enumerable.</returns>
		public static Span<object?> ReplaceDBNullWithNull(this Span<object?> values)
		{
			var len = values.Length;
			for (var i = 0; i < len; i++)
			{
				ref var value = ref values[i];
				if (value == DBNull.Value) value = null;
			}
			return values;
		}

		/// <summary>
		/// Replaces any DBNull values in the array with null;
		/// </summary>
		/// <param name="values">The source values.</param>
		/// <returns>The converted enumerable.</returns>
		public static object?[] ReplaceDBNullWithNull(this object?[] values)
		{
			if (values is null)	throw new ArgumentNullException(nameof(values));
			Contract.EndContractBlock();

			values.AsSpan().ReplaceDBNullWithNull();

			return values;
		}

		/// <summary>
		/// Generic enumerable extension for DataColumnCollection.
		/// </summary>
		/// <param name="columns">The column collection.</param>
		/// <returns>An enumerable of DataColumns.</returns>
		public static IEnumerable<DataColumn> AsEnumerable(this DataColumnCollection columns)
		{
			if (columns is null) throw new ArgumentNullException(nameof(columns));
			Contract.EndContractBlock();

			foreach (DataColumn c in columns)
				yield return c;
		}

		/// <summary>
		/// Generic enumerable extension for DataRowCollection.
		/// </summary>
		/// <param name="rows">The row collection.</param>
		/// <returns>An enumerable of DataRows.</returns>
		public static IEnumerable<DataRow> AsEnumerable(this DataRowCollection rows)
		{
			if (rows is null) throw new ArgumentNullException(nameof(rows));
			Contract.EndContractBlock();

			foreach (DataRow r in rows)
				yield return r;
		}

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<(string Field, string Column)>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
			=> Transformer<T>
			.Create(fieldMappingOverrides)
			.Results(table, clearSourceTable);

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, params (string Field, string Column)[] fieldMappingOverrides) where T : new()
			=> Transformer<T>
			.Create(fieldMappingOverrides)
			.Results(table, false);

		/// <summary>
		/// Loads all data into a queue before iterating (dequeuing) the results as type T.
		/// </summary>
		/// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
		/// <param name="table">The DataTable to read from.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="clearSourceTable">Clears the source table before providing the enumeration.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> To<T>(this DataTable table, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides, bool clearSourceTable = false) where T : new()
			=> table.To<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)), clearSourceTable);

		/// <summary>
		/// Useful extension for dequeuing items from a queue.
		/// Not thread safe but queueing/dequeuing items in between items is supported.
		/// </summary>
		/// <typeparam name="T">Return type of the source queue</typeparam>
		/// <returns>An enumerable of the items contained within the queue.</returns>
		public static IEnumerable<T> DequeueEach<T>(this Queue<T> source)
		{
			if (source is null)	throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			while (source.Count != 0)
				yield return source.Dequeue();
		}

	}
}
