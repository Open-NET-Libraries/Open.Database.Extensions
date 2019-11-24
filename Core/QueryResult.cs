using Open.Database.Extensions.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	/// <typeparam name="TResult">The type of the result property.</typeparam>
	public class QueryResult<TResult>
		where TResult : class

	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		internal QueryResult(IEnumerable<int> ordinals, IEnumerable<string> names, TResult result)
		{
			if (ordinals == null) throw new ArgumentNullException(nameof(ordinals));
			if (names == null) throw new ArgumentNullException(nameof(names));
			Result = result ?? throw new ArgumentNullException(nameof(result));
			Contract.EndContractBlock();

			Ordinals = ordinals as IList<int> ?? ordinals.ToArray();
			Names = names as IList<string> ?? names.ToArray();
			if (Ordinals.Count != Names.Count) throw new ArgumentException("Mismatched array lengths of ordinals and names.");
			ColumnCount = Ordinals.Count;
		}

		/// <summary>
		/// The number of columns.
		/// </summary>
		public int ColumnCount { get; }

		/// <summary>
		/// The ordinal values requested.
		/// </summary>
		public IList<int> Ordinals { get; }

		/// <summary>
		/// The column names requested.
		/// </summary>
		public IList<string> Names { get; }

		/// <summary>
		/// The values requested.  A Queue is used since values are typically used first in first out and dequeuing results helps reduced redundant memory usage.
		/// </summary>
		public TResult Result { get; }

	}

	/// <summary>
	/// A set of extensions for getting column data from a QueryResult.
	/// </summary>
	public static class QueryResultExtensions
	{
		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IEnumerable<Dictionary<string, object?>> DequeueAsMappedDictionaries(this QueryResult<Queue<object[]>> source)
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var q = source.Result;
			var names = source.Names;
			var count = source.ColumnCount;
			while (q.Count != 0)
			{
				var r = q.Dequeue();
				var d = new Dictionary<string, object?>(count);
				for (var i = 0; i < count; i++)
					d.Add(names[i], CoreExtensions.DBNullValueToNull(r[i]));
				yield return d;
			}
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this Task<QueryResult<Queue<object[]>>> source)
			=> (await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false)).DequeueAsMappedDictionaries();

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this ValueTask<QueryResult<Queue<object[]>>> source)
			=> (await source.ConfigureAwait(false)).DequeueAsMappedDictionaries();

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object[]>> source, IEnumerable<(string Field, string Column)>? fieldMappingOverrides = null)
			where T : new()
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(source);
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object[]>> source, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static async ValueTask<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object[]>>> source, IEnumerable<(string, string)>? fieldMappingOverrides = null)
			where T : new()
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static async ValueTask<IEnumerable<T>> DequeueAs<T>(this ValueTask<QueryResult<Queue<object[]>>> source, IEnumerable<(string, string)>? fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(await source.ConfigureAwait(false));
		}


		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static ValueTask<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object[]>>> source, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static ValueTask<IEnumerable<T>> DequeueAs<T>(this ValueTask<QueryResult<Queue<object[]>>> source, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IEnumerable<Dictionary<string, object?>> AsMappedDictionaries(this QueryResult<IEnumerable<object[]>> source)
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var q = source.Result;
			var names = source.Names;
			var count = source.ColumnCount;
			foreach (var r in q)
			{
				var d = new Dictionary<string, object?>(count);
				for (var i = 0; i < count; i++)
					d.Add(names[i], CoreExtensions.DBNullValueToNull(r[i]));
				yield return d;
			}
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this ValueTask<QueryResult<IEnumerable<object[]>>> source)
			=> AsMappedDictionaries(await source.ConfigureAwait(false));

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this Task<QueryResult<IEnumerable<object[]>>> source)
			=> AsMappedDictionaries(await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false));

	}

}
