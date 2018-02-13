using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	public class QueryResult<TResult>
		where TResult : class

	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		internal QueryResult(int[] ordinals, string[] names, TResult result)
		{
			Ordinals = ordinals ?? throw new ArgumentNullException(nameof(ordinals));
			Names = names ?? throw new ArgumentNullException(nameof(names));
			Result = result ?? throw new ArgumentNullException(nameof(result));
			if (ordinals.Length != names.Length) throw new ArgumentException("Mismatched array lengths of ordinals and names.");
			ColumnCount = ordinals.Length;
		}

		/// <summary>
		/// The number of columns.
		/// </summary>
		public readonly int ColumnCount;

		/// <summary>
		/// The ordinal values requested.
		/// </summary>
		public readonly int[] Ordinals;

		/// <summary>
		/// The column names requested.
		/// </summary>
		public readonly string[] Names;

		/// <summary>
		/// The values requested.  A Queue is used since values are typically used first in first out and dequeuing results helps reduced redunant memory usage.
		/// </summary>
		public readonly TResult Result;

	}

	/// <summary>
	/// A set of extensions for getting column data from a QueryResult.
	/// </summary>
	public static class QueryResultExtensions
	{
		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IEnumerable<Dictionary<string, object>> DequeueAsMappedDictionaries(this QueryResult<Queue<object[]>> source)
		{
			var q = source.Result;
			var names = source.Names;
			var count = source.ColumnCount;
			while (q.Count != 0)
			{
				var r = q.Dequeue();
				var d = new Dictionary<string, object>(count);
				for (var i = 0; i < count; i++)
					d.Add(names[i], r[i]);
				yield return d;
			}
		}

        /// <summary>
        /// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
        /// </summary>
        /// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
        /// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
        public static async Task<IEnumerable<Dictionary<string, object>>> DequeueAsMappedDictionaries(this Task<QueryResult<Queue<object[]>>> source)
            => (await source).DequeueAsMappedDictionaries();

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object[]>> source, IEnumerable<(string Field, string Column)> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(source);
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object[]>> source, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> source.DequeueAs<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static async Task<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object[]>>> source, IEnumerable<(string, string)> fieldMappingOverrides = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.AsDequeueingEnumerable(await source);
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static Task<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object[]>>> source, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> source.DequeueAs<T>(fieldMappingOverrides?.Select(kvp=>(kvp.Key,kvp.Value)));


        /// <summary>
        /// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
        /// </summary>
        /// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
        /// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
        public static ISourceBlock<Dictionary<string, object>> AsMappedDictionaries(this QueryResult<ISourceBlock<object[]>> source)
        {
            var q = source.Result;
            var names = source.Names;
            var count = source.ColumnCount;

            var x = new TransformBlock<object[], Dictionary<string, object>>(r=>
            {
                var d = new Dictionary<string, object>(count);
                for (var i = 0; i < count; i++)
                    d.Add(names[i], r[i]);
                return d;
            });

            q.LinkTo(x);
            q.Completion.ContinueWith(t => x.Complete());
            return x;
        }

        /// <summary>
        /// Returns a block that attempts to map the fields to type T.
        /// </summary>
        /// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
        /// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
        public static ISourceBlock<T> To<T>(this QueryResult<ISourceBlock<object[]>> source, IEnumerable<(string Field, string Column)> fieldMappingOverrides)
            where T : new()
        {
            var x = new Transformer<T>(fieldMappingOverrides);
            return x.Results(source);
        }

		/// <summary>
		/// Returns a block that attempts to map the fields to type T.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static ISourceBlock<T> To<T>(this QueryResult<ISourceBlock<object[]>> source, IEnumerable<KeyValuePair<string, string>> fieldMappingOverrides)
			where T : new()
			=> source.To<T>(fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IEnumerable<Dictionary<string, object>> AsMappedDictionaries(this QueryResult<IEnumerable<object[]>> source)
		{
			var q = source.Result;
			var names = source.Names;
			var count = source.ColumnCount;
			foreach(var r in q)
			{
				var d = new Dictionary<string, object>(count);
				for (var i = 0; i < count; i++)
					d.Add(names[i], r[i]);
				yield return d;
			}
		}

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async Task<IEnumerable<Dictionary<string, object>>> AsMappedDictionaries(this Task<QueryResult<IEnumerable<object[]>>> source)
			=> AsMappedDictionaries(await source);

	}

}
