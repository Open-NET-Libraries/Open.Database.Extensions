using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks.Dataflow;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable NotAccessedField.Global
// ReSharper disable UnusedMember.Global

namespace Open.Database.Extensions.Dataflow
{
	/// <summary>
	/// A set of extensions for getting column data from a QueryResult.
	/// </summary>
	public static class QueryResultExtensions
	{
		internal static object? DBNullValueToNull(object? value)
			=> value == DBNull.Value ? null : value;

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a RetrieveAsSourceBlock method.</param>
		/// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IReceivableSourceBlock<Dictionary<string, object?>> AsMappedDictionaries(this QueryResult<IReceivableSourceBlock<object[]>> source)
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var q = source.Result;
			var names = source.Names;
			var count = source.ColumnCount;

			var x = new TransformBlock<object[], Dictionary<string, object?>>(r =>
			{
				var d = new Dictionary<string, object?>(count);
				for (var i = 0; i < count; i++)
					d.Add(names[i], DBNullValueToNull(r[i]));
				return d;
			});

			q.LinkTo(x, new DataflowLinkOptions { PropagateCompletion = true });
			return x;
		}

		/// <summary>
		/// Returns a block that attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a RetrieveAsSourceBlock method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <param name="options">The ExecutionDataflowBlockOptions for transforming the data into the source block.</param>
		/// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IReceivableSourceBlock<T> To<T>(
			this QueryResult<IReceivableSourceBlock<object[]>> source,
			IEnumerable<(string Field, string Column)>? fieldMappingOverrides,
			ExecutionDataflowBlockOptions? options = null)
			where T : new()
		{
			var x = new Transformer<T>(fieldMappingOverrides);
			return x.Results(source, options);
		}

		/// <summary>
		/// Returns a block that attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a RetrieveAsSourceBlock method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IReceivableSourceBlock<T> To<T>(this QueryResult<IReceivableSourceBlock<object[]>> source, IEnumerable<KeyValuePair<string, string>>? fieldMappingOverrides)
			where T : new()
			=> To<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns a block that attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a RetrieveAsSourceBlock method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An block that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IReceivableSourceBlock<T> To<T>(this QueryResult<IReceivableSourceBlock<object[]>> source, params (string Field, string Column)[] fieldMappingOverrides)
			where T : new()
			=> To<T>(source, fieldMappingOverrides as IEnumerable<(string Field, string Column)>);
	}

}
