﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Database.Extensions.Core
{
	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	/// <typeparam name="TResult">The type of the result property.</typeparam>
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1051:Do not declare visible instance fields", Justification = "Ensure single instance.")]
	public class QueryResult<TResult>
	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResult(in ImmutableArray<int> ordinals, in ImmutableArray<string> names, TResult result)
		{
			if (ordinals.Length != names.Length) throw new ArgumentException("Mismatched array lengths of ordinals and names.");
			Ordinals = ordinals;
			Names = names;
			Result = result;
			Contract.EndContractBlock();

			ColumnCount = ordinals.Length;
		}

		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResult(IEnumerable<int> ordinals, IEnumerable<string> names, TResult result)
			: this(Immute(ordinals), Immute(names), result)
		{
		}

		static ImmutableArray<T> Immute<T>(IEnumerable<T> source)
			=> source is ImmutableArray<T> o ? o : source?.ToImmutableArray() ?? throw new ArgumentNullException(nameof(source));

		/// <summary>
		/// The number of columns.
		/// </summary>
		public readonly int ColumnCount;

		/// <summary>
		/// The ordinal values requested.
		/// </summary>
		public readonly ImmutableArray<int> Ordinals;


		/// <summary>
		/// The column names requested.
		/// </summary>
		public readonly ImmutableArray<string> Names;

		/// <summary>
		/// The values requested.  A Queue is used since values are typically used first in first out and dequeuing results helps reduced redundant memory usage.
		/// </summary>
		public readonly TResult Result;

		/// <param name="result">The source of the result.</param>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "The result property exists.")]
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1065:Do not raise exceptions in unexpected locations", Justification = "Handle null reference better than just a NullReferenceException.")]
		public static implicit operator TResult(QueryResult<TResult> result) => (result ?? throw new ArgumentNullException(nameof(result))).Result;

	}

	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	/// <typeparam name="T">The type of the items in the resultant enumerble.</typeparam>
	/// <typeparam name="TResult">The type of the result property.</typeparam>
	public class QueryResultCollection<T, TResult> : QueryResult<TResult>, IEnumerable<T>
		where TResult : IEnumerable<T>
	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultCollection(in ImmutableArray<int> ordinals, in ImmutableArray<string> names, TResult result)
			: base(in ordinals, in names, result)
		{
			if (result == null) throw new ArgumentNullException(nameof(result));
		}

		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultCollection(IEnumerable<int> ordinals, IEnumerable<string> names, TResult result)
			: base(ordinals, names, result)
		{

		}


		/// <inheritdoc />
		public IEnumerator<T> GetEnumerator()
			=> Result.GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator()
			=> GetEnumerator();
	}

	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	/// <typeparam name="T">The type of the items in the resultant enumerble.</typeparam>
	public class QueryResultCollection<T> : QueryResultCollection<T, IEnumerable<T>>
	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultCollection(in ImmutableArray<int> ordinals, in ImmutableArray<string> names, IEnumerable<T> result)
			: base(in ordinals, in names, result)
		{
		}

		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultCollection(IEnumerable<int> ordinals, IEnumerable<string> names, IEnumerable<T> result)
			: base(ordinals, names, result)
		{

		}
	}

	/// <summary>
	/// A container for data reader results that also provides the column names and other helpful data methods.
	/// </summary>
	/// <typeparam name="T">The type of the items in the resultant enumerble.</typeparam>
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1710:Identifiers should have correct suffix", Justification = "")]
	public class QueryResultQueue<T> : QueryResult<Queue<T>> // Not exposed as enumerable to avoid confusion with the queue.
	{
		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultQueue(in ImmutableArray<int> ordinals, in ImmutableArray<string> names, Queue<T> result)
			: base(in ordinals, in names, result)
		{
		}

		/// <param name="ordinals">The ordinal values requested</param>
		/// <param name="names">The column names requested.</param>
		/// <param name="result">The result.</param>
		public QueryResultQueue(IEnumerable<int> ordinals, IEnumerable<string> names, Queue<T> result)
			: base(ordinals, names, result)
		{

		}
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
		public static IEnumerable<Dictionary<string, object?>> DequeueAsMappedDictionaries(this QueryResult<Queue<object?[]>> source)
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
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this Task<QueryResult<Queue<object?[]>>> source)
			=> (await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false)).DequeueAsMappedDictionaries();

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> DequeueAsMappedDictionaries(this ValueTask<QueryResult<Queue<object?[]>>> source)
			=> (await source.ConfigureAwait(false)).DequeueAsMappedDictionaries();

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object?[]>> source, IEnumerable<(string Field, string? Column)>? fieldMappingOverrides = null)
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
		public static IEnumerable<T> DequeueAs<T>(this QueryResult<Queue<object?[]>> source, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static async ValueTask<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object?[]>>> source, IEnumerable<(string, string?)>? fieldMappingOverrides = null)
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
		public static async ValueTask<IEnumerable<T>> DequeueAs<T>(this ValueTask<QueryResult<Queue<object?[]>>> source, IEnumerable<(string, string?)>? fieldMappingOverrides = null)
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
		public static ValueTask<IEnumerable<T>> DequeueAs<T>(this Task<QueryResult<Queue<object?[]>>> source, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and attempts to map the fields to type T.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
		/// <returns>An enumerable that dequeues the results and returns an entity of type T.</returns>
		public static ValueTask<IEnumerable<T>> DequeueAs<T>(this ValueTask<QueryResult<Queue<object?[]>>> source, IEnumerable<KeyValuePair<string, string?>>? fieldMappingOverrides)
			where T : new()
			=> DequeueAs<T>(source, fieldMappingOverrides?.Select(kvp => (kvp.Key, kvp.Value)));

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static IEnumerable<Dictionary<string, object?>> AsMappedDictionaries(this QueryResult<IEnumerable<object?[]>> source)
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
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this ValueTask<QueryResult<IEnumerable<object?[]>>> source)
			=> AsMappedDictionaries(await source.ConfigureAwait(false));

		/// <summary>
		/// Returns an enumerable that dequeues the results and returns a column mapped dictionary for each entry.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="source">The query result.  Typically produced by a .Retrieve method.</param>
		/// <returns>An enumerable that dequeues the results and returns a column mapped dictionary for each entry</returns>
		public static async ValueTask<IEnumerable<Dictionary<string, object?>>> AsMappedDictionaries(this Task<QueryResult<IEnumerable<object?[]>>> source)
			=> AsMappedDictionaries(await (source ?? throw new ArgumentNullException(nameof(source))).ConfigureAwait(false));

	}

}
