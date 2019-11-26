using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#if NETSTANDARD2_1
using System.Runtime.CompilerServices;
#endif

namespace Open.Database.Extensions
{
	/// <summary>
	/// Extension methods for Data Readers.
	/// </summary>
	public static class DataReaderExtensions
	{
		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		public static void ForEach(this IDataReader reader, Action<IDataRecord> handler)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			while (reader.Read())
				handler(reader);
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
		/// <param name="throwOnCancellation">If true, when cancelled, will exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
		public static void ForEach(this IDataReader reader, Action<IDataRecord> handler, CancellationToken cancellationToken, bool throwOnCancellation = false)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (cancellationToken.CanBeCanceled)
			{
				if (throwOnCancellation)
					cancellationToken.ThrowIfCancellationRequested();
				else if (cancellationToken.IsCancellationRequested)
					return;

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (reader.Read())
				{
					if (cancelled)
					{
						handler(reader); // we recieved the results, might as well use them.
						if (throwOnCancellation)
							cancellationToken.ThrowIfCancellationRequested();

						break;
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
						handler(reader);
					}
				}
			}
			else
			{
				while (reader.Read())
					handler(reader);
			}
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		public static async ValueTask ForEachAsync(this DbDataReader reader,
			Action<IDataRecord> handler,
			bool useReadAsync = true,
			CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true))
					handler(reader);
			}
			else
			{
				ForEach(reader, handler, cancellationToken, true);
			}
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask ForEachAsync(this DbDataReader reader, Func<IDataRecord, ValueTask> handler, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (handler is null) throw new ArgumentNullException(nameof(handler));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true))
					await handler(reader);
			}
			else if (cancellationToken.CanBeCanceled)
			{
				cancellationToken.ThrowIfCancellationRequested();

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (reader.Read())
				{
					if (cancelled)
					{
						await handler(reader); // we recieved the results, might as well use them.
						cancellationToken.ThrowIfCancellationRequested();
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
						await handler(reader);
					}
				}
			}
			else
			{
				while (reader.Read())
					await handler(reader);
			}
		}

		/// <summary>
		/// Iterates all records from an IDataReader.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="handler">The handler function for each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static ValueTask ForEachAsync(this DbDataReader reader, Func<IDataRecord, ValueTask> handler, CancellationToken cancellationToken)
			=> ForEachAsync(reader, handler, true, cancellationToken);

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			if (reader.Read())
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = new object[fieldCount];
					reader.GetValues(row);
					yield return row;
				} while (reader.Read());
			}
		}

		internal static IEnumerable<object[]> AsEnumerableInternal(this IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			if (readStarted || reader.Read())
			{
				var o = ordinals as IList<int> ?? ordinals.ToArray();
				var fieldCount = o.Count;
				if (fieldCount == 0)
				{
					do
					{
						yield return Array.Empty<object>();
					}
					while (reader.Read());
				}
				else
				{
					do
					{
						var row = new object[fieldCount];
						for (var i = 0; i < fieldCount; i++)
							row[i] = reader.GetValue(o[i]);
						yield return row;
					}
					while (reader.Read());
				}

			}
		}

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals)
			=> AsEnumerableInternal(reader, ordinals, false);

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params int[] others)
			=> AsEnumerable(reader, Enumerable.Repeat(n, 1).Concat(others));

#if NETSTANDARD2_1
		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="cancellationToken">Optional iteration cancellation token.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static async IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			if (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true))
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = new object[fieldCount];
					reader.GetValues(row);
					yield return row;
				}
				while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true));
			}
		}

		static async IAsyncEnumerable<object[]> AsAsyncEnumerableInternal(this DbDataReader reader, IEnumerable<int> ordinals, bool readStarted, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			if (readStarted || !cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true))
			{
				var o = ordinals as IList<int> ?? ordinals.ToArray();
				var fieldCount = o.Count;
				if (fieldCount == 0)
				{
					do
					{
						yield return Array.Empty<object>();
					}
					while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true));
				}
				else
				{
					do
					{
						var row = new object[fieldCount];
						for (var i = 0; i < fieldCount; i++)
							row[i] = reader.GetValue(o[i]);
						yield return row;
					}
					while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true));
				}
			}
		}

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// DBNull values are retained.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
		/// <param name="cancellationToken">Optional iteration cancellation token.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, CancellationToken cancellationToken = default)
			=> AsAsyncEnumerableInternal(reader, ordinals, false, cancellationToken);


		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="cancellationToken">The iteration cancellation token.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Extended params prevent this.")]
		public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, CancellationToken cancellationToken, int n, params int[] others)
			=> AsAsyncEnumerable(reader, Enumerable.Repeat(n, 1).Concat(others), cancellationToken);

		/// <summary>
		/// Enumerates all the remaining values of the current result set of a data reader.
		/// </summary>
		/// <param name="reader">The reader to enumerate.</param>
		/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
		/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
		/// <returns>An enumeration of the values returned from a data reader.</returns>
		public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, int n, params int[] others)
			=> AsAsyncEnumerable(reader, Enumerable.Repeat(n, 1).Concat(others));
#endif
		/// <summary>
		/// Iterates records from an IDataReader and passes the IDataRecord to a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> transform)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			while (reader.Read())
				yield return transform(reader);
		}

		/// <summary>
		/// Iterates records from an IDataReader and passes the IDataRecord to a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
		/// <param name="throwOnCancellation">If true, when cancelled, will exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> transform, CancellationToken cancellationToken, bool throwOnCancellation = false)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (cancellationToken.CanBeCanceled)
			{
				if (throwOnCancellation)
					cancellationToken.ThrowIfCancellationRequested();
				else if (cancellationToken.IsCancellationRequested)
					yield break;

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (reader.Read())
				{
					if (cancelled)
					{
						yield return transform(reader); // we recieved the results, might as well use them.
						if (throwOnCancellation)
							cancellationToken.ThrowIfCancellationRequested();

						break;
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
						yield return transform(reader);
					}
				}
			}
			else
			{
				while (reader.Read())
					yield return transform(reader);
			}
		}

		/// <summary>
		/// Iterates records from an IDataReader and passes the IDataRecord to a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="throwOnCancellation">If true, when cancelled, will exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload provided for convienience.")]
		public static IEnumerable<T> Select<T>(this IDataReader reader, CancellationToken cancellationToken, Func<IDataRecord, T> transform, bool throwOnCancellation = false)
			=> Select(reader, transform, cancellationToken, throwOnCancellation);

#if NETSTANDARD2_1
		/// <summary>
		/// Asynchronously iterates records from an DbDataReader and passes the IDataRecord to a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The DbDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static async IAsyncEnumerable<T> SelectAsync<T>(this DbDataReader reader,
			Func<IDataRecord, T> transform)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			while (await reader.ReadAsync().ConfigureAwait(true))
				yield return transform(reader);
		}

		/// <summary>
		/// Asyncronously iterates all records from an IDataReader.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The DbDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <param name="throwOnCancellation">If true, when cancelled, will exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
		/// <returns>An enumerable used to iterate the results.</returns>
		public static async IAsyncEnumerable<T> IterateAsync<T>(this DbDataReader reader,
			Func<IDataRecord, T> transform,
			[EnumeratorCancellation] CancellationToken cancellationToken, bool throwOnCancellation = false)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			if (cancellationToken.CanBeCanceled)
			{
				if (throwOnCancellation)
					cancellationToken.ThrowIfCancellationRequested();
				else if (cancellationToken.IsCancellationRequested)
					yield break;

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (await reader.ReadAsync().ConfigureAwait(true))
				{
					if (cancelled)
					{
						yield return transform(reader); // we recieved the results, might as well use them.
						if (throwOnCancellation)
							cancellationToken.ThrowIfCancellationRequested();

						break;
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
						yield return transform(reader);
					}
				}
			}
			{
				while (await reader.ReadAsync().ConfigureAwait(true))
					yield return transform(reader);
			}
		}
#endif

		/// <summary>
		/// Shortcut for .Iterate(transform).ToList();
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A list of the transformed results.</returns>
		public static List<T> ToList<T>(this IDataReader reader,
			Func<IDataRecord, T> transform, CancellationToken cancellationToken = default)
			=> reader.Select(transform, cancellationToken).ToList();

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
			Func<IDataRecord, T> transform, CancellationToken cancellationToken = default)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var list = new List<T>();
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true)) list.Add(transform(reader));
			return list;
		}

		/// <summary>
		/// Asynchronously iterates all records using an IDataReader and returns the desired results as a list.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The SqlDataReader to read from.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>A task containing a list of all results.</returns>
		public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
			Func<IDataRecord, ValueTask<T>> transform, CancellationToken cancellationToken = default)
		{
			if (transform is null) throw new ArgumentNullException(nameof(transform));
			Contract.EndContractBlock();

			var list = new List<T>();
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true)) list.Add(await transform(reader));
			return list;
		}

		/// <summary>
		/// Shortcut for .Iterate(transform).ToArray();
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="transform">The transform function to process each IDataRecord.</param>
		/// <returns>An array of the transformed results.</returns>
		public static T[] ToArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
			=> reader.Select(transform).ToArray();

		/// <summary>
		/// Loads all remaining data from an IDataReader into a DataTable.
		/// </summary>
		/// <param name="reader">The IDataReader to load data from.</param>
		/// <returns>The resultant DataTable.</returns>
		public static DataTable ToDataTable(this IDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var table = new DataTable();
			table.Load(reader);
			return table;
		}

		/// <summary>
		/// Loads all data from a command through an IDataReader into a DataTables.
		/// Calls .NextResult() to check for more results.
		/// </summary>
		/// <param name="reader">The IDataReader to load data from.</param>
		/// <returns>The resultant list of DataTables.</returns>
		public static List<DataTable> ToDataTables(this IDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var results = new List<DataTable>();
			do
			{
				results.Add(reader.ToDataTable());
			}
			while (reader.NextResult());
			return results;
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			while (reader.Read() && predicate(reader)) { }
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
		/// <param name="throwOnCancellation">If true, when cancelled, will exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
		public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate, CancellationToken cancellationToken, bool throwOnCancellation = false)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			if (cancellationToken.CanBeCanceled)
			{
				if (throwOnCancellation)
					cancellationToken.ThrowIfCancellationRequested();
				else if (cancellationToken.IsCancellationRequested)
					return;

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (reader.Read() && predicate(reader))
				{
					if (cancelled)
					{
						if (throwOnCancellation)
							cancellationToken.ThrowIfCancellationRequested();

						break;
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
					}
				}
			}
			else
			{
				while (reader.Read() && predicate(reader)) { }
			}
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true) && predicate(reader)) { }
		}

		/// <summary>
		/// Iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, bool useReadAsync = false, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true) && await predicate(reader)) { }
			}
			else if (cancellationToken.CanBeCanceled)
			{
				cancellationToken.ThrowIfCancellationRequested();

				// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
				var cancelled = false;
				while (reader.Read() && await predicate(reader))
				{
					if (cancelled)
					{
						cancellationToken.ThrowIfCancellationRequested();
						break;
					}
					else
					{
						cancelled = cancellationToken.IsCancellationRequested;
					}
				}
			}
			else
			{
				while (reader.Read() && await predicate(reader)) { }
			}
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader while the predicate returns true.
		/// </summary>
		/// <param name="reader">The DbDataReader to load data from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true) && await predicate(reader)) { }
		}

		/// <summary>
		/// Asynchronously iterates an IDataReader on a command while the predicate returns true.
		/// </summary>
		/// <param name="reader">The DbDataReader to load data from.</param>
		/// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (predicate is null) throw new ArgumentNullException(nameof(predicate));
			Contract.EndContractBlock();

			if (useReadAsync)
			{
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true) && predicate(reader)) { }
			}
			else if (cancellationToken.CanBeCanceled)
			{
				while (!cancellationToken.IsCancellationRequested && reader.Read() && predicate(reader))
					cancellationToken.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read() && predicate(reader)) { }
			}
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are then converted to null.
		/// </summary>
		/// <returns>The enumerable first ordinal values.</returns>
		public static IEnumerable<object?> FirstOrdinalResults(this IDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var results = new Queue<object>(reader.Select(r => r.GetValue(0)));
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDataReader reader)
			=> reader is DbDataReader dbr
			? dbr.FirstOrdinalResults<T0>()
			: reader.FirstOrdinalResults().Cast<T0>();

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <returns>The enumerable of casted values.</returns>
		public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var results = new Queue<T0>();
			while (reader.Read())
			{
				results.Enqueue(
					reader.IsDBNull(0)
					? default
					: reader.GetFieldValue<T0>(0)
				);
			}

			return results.DequeueEach();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// DBNull values are converted to null.
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The list of values.</returns>
		public static async ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var results = new Queue<object>();
			await reader.ForEachAsync(r => results.Enqueue(r.GetValue(0)), useReadAsync, cancellationToken).ConfigureAwait(false);
			return results.DequeueEach().DBNullToNull();
		}

		/// <summary>
		/// Reads the first column values from every record.
		/// Any DBNull values are then converted to null and casted to type T0;
		/// </summary>
		/// <param name="reader">The IDataReader to iterate.</param>
		/// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
		/// <param name="cancellationToken">Optional cancellation token.</param>
		/// <returns>The enumerable of casted values.</returns>
		public static async ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			var results = new Queue<T0>();
			if (useReadAsync)
			{
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true))
				{
					results.Enqueue(
						await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false)
						? default
						: await reader.GetFieldValueAsync<T0>(0, cancellationToken).ConfigureAwait(false)
					);
				}
			}
			else if (cancellationToken.CanBeCanceled)
			{
				while (!cancellationToken.IsCancellationRequested && reader.Read())
				{
					results.Enqueue(
						reader.IsDBNull(0)
						? default
						: reader.GetFieldValue<T0>(0)
					);
				}
				cancellationToken.ThrowIfCancellationRequested();
			}
			else
			{
				while (reader.Read())
				{
					results.Enqueue(
						reader.IsDBNull(0)
						? default
						: reader.GetFieldValue<T0>(0)
					);
				}
			}

			return results.DequeueEach();
		}
	}
}
