using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#if NETSTANDARD2_1
using System.Runtime.CompilerServices;
#endif

namespace Open.Database.Extensions;

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
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <param name="cancellationToken">An optional cancellation token for stopping the iteration.</param>
    public static void ForEach(this IDataReader reader, Action<IDataRecord> handler, bool throwOnCancellation, CancellationToken cancellationToken = default)
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
    /// <param name="cancellationToken">An optional cancellation token for stopping the iteration.</param>
    public static void ForEach(this IDataReader reader, Action<IDataRecord> handler, CancellationToken cancellationToken = default)
        => ForEach(reader, handler, false, cancellationToken);

    /// <summary>
    /// Iterates all records from an IDataReader.
    /// </summary>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="useReadAsync">If true (default) will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
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
            ForEach(reader, handler, true, cancellationToken);
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
                await handler(reader).ConfigureAwait(false);
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
                    await handler(reader).ConfigureAwait(false); // we recieved the results, might as well use them.
                    cancellationToken.ThrowIfCancellationRequested();
                }
                else
                {
                    cancelled = cancellationToken.IsCancellationRequested;
                    await handler(reader).ConfigureAwait(false);
                }
            }
        }
        else
        {
            while (reader.Read())
                await handler(reader).ConfigureAwait(false);
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
        return reader is null
            ? throw new ArgumentNullException(nameof(reader))
            : AsEnumerableCore(reader);

        static IEnumerable<object[]> AsEnumerableCore(IDataReader reader)
        {
            if (!reader.Read())
                yield break;

            var fieldCount = reader.FieldCount;
            do
            {
                var row = new object[fieldCount];
                reader.GetValues(row);
                yield return row;
            } while (reader.Read());
        }
    }

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IEnumerable<object?[]> AsEnumerable(this IDataReader reader, ArrayPool<object?> arrayPool)
    {
        return reader is null
            ? throw new ArgumentNullException(nameof(reader))
            : arrayPool is null
            ? throw new ArgumentNullException(nameof(arrayPool))
            : AsEnumerableCore(reader, arrayPool);

        static IEnumerable<object?[]> AsEnumerableCore(IDataReader reader, ArrayPool<object?> arrayPool)
        {
            if (!reader.Read())
                yield break;

            var fieldCount = reader.FieldCount;
            do
            {
                var row = arrayPool.Rent(fieldCount);
                reader.GetValues(row);
                yield return row;
            } while (reader.Read());
        }
    }

    internal static IEnumerable<object[]> AsEnumerableInternal(this IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: AsEnumerableInternalCore(reader, ordinals, readStarted);

		static IEnumerable<object[]> AsEnumerableInternalCore(IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
        {
            if (!readStarted && !reader.Read())
                yield break;

            var o = ordinals as IList<int> ?? ordinals.ToArray();
            var fieldCount = o.Count;
            if (fieldCount == 0)
            {
                do
                {
                    yield return Array.Empty<object>();
                }
                while (reader.Read());
                yield break;
            }

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

    internal static IEnumerable<object?[]> AsEnumerableInternal(this IDataReader reader, IEnumerable<int> ordinals, bool readStarted, ArrayPool<object?> arrayPool)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: arrayPool is null ? throw new ArgumentNullException(nameof(arrayPool))
            : AsEnumerableInternalCore();

		IEnumerable<object?[]> AsEnumerableInternalCore()
		{
            if (!readStarted && !reader.Read())
                yield break;

			var o = ordinals as IList<int> ?? ordinals.ToArray();
			var fieldCount = o.Count;
            do
            {
                var row = arrayPool.Rent(fieldCount);
                for (var i = 0; i < fieldCount; i++)
                    row[i] = reader.GetValue(o[i]);
                yield return row;
            }
            while (reader.Read());
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
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IEnumerable<object?[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals, ArrayPool<object?> arrayPool)
        => AsEnumerableInternal(reader, ordinals, false, arrayPool);

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params int[] others)
        => AsEnumerable(reader, CoreExtensions.Concat(n, others));

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IEnumerable<object?[]> AsEnumerable(this IDataReader reader, ArrayPool<object?> arrayPool, int n, params int[] others)
        => AsEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool);

    /// <summary>
    /// Iterates records from an IDataReader and passes the IDataRecord to a transform function.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> transform)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
            ? throw new ArgumentNullException(nameof(transform))
            : SelectCore();

		IEnumerable<T> SelectCore()
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
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    public static IEnumerable<T> Select<T>(this IDataReader reader, Func<IDataRecord, T> transform, CancellationToken cancellationToken, bool throwOnCancellation = false)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectCore(reader, transform, cancellationToken, throwOnCancellation);

		static IEnumerable<T> SelectCore(IDataReader reader, Func<IDataRecord, T> transform, CancellationToken cancellationToken, bool throwOnCancellation)
		{
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
	}

    /// <summary>
    /// Iterates records from an IDataReader and passes the IDataRecord to a transform function.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload provided for convienience.")]
    public static IEnumerable<T> Select<T>(this IDataReader reader, CancellationToken cancellationToken, Func<IDataRecord, T> transform, bool throwOnCancellation = false)
        => Select(reader, transform, cancellationToken, throwOnCancellation);

#if NETSTANDARD2_1

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="cancellationToken">Optional iteration cancellation token.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, CancellationToken cancellationToken = default)
    {
		return reader is null
            ? throw new ArgumentNullException(nameof(reader))
            : AsAsyncEnumerableCore(reader, cancellationToken);

		static async IAsyncEnumerable<object[]> AsAsyncEnumerableCore(DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			Contract.EndContractBlock();

			if (cancellationToken.IsCancellationRequested || !await reader.ReadAsync().ConfigureAwait(true))
				yield break;

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

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="arrayPool">An optional array pool to acquire buffers from.</param>
    /// <param name="cancellationToken">Optional iteration cancellation token.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    public static IAsyncEnumerable<object?[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object?> arrayPool, CancellationToken cancellationToken = default)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: arrayPool is null
			? throw new ArgumentNullException(nameof(arrayPool))
			: AsAsyncEnumerableCore(reader, arrayPool, cancellationToken);

		static async IAsyncEnumerable<object?[]> AsAsyncEnumerableCore(DbDataReader reader, ArrayPool<object?> arrayPool, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested || !await reader.ReadAsync().ConfigureAwait(true))
				yield break;

			var fieldCount = reader.FieldCount;
			do
			{
				var row = arrayPool.Rent(fieldCount);
				reader.GetValues(row);
				yield return row;
			}
			while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true));
		}
	}

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    static IAsyncEnumerable<object[]> AsAsyncEnumerableInternal(this DbDataReader reader, IEnumerable<int> ordinals, bool readStarted, CancellationToken cancellationToken)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: AsAsyncEnumerableInternalCore(reader, ordinals, readStarted, cancellationToken);

		static async IAsyncEnumerable<object[]> AsAsyncEnumerableInternalCore(DbDataReader reader, IEnumerable<int> ordinals, bool readStarted, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (!readStarted && (cancellationToken.IsCancellationRequested || !await reader.ReadAsync().ConfigureAwait(true)))
				yield break;

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

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    static IAsyncEnumerable<object?[]> AsAsyncEnumerableInternal(
		this DbDataReader reader,
		IEnumerable<int> ordinals,
		bool readStarted,
		ArrayPool<object?> arrayPool,
		CancellationToken cancellationToken)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: arrayPool is null
			? throw new ArgumentNullException(nameof(arrayPool))
			: AsAsyncEnumerableInternalCore(reader, ordinals, readStarted, arrayPool, cancellationToken);

		static async IAsyncEnumerable<object?[]> AsAsyncEnumerableInternalCore(
			DbDataReader reader,
			IEnumerable<int> ordinals,
			bool readStarted,
			ArrayPool<object?> arrayPool,
            [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (!readStarted && (cancellationToken.IsCancellationRequested || !await reader.ReadAsync().ConfigureAwait(true)))
				yield break;

			var o = ordinals as IList<int> ?? ordinals.ToArray();
			var fieldCount = o.Count;
			do
			{
				var row = arrayPool.Rent(fieldCount);
				for (var i = 0; i < fieldCount; i++)
					row[i] = reader.GetValue(o[i]);
				yield return row;
			}
			while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true));
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
    /// DBNull values are retained.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="cancellationToken">Optional iteration cancellation token.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IAsyncEnumerable<object?[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, ArrayPool<object?> arrayPool, CancellationToken cancellationToken = default)
        => AsAsyncEnumerableInternal(reader, ordinals, false, arrayPool, cancellationToken);

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
        => AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), cancellationToken);

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="cancellationToken">The iteration cancellation token.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Extended params prevent this.")]
    public static IAsyncEnumerable<object?[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object?> arrayPool, CancellationToken cancellationToken, int n, params int[] others)
        => AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool, cancellationToken);

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, int n, params int[] others)
        => AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others));

    /// <summary>
    /// Enumerates all the remaining values of the current result set of a data reader.
    /// </summary>
    /// <param name="reader">The reader to enumerate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>An enumeration of the values returned from a data reader.</returns>
    public static IAsyncEnumerable<object?[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object?> arrayPool, int n, params int[] others)
        => AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool);

    /// <summary>
    /// Asyncronously iterates all records from an IDataReader.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The DbDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    public static IAsyncEnumerable<T> SelectAsync<T>(this DbDataReader reader,
        Func<IDataRecord, T> transform,
        bool throwOnCancellation,
        CancellationToken cancellationToken = default)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectAsyncCore(reader, transform, throwOnCancellation, cancellationToken);

		static async IAsyncEnumerable<T> SelectAsyncCore(DbDataReader reader, Func<IDataRecord, T> transform, bool throwOnCancellation, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (throwOnCancellation)
			{
				cancellationToken.ThrowIfCancellationRequested();
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true))
					yield return transform(reader);
			}
			else
			{
				if (cancellationToken.IsCancellationRequested) yield break;
				while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(true))
					yield return transform(reader);
			}
		}
	}

    /// <summary>
    /// Asynchronously iterates records from an DbDataReader and passes the IDataRecord to a transform function.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The DbDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    public static IAsyncEnumerable<T> SelectAsync<T>(this DbDataReader reader,
        Func<IDataRecord, T> transform,
        CancellationToken cancellationToken = default)
        => SelectAsync(reader, transform, false, cancellationToken);

    /// <summary>
    /// Asyncronously iterates all records from an IDataReader.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The DbDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional for this method to prevent cancellation exception.")]
    public static IAsyncEnumerable<T> SelectAsync<T>(this IDataReader reader,
        Func<IDataRecord, ValueTask<T>> transform,
        bool throwOnCancellation,
        CancellationToken cancellationToken = default)
    {
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectAsyncCore(reader, transform, throwOnCancellation, cancellationToken);

		static async IAsyncEnumerable<T> SelectAsyncCore(IDataReader reader, Func<IDataRecord, ValueTask<T>> transform, bool throwOnCancellation, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (throwOnCancellation)
			{
				cancellationToken.ThrowIfCancellationRequested();
				if (reader is DbDataReader r)
				{
					while (await r.ReadAsync(cancellationToken).ConfigureAwait(true))
						yield return await transform(r).ConfigureAwait(false);
				}
				else
				{
					while (reader.Read())
					{
						cancellationToken.ThrowIfCancellationRequested();
						yield return await transform(reader).ConfigureAwait(false);
					}
				}
			}
			else
			{
				if (cancellationToken.IsCancellationRequested) yield break;
				if (reader is DbDataReader r)
				{
					while (!cancellationToken.IsCancellationRequested && await r.ReadAsync().ConfigureAwait(true))
						yield return await transform(r).ConfigureAwait(false);
				}
				else
				{
					while (!cancellationToken.IsCancellationRequested && reader.Read())
						yield return await transform(reader).ConfigureAwait(false);
				}
			}
		}
	}

    /// <summary>
    /// Asynchronously iterates records from an DbDataReader and passes the IDataRecord to a transform function.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The DbDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>An enumerable used to iterate the results.</returns>
    public static IAsyncEnumerable<T> SelectAsync<T>(this IDataReader reader,
        Func<IDataRecord, ValueTask<T>> transform,
        CancellationToken cancellationToken = default)
        => SelectAsync(reader, transform, false, cancellationToken);
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
        if (reader is null) throw new ArgumentNullException(nameof(reader));
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
        if (reader is null) throw new ArgumentNullException(nameof(reader));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        var list = new List<T>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(true)) list.Add(await transform(reader).ConfigureAwait(false));
        return list;
    }

    /// <summary>
    /// Shortcut for .Select(transform).ToArray();
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>An array of the transformed results.</returns>
    public static T[] ToArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
        => reader.Select(transform).ToArray();

    /// <summary>
    /// Shortcut for .Select(transform).ToImmutableArray();
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>An array of the transformed results.</returns>
    public static ImmutableArray<T> ToImmutableArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
        => reader.Select(transform).ToImmutableArray();

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
    /// <param name="throwOnCancellation">If true, when cancelled, may exit the iteration via an exception. Otherwise when cancelled will simply stop iterating and return without exception.</param>
    /// <param name="cancellationToken">An optional cancellation token for stopping the iteration.</param>
    public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate, bool throwOnCancellation, CancellationToken cancellationToken = default)
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
    /// <param name="cancellationToken">An optional cancellation token for stopping the iteration.</param>
    public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate, CancellationToken cancellationToken = default)
        => IterateWhile(reader, predicate, false, cancellationToken);

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
    /// Asynchronously iterates an IDataReader on a command while the predicate returns true.
    /// </summary>
    /// <param name="reader">The DbDataReader to load data from.</param>
    /// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
    /// <param name="useReadAsync">If true will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, bool useReadAsync, CancellationToken cancellationToken = default)
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
            cancellationToken.ThrowIfCancellationRequested();
            while (reader.Read() && predicate(reader))
                cancellationToken.ThrowIfCancellationRequested();
        }
        else
        {
            while (reader.Read() && predicate(reader)) { }
        }
    }

    static async ValueTask IterateWhileAsyncInternal(IDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, CancellationToken cancellationToken)
    {
        if (reader is null) throw new ArgumentNullException(nameof(reader));
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        Contract.EndContractBlock();

        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The following pattern allows for the reader to complete if it actually reached the end before cancellation.
            var cancelled = false;
            while (reader.Read() && await predicate(reader).ConfigureAwait(true))
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
            while (reader.Read() && await predicate(reader).ConfigureAwait(true)) { }
        }
    }

    /// <summary>
    /// Asynchronously iterates an IDataReader while the predicate returns true.
    /// </summary>
    /// <param name="reader">The DbDataReader to load data from.</param>
    /// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public static async ValueTask IterateWhileAsync(this IDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (reader is null) throw new ArgumentNullException(nameof(reader));
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        Contract.EndContractBlock();

        if (reader is DbDataReader r)
        {
            while (await r.ReadAsync(cancellationToken).ConfigureAwait(true) && await predicate(reader).ConfigureAwait(true)) { }
        }
        else
        {
            // Does not use .ReadAsync();
            await IterateWhileAsyncInternal(reader, predicate, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Iterates an IDataReader while the predicate returns true.
    /// </summary>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="predicate">The handler function that processes each IDataRecord and decides if iteration should continue.</param>
    /// <param name="useReadAsync">If true will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public static ValueTask IterateWhileAsync(this IDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, bool useReadAsync, CancellationToken cancellationToken = default)
    {
        if (reader is null) throw new ArgumentNullException(nameof(reader));
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        Contract.EndContractBlock();

        return useReadAsync
            ? IterateWhileAsync(reader, predicate, cancellationToken)
            // Does not use .ReadAsync();
            : IterateWhileAsyncInternal(reader, predicate, cancellationToken);
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
                ? default!
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
                    ? default!
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
                    ? default!
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
                    ? default!
                    : reader.GetFieldValue<T0>(0)
                );
            }
        }

        return results.DequeueEach();
    }
}
