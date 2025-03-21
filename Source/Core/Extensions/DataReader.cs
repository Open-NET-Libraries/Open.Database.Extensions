﻿using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// Extension methods for Data Readers.
/// </summary>
#pragma warning disable IDE0079 // Remove unnecessary suppression
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload provided for convienience.")]
[SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional to prevent cancellation exception.")]
#pragma warning restore IDE0079 // Remove unnecessary suppression
public static class DataReaderExtensions
{
	/// <summary>
	/// Iterates all records from an <see cref="IDataReader"/>.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
	/// <param name="throwOnCancellation">If true, when canceled, may exit the iteration via an exception. Otherwise when canceled will simply stop iterating and return without exception.</param>
	/// <param name="cancellationToken">An optional cancellation token for stopping the iteration.</param>
	public static void ForEach(
		this IDataReader reader,
		Action<IDataRecord> handler,
		bool throwOnCancellation,
		CancellationToken cancellationToken = default)
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
			bool cancelled = false;
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

	/// <inheritdoc cref="ForEach(IDataReader, Action{IDataRecord}, bool, CancellationToken)"/>
	public static void ForEach(
		this IDataReader reader,
		Action<IDataRecord> handler,
		CancellationToken cancellationToken = default)
		=> ForEach(reader, handler, false, cancellationToken);

	/// <summary>
	/// Iterates all records from an <see cref="DbDataReader"/>.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="handler">The handler function for each <see cref="IDataRecord"/>.</param>
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
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
				handler(reader);
		}
		else
		{
			ForEach(reader, handler, true, cancellationToken);
		}
	}

	/// <inheritdoc cref="ForEachAsync(DbDataReader, Action{IDataRecord}, bool, CancellationToken)"/>
	public static async ValueTask ForEachAsync(
		this DbDataReader reader,
		Func<IDataRecord, ValueTask> handler,
		bool useReadAsync = true,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		if (useReadAsync)
		{
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
				await handler(reader).ConfigureAwait(false);
		}
		else if (cancellationToken.CanBeCanceled)
		{
			cancellationToken.ThrowIfCancellationRequested();

			// The following pattern allows for the reader to complete if it actually reached the end before cancellation.
			bool cancelled = false;
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

	/// <inheritdoc cref="ForEachAsync(DbDataReader, Action{IDataRecord}, bool, CancellationToken)"/>
	public static ValueTask ForEachAsync(this DbDataReader reader, Func<IDataRecord, ValueTask> handler, CancellationToken cancellationToken)
		=> ForEachAsync(reader, handler, true, cancellationToken);

	static IEnumerable<object[]> AsEnumerableCore(IDataReader reader)
	{
		Debug.Assert(reader is not null);

		if (!reader.Read())
			yield break;

		int fieldCount = reader.FieldCount;
		do
		{
			object[] row = new object[fieldCount];
			reader.GetValues(row);
			yield return row;
		} while (reader.Read());
	}

	static IEnumerable<object[]> AsEnumerableCore(IDataReader reader, ArrayPool<object> arrayPool)
	{
		Debug.Assert(reader is not null);
		Debug.Assert(arrayPool is not null);

		if (!reader.Read())
			yield break;

		int fieldCount = reader.FieldCount;
		do
		{
			object[] row = arrayPool.Rent(fieldCount);
			reader.GetValues(row);
			yield return row;
		} while (reader.Read());
	}

	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader)
		=> reader is null
			? throw new ArgumentNullException(nameof(reader))
			: AsEnumerableCore(reader);

	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, ArrayPool<object>? arrayPool)
		=> reader is null
			? throw new ArgumentNullException(nameof(reader))
			: arrayPool is null
			? AsEnumerableCore(reader)
			: AsEnumerableCore(reader, arrayPool);

	static IEnumerable<object[]> AsEnumerableInternalCore(
		IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
	{
		Debug.Assert(reader is not null);
		Debug.Assert(ordinals is not null);

		if (!readStarted && !reader.Read())
			yield break;

		IList<int> o = ordinals as IList<int> ?? ordinals.ToArray();
		int fieldCount = o.Count;
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
			object[] row = new object[fieldCount];
			for (int i = 0; i < fieldCount; i++)
				row[i] = reader.GetValue(o[i]);
			yield return row;
		}
		while (reader.Read());
	}

	static IEnumerable<object[]> AsEnumerableInternalCore(
		IDataReader reader, IEnumerable<int> ordinals, bool readStarted, ArrayPool<object> arrayPool)
	{
		Debug.Assert(reader is not null);
		Debug.Assert(ordinals is not null);
		Debug.Assert(arrayPool is not null);

		if (!readStarted && !reader.Read())
			yield break;

		IList<int> o = ordinals as IList<int> ?? ordinals.ToArray();
		int fieldCount = o.Count;
		do
		{
			object[] row = arrayPool.Rent(fieldCount);
			for (int i = 0; i < fieldCount; i++)
				row[i] = reader.GetValue(o[i]);
			yield return row;
		}
		while (reader.Read());
	}

	internal static IEnumerable<object[]> AsEnumerableInternal(
		this IDataReader reader,
		IEnumerable<int> ordinals,
		bool readStarted)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
		Contract.EndContractBlock();

		return AsEnumerableInternalCore(reader, ordinals, readStarted);
	}

	internal static IEnumerable<object[]> AsEnumerableInternal(
		this IDataReader reader,
		IEnumerable<int> ordinals,
		bool readStarted,
		ArrayPool<object>? arrayPool)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
		Contract.EndContractBlock();

		return arrayPool is null
			? AsEnumerableInternalCore(reader, ordinals, readStarted)
			: AsEnumerableInternalCore(reader, ordinals, readStarted, arrayPool);
	}

	/// <inheritdoc cref="AsEnumerable(IDataReader, IEnumerable{int}, ArrayPool{object?})"/>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals)
		=> AsEnumerableInternal(reader, ordinals, false);

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
#if NET8_0_OR_GREATER
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params IEnumerable<int> others)
		=> AsEnumerableInternal(reader, others.Prepend(n), false);
#else

	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params int[] others)
		=> AsEnumerable(reader, CoreExtensions.Concat(n, others));
#endif

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals, ArrayPool<object>? arrayPool)
		=> AsEnumerableInternal(reader, ordinals, false, arrayPool);

	/// <summary>
	/// Provides an enumerable for iterating all the remaining values of the current result set of a data reader.
	/// </summary>
	/// <remarks><see cref="DBNull"/> values are retained.</remarks>
	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <returns>An enumerable of the values returned from a data reader.</returns>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, ArrayPool<object>? arrayPool, int n, params int[] others)
		=> AsEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool);

	/// <inheritdoc cref="Select{T}(IDataReader, Func{IDataRecord, T}, CancellationToken, bool)"/>
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
	/// Iterates records from an <see cref="IDataReader"/> and passes the IDataRecord to a transform function.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The reader to iterate.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="cancellationToken">A cancellation token for stopping the iteration.</param>
	/// <param name="throwOnCancellation">If true, when canceled, may exit the iteration via an exception. Otherwise when canceled will simply stop iterating and return without exception.</param>
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
				bool cancelled = false;
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

	/// <inheritdoc cref="Select{T}(IDataReader, Func{IDataRecord, T}, CancellationToken, bool)"/>
	public static IEnumerable<T> Select<T>(this IDataReader reader, CancellationToken cancellationToken, Func<IDataRecord, T> transform, bool throwOnCancellation = false)
		=> Select(reader, transform, cancellationToken, throwOnCancellation);

#if NETSTANDARD2_0
#else

	static async IAsyncEnumerable<object[]> AsAsyncEnumerableCore(DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
	{
		Contract.EndContractBlock();

		if (cancellationToken.IsCancellationRequested
		|| !await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
		{
			yield break;
		}

		int fieldCount = reader.FieldCount;
		do
		{
			object[] row = new object[fieldCount];
			reader.GetValues(row);
			yield return row;
		}
		while (!cancellationToken.IsCancellationRequested
			&& await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false));
	}

	static async IAsyncEnumerable<object[]> AsAsyncEnumerableCore(DbDataReader reader, ArrayPool<object> arrayPool, [EnumeratorCancellation] CancellationToken cancellationToken)
	{
		if (cancellationToken.IsCancellationRequested || !await reader.ReadAsync().ConfigureAwait(false))
			yield break;

		int fieldCount = reader.FieldCount;
		do
		{
			object[] row = arrayPool.Rent(fieldCount);
			reader.GetValues(row);
			yield return row;
		}
		while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync().ConfigureAwait(false));
	}

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="cancellationToken">Optional iteration cancellation token.</param>
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(
		this DbDataReader reader,
		CancellationToken cancellationToken = default)
		=> reader is null
			? throw new ArgumentNullException(nameof(reader))
			: AsAsyncEnumerableCore(reader, cancellationToken);

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="arrayPool">An optional array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">Optional iteration cancellation token.</param>
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(
		this DbDataReader reader,
		ArrayPool<object>? arrayPool,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		return arrayPool is null
			? AsAsyncEnumerableCore(reader, cancellationToken)
			: AsAsyncEnumerableCore(reader, arrayPool, cancellationToken);
	}

	static IAsyncEnumerable<object[]> AsAsyncEnumerableInternal(
		this DbDataReader reader,
		IEnumerable<int> ordinals,
		bool readStarted,
		CancellationToken cancellationToken)
	{
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: ordinals is null
			? throw new ArgumentNullException(nameof(ordinals))
			: AsAsyncEnumerableInternalCore(reader, ordinals, readStarted, cancellationToken);

		static async IAsyncEnumerable<object[]> AsAsyncEnumerableInternalCore(DbDataReader reader, IEnumerable<int> ordinals, bool readStarted, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (!readStarted && (cancellationToken.IsCancellationRequested || !await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false)))
				yield break;

			IList<int> o = ordinals as IList<int> ?? ordinals.ToArray();
			int fieldCount = o.Count;
			if (fieldCount == 0)
			{
				do
				{
					yield return Array.Empty<object>();
				}
				while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false));
			}
			else
			{
				do
				{
					object[] row = new object[fieldCount];
					for (int i = 0; i < fieldCount; i++)
						row[i] = reader.GetValue(o[i]);
					yield return row;
				}
				while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false));
			}
		}
	}

	static IAsyncEnumerable<object[]> AsAsyncEnumerableInternal(
		this DbDataReader reader,
		IEnumerable<int> ordinals,
		bool readStarted,
		ArrayPool<object> arrayPool,
		CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
		Debug.Assert(arrayPool is not null);
		Contract.EndContractBlock();

		return AsAsyncEnumerableInternalCore(reader, ordinals, readStarted, arrayPool, cancellationToken);

		static async IAsyncEnumerable<object[]> AsAsyncEnumerableInternalCore(
			DbDataReader reader,
			IEnumerable<int> ordinals,
			bool readStarted,
			ArrayPool<object> arrayPool,
			[EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (!readStarted && (cancellationToken.IsCancellationRequested
			|| !await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false)))
			{
				yield break;
			}

			IList<int> o = ordinals as IList<int> ?? ordinals.ToArray();
			int fieldCount = o.Count;
			do
			{
				object[] row = arrayPool.Rent(fieldCount);
				for (int i = 0; i < fieldCount; i++)
					row[i] = reader.GetValue(o[i]);
				yield return row;
			}
			while (!cancellationToken.IsCancellationRequested
				&& await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false));
		}
	}

	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, CancellationToken cancellationToken = default)
		=> AsAsyncEnumerableInternal(reader, ordinals, false, cancellationToken);

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="ordinals">The limited set of ordinals to include.  If none are specified, the returned objects will be empty.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">Optional iteration cancellation token.</param>
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, int[])"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, ArrayPool<object>? arrayPool, CancellationToken cancellationToken = default)
		=> arrayPool is null
		? AsAsyncEnumerableInternal(reader, ordinals, false, cancellationToken)
		: AsAsyncEnumerableInternal(reader, ordinals, false, arrayPool, cancellationToken);

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="cancellationToken">The iteration cancellation token.</param>
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}?, CancellationToken)"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, CancellationToken cancellationToken, int n, params int[] others)
		=> AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), cancellationToken);

	/// <param name="reader">The reader to enumerate.</param>
	/// <param name="arrayPool">The array pool to acquire buffers from.</param>
	/// <param name="cancellationToken">The iteration cancellation token.</param>
	/// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
	/// <param name="others">The remaining ordinals to request from the reader for each record.</param>
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object>? arrayPool, CancellationToken cancellationToken, int n, params int[] others)
		=> AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool, cancellationToken);

	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, ArrayPool{object?}, CancellationToken, int, int[])"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, int n, params int[] others)
		=> AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others));

	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, ArrayPool{object?}, CancellationToken, int, int[])"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object>? arrayPool, int n, params int[] others)
		=> AsAsyncEnumerable(reader, CoreExtensions.Concat(n, others), arrayPool);

	/// <summary>
	/// Asyncronously iterates all records from a data reader..]
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The DbDataReader to iterate.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="throwOnCancellation">If true, when canceled, may exit the iteration via an exception. Otherwise when canceled will simply stop iterating and return without exception.</param>
	/// <param name="cancellationToken">The cancellation token.</param>
	/// <returns>An enumerable used to iterate the results.</returns>
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

		static async IAsyncEnumerable<T> SelectAsyncCore(
			DbDataReader reader,
			Func<IDataRecord, T> transform,
			bool throwOnCancellation,
			[EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (throwOnCancellation)
			{
				cancellationToken.ThrowIfCancellationRequested();
				while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
					yield return transform(reader);
			}
			else
			{
				if (cancellationToken.IsCancellationRequested) yield break;
				while (!cancellationToken.IsCancellationRequested
					&& await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
				{
					yield return transform(reader);
				}
			}
		}
	}

	/// <inheritdoc cref="SelectAsync{T}(DbDataReader, Func{IDataRecord, T}, bool, CancellationToken)"/>
	public static IAsyncEnumerable<T> SelectAsync<T>(this DbDataReader reader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
		=> SelectAsync(reader, transform, false, cancellationToken);

	/// <inheritdoc cref="SelectAsync{T}(DbDataReader, Func{IDataRecord, T}, bool, CancellationToken)"/>
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
					while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
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
					while (!cancellationToken.IsCancellationRequested && await r.ReadAsync().ConfigureAwait(false))
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

	/// <inheritdoc cref="SelectAsync{T}(DbDataReader, Func{IDataRecord, T}, bool, CancellationToken)"/>
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
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A list of the transformed results.</returns>
	public static List<T> ToList<T>(this IDataReader reader,
		Func<IDataRecord, T> transform, CancellationToken cancellationToken = default)
		=> reader.Select(transform, cancellationToken).ToList();

	/// <summary>
	/// Asynchronously iterates all records using the data reader and returns the desired results as a list.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The SqlDataReader to read from.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <returns>A task containing a list of all results.</returns>
	public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
		Func<IDataRecord, T> transform, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var list = new List<T>();
		while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) list.Add(transform(reader));
		return list;
	}

	/// <inheritdoc cref="ToListAsync{T}(DbDataReader, Func{IDataRecord, T}, CancellationToken)"/>
	public static async ValueTask<List<T>> ToListAsync<T>(this DbDataReader reader,
		Func<IDataRecord, ValueTask<T>> transform, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (transform is null) throw new ArgumentNullException(nameof(transform));
		Contract.EndContractBlock();

		var list = new List<T>();
		while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) list.Add(await transform(reader).ConfigureAwait(false));
		return list;
	}

	/// <summary>
	/// Shortcut for .Select(transform).ToArray();
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <returns>An array of the transformed results.</returns>
	public static T[] ToArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
		=> reader.Select(transform).ToArray();

	/// <summary>
	/// Shortcut for .Select(transform).ToImmutableArray();
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="transform">The transform function to process each <see cref="IDataRecord"/>.</param>
	/// <returns>An immutable array of the transformed results.</returns>
	public static ImmutableArray<T> ToImmutableArray<T>(this IDataReader reader, Func<IDataRecord, T> transform)
		=> reader.Select(transform).ToImmutableArray();

	/// <summary>
	/// Loads all remaining data from an <see cref="IDataReader"/> into a DataTable.
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
	/// Loads all data from a command through an <see cref="IDataReader"/> into a DataTables.
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
	/// Iterates an <see cref="IDataReader"/> while the predicate returns true.
	/// </summary>
	/// <param name="reader">The <see cref="IDataReader"/> to iterate.</param>
	/// <param name="predicate">The handler function that processes each <see cref="IDataRecord"/> and decides if iteration should continue.</param>
	/// <param name="throwOnCancellation">If true, when canceled, may exit the iteration via an exception. Otherwise when canceled will simply stop iterating and return without exception.</param>
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
			bool cancelled = false;
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

	/// <inheritdoc cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, bool, CancellationToken)"/>
	public static void IterateWhile(this IDataReader reader, Func<IDataRecord, bool> predicate, CancellationToken cancellationToken = default)
		=> IterateWhile(reader, predicate, false, cancellationToken);

	/// <inheritdoc cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, bool, CancellationToken)"/>
	public static async ValueTask IterateWhileAsync(
		this DbDataReader reader,
		Func<IDataRecord, bool> predicate,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && predicate(reader)) { }
	}

	/// <param name="reader">The DbDataReader to load data from.</param>
	/// <param name="predicate">The handler function that processes each <see cref="IDataRecord"/> and decides if iteration should continue.</param>
	/// <param name="useReadAsync">If true will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results but still allowing cancellation.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <inheritdoc cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, bool, CancellationToken)"/>
	public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, bool useReadAsync, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		if (useReadAsync)
		{
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && predicate(reader)) { }
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
			bool cancelled = false;
			while (reader.Read() && await predicate(reader).ConfigureAwait(false))
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
			while (reader.Read() && await predicate(reader).ConfigureAwait(false)) { }
		}
	}

	/// <inheritdoc cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, bool, CancellationToken)"/>
	public static async ValueTask IterateWhileAsync(
		this IDataReader reader,
		Func<IDataRecord, ValueTask<bool>> predicate,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		if (reader is DbDataReader r)
		{
			while (await r.ReadAsync(cancellationToken).ConfigureAwait(false) && await predicate(reader).ConfigureAwait(false)) { }
		}
		else
		{
			// Does not use .ReadAsync();
			await IterateWhileAsyncInternal(reader, predicate, cancellationToken).ConfigureAwait(false);
		}
	}

	/// <param name="reader">The <see cref="IDataReader"/> to iterate.</param>
	/// <param name="predicate">The handler function that processes each <see cref="IDataRecord"/> and decides if iteration should continue.</param>
	/// <param name="useReadAsync">If true will iterate the results using .ReadAsync() otherwise will only Execute the reader asynchronously and then use .Read() to iterate the results.</param>
	/// <param name="cancellationToken">Optional cancellation token.</param>
	/// <inheritdoc cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, bool, CancellationToken)"/>
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
	/// <see cref="DBNull"/> values are then converted to null.
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
	/// Any<see cref="DBNull"/> values are then converted to null and casted to type T0;
	/// </summary>
	/// <returns>The enumerable of casted values.</returns>
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDataReader reader)
		=> reader is DbDataReader dbr
		? dbr.FirstOrdinalResults<T0>()
		: reader.FirstOrdinalResults().Cast<T0>();

	/// <summary>
	/// Reads the first column values from every record.
	/// Any<see cref="DBNull"/> values are then converted to null and casted to type T0;
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
	/// <see cref="DBNull"/> values are converted to null.
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
	/// Any<see cref="DBNull"/> values are then converted to null and casted to type T0;
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
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
