using System.Diagnostics;

namespace Open.Database.Extensions;

/// <summary>
/// Provides extension methods that read rows and result sets from an <see cref="IDataReader"/> or <see cref="DbDataReader"/>.
/// </summary>
#pragma warning disable IDE0079 // Remove unnecessary suppression
[SuppressMessage("Design", "CA1068:CancellationToken parameters must come last", Justification = "Overload provided for convienience.")]
[SuppressMessage("Reliability", "CA2016:Forward the 'CancellationToken' parameter to methods that take one", Justification = "Intentional to prevent cancellation exception.")]
#pragma warning restore IDE0079 // Remove unnecessary suppression
public static class DataReaderExtensions
{

	/// <summary>
	/// Calls a handler for each remaining record of the current result set of an <see cref="IDataReader"/>;
	/// the reader's connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// The token is checked before each read:
	/// a cancel throws, and the read is not made.
	/// A cancel during the handler of a row lets that row finish,
	/// then throws at the next check, even if that row was the last.
	/// Once a read has returned no row the result is complete,
	/// and a cancel after that is not an error.
	/// Rows already handed out stay handed out.
	/// To stop early without an exception,
	/// use <see cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, CancellationToken)"/>
	/// and return <see langword="false"/>.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="handler"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled.
	/// </exception>
	public static void ForEach(
		this IDataReader reader,
		Action<IDataRecord> handler,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (handler is null) throw new ArgumentNullException(nameof(handler));
		Contract.EndContractBlock();

		if (cancellationToken.CanBeCanceled)
		{
			while (ReadNext(reader, cancellationToken))
				handler(reader);
		}
		else
		{
			while (reader.Read())
				handler(reader);
		}
	}

	/// <summary>
	/// Reads the next row after checking the token:
	/// a canceled token throws before the read is made.
	/// </summary>
	/// <remarks>
	/// Once a read has returned no row the result is complete,
	/// and the token is not looked at again.
	/// </remarks>
	/// <param name="reader">The reader to read from.</param>
	/// <param name="cancellationToken">The token to check before the read.</param>
	/// <returns>
	/// <see langword="true"/> if a row was read; otherwise, <see langword="false"/>.
	/// </returns>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled.
	/// </exception>
	static bool ReadNext(
		IDataReader reader,
		CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();
		return reader.Read();
	}

	/// <summary>
	/// Asynchronously calls a handler for each remaining record of the current result set of a <see cref="DbDataReader"/>;
	/// the reader's connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// The token is passed to <c>ReadAsync</c>, so the provider
	/// cancels the read itself. When <paramref name="useReadAsync"/>
	/// is <see langword="false"/> it is checked before each <c>Read</c> instead.
	/// A cancel throws, and rows already handed out stay handed out.
	/// A cancel during the handler of a row throws at the next read,
	/// even if that row was the last.
	/// Once a read has returned no row the result is complete,
	/// and a cancel after that is not an error.
	/// </para>
	/// </remarks>
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous operation.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="handler"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled and cut the read short.
	/// </exception>
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
			while (await reader
				.ReadAsync(cancellationToken)
				.ConfigureAwait(false))
			{
				handler(reader);
			}
		}
		else
		{
			ForEach(reader, handler, cancellationToken);
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
			while (await reader
				.ReadAsync(cancellationToken)
				.ConfigureAwait(false))
			{
				await handler(reader).ConfigureAwait(false);
			}
		}
		else
		{
			while (ReadNext(reader, cancellationToken))
				await handler(reader).ConfigureAwait(false);
		}
	}

	/// <inheritdoc cref="ForEachAsync(DbDataReader, Action{IDataRecord}, bool, CancellationToken)" path="/*[not(self::param)]"/>
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='handler']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
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

	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, IEnumerable{int})" path="/*[not(self::param) and not(self::exception) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader)
		=> reader is null
			? throw new ArgumentNullException(nameof(reader))
			: AsEnumerableCore(reader);

	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
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

	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, IEnumerable{int})" path="/*[not(self::param) and not(self::exception) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="ordinals"/> is <see langword="null"/>.
	/// </exception>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals)
		=> AsEnumerableInternal(reader, ordinals, false);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, IEnumerable{int})" path="/*[not(self::param) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// </remarks>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, int n, params IEnumerable<int> others)
		=> AsEnumerableInternal(reader, others.Prepend(n), false);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <inheritdoc cref="AsEnumerable(IDataReader, ArrayPool{object?}, int, IEnumerable{int})" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="ordinals"/> is <see langword="null"/>.
	/// </exception>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, IEnumerable<int> ordinals, ArrayPool<object>? arrayPool)
		=> AsEnumerableInternal(reader, ordinals, false, arrayPool);

	/// <summary>
	/// Enumerates the remaining records of the current result set of a data reader as arrays of values;
	/// the reader's connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='row-pool']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <returns>
	/// An enumerable of arrays, one for each record, holding its values.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static IEnumerable<object[]> AsEnumerable(this IDataReader reader, ArrayPool<object>? arrayPool, int n, params IEnumerable<int> others)
		=> AsEnumerable(reader, others.Prepend(n), arrayPool);

	/// <inheritdoc cref="Select{T}(IDataReader, Func{IDataRecord, T}, CancellationToken)" path="/*[not(self::param) and not(self::remarks) and not(self::exception)]"/>
	/// <remarks>
	/// The iteration is deferred: nothing is read until the result is enumerated.
	/// Rows already handed out stay handed out.
	/// To stop early, stop enumerating.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform']/*" />
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="transform"/> is <see langword="null"/>.
	/// </exception>
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
	/// Lazily transforms each remaining record of an <see cref="IDataReader"/> through a function.
	/// </summary>
	/// <remarks>
	/// The iteration is deferred: nothing is read,
	/// and a canceled token does not throw,
	/// until the result is enumerated.
	/// The token is checked before each read:
	/// a cancel throws from the enumeration, and the read is not made.
	/// A cancel while a row is being yielded or transformed
	/// lets that row finish, then throws at the next check,
	/// even if that row was the last.
	/// Once a read has returned no row the result is complete,
	/// and a cancel after that is not an error.
	/// Rows already handed out stay handed out.
	/// To stop early without an exception, stop enumerating.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <returns>An enumerable of the transformed results.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="transform"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled (thrown by the enumeration).
	/// </exception>
	public static IEnumerable<T> Select<T>(
		this IDataReader reader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken)
	{
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectCore(reader, transform, cancellationToken);

		static IEnumerable<T> SelectCore(
			IDataReader reader,
			Func<IDataRecord, T> transform,
			CancellationToken cancellationToken)
		{
			if (cancellationToken.CanBeCanceled)
			{
				while (ReadNext(reader, cancellationToken))
					yield return transform(reader);
			}
			else
			{
				while (reader.Read())
					yield return transform(reader);
			}
		}
	}

	/// <inheritdoc
	/// cref="Select{T}(IDataReader, Func{IDataRecord, T}, CancellationToken)"/>
	public static IEnumerable<T> Select<T>(
		this IDataReader reader,
		CancellationToken cancellationToken,
		Func<IDataRecord, T> transform)
		=> Select(reader, transform, cancellationToken);

	static async IAsyncEnumerable<object[]> AsAsyncEnumerableCore(DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
	{
		Contract.EndContractBlock();

		if (cancellationToken.IsCancellationRequested
		|| !await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false))
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
			&& await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false));
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

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)" path="/*[not(self::param) and not(self::exception) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <para>
	/// A canceled token ends the enumeration without an exception,
	/// and rows already yielded stay yielded.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(
		this DbDataReader reader,
		CancellationToken cancellationToken = default)
		=> reader is null
			? throw new ArgumentNullException(nameof(reader))
			: AsAsyncEnumerableCore(reader, cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
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
			if (!readStarted && (cancellationToken.IsCancellationRequested || !await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false)))
				yield break;

			IList<int> o = ordinals as IList<int> ?? ordinals.ToArray();
			int fieldCount = o.Count;
			if (fieldCount == 0)
			{
				do
				{
					yield return Array.Empty<object>();
				}
				while (!cancellationToken.IsCancellationRequested
					&& await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false));
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
				while (!cancellationToken.IsCancellationRequested
					&& await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false));
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
			|| !await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false)))
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
				&& await reader.ReadAsync(cancellationToken).CancelledAsFalse().ConfigureAwait(false));
		}
	}

	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)" path="/*[not(self::param) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <para>
	/// A canceled token ends the enumeration without an exception,
	/// and rows already yielded stay yielded.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, CancellationToken cancellationToken = default)
		=> AsAsyncEnumerableInternal(reader, ordinals, false, cancellationToken);

	/// <summary>
	/// Asynchronously enumerates the remaining records of the current result set of a <see cref="DbDataReader"/> as arrays of values;
	/// the reader's connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='row-pool']/*" />
	/// <para>
	/// A canceled token ends the enumeration without an exception,
	/// and rows already yielded stay yielded.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='ordinals-limited']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// An asynchronous enumerable of arrays, one for each record,
	/// holding the values of the requested ordinals.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="ordinals"/> is <see langword="null"/>.
	/// </exception>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, IEnumerable<int> ordinals, ArrayPool<object>? arrayPool, CancellationToken cancellationToken = default)
		=> arrayPool is null
		? AsAsyncEnumerableInternal(reader, ordinals, false, cancellationToken)
		: AsAsyncEnumerableInternal(reader, ordinals, false, arrayPool, cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)" path="/*[not(self::param) and not(self::exception) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <para>
	/// A canceled token ends the enumeration without an exception,
	/// and rows already yielded stay yielded.
	/// </para>
	/// </remarks>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, CancellationToken cancellationToken, int n, params IEnumerable<int> others)
		=> AsAsyncEnumerable(reader, others.Prepend(n), cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, IEnumerable{int}, ArrayPool{object?}, CancellationToken)" path="/*[not(self::param) and not(self::exception)]"/>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="others"/> is <see langword="null"/>.
	/// </exception>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object>? arrayPool, CancellationToken cancellationToken, int n, params IEnumerable<int> others)
		=> AsAsyncEnumerable(reader, others.Prepend(n), arrayPool, cancellationToken);

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, ArrayPool{object?}, CancellationToken, int, IEnumerable{int})" path="/*[not(self::param) and not(self::remarks)]"/>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// <see cref="DBNull"/> values are retained.
	/// </para>
	/// <para>
	/// A canceled token ends the enumeration without an exception,
	/// and rows already yielded stay yielded.
	/// </para>
	/// </remarks>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, int n, params IEnumerable<int> others)
		=> AsAsyncEnumerable(reader, others.Prepend(n));

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-enumerate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='arrayPool']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='n-others']/*" />
	/// <inheritdoc cref="AsAsyncEnumerable(DbDataReader, ArrayPool{object?}, CancellationToken, int, IEnumerable{int})" path="/*[not(self::param)]"/>
	public static IAsyncEnumerable<object[]> AsAsyncEnumerable(this DbDataReader reader, ArrayPool<object>? arrayPool, int n, params IEnumerable<int> others)
		=> AsAsyncEnumerable(reader, others.Prepend(n), arrayPool);

	/// <summary>
	/// Asynchronously and lazily transforms each remaining record of a <see cref="DbDataReader"/> through a function.
	/// </summary>
	/// <remarks>
	/// The iteration is deferred until the result is enumerated.
	/// The token is passed to <c>ReadAsync</c>,
	/// so the provider cancels the read itself
	/// (a plain <see cref="IDataReader"/> is checked before each <c>Read</c>).
	/// A cancel throws from the enumeration.
	/// A cancel while a row is being yielded or transformed
	/// throws at the next read, even if that row was the last.
	/// Once a read has returned no row the result is complete,
	/// and a cancel after that is not an error.
	/// Rows already handed out stay handed out.
	/// To stop early without an exception, leave the loop.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>An asynchronous enumerable of the transformed results.</returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="transform"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled (thrown by the enumeration).
	/// </exception>
	public static IAsyncEnumerable<T> SelectAsync<T>(this DbDataReader reader,
		Func<IDataRecord, T> transform,
		CancellationToken cancellationToken = default)
	{
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectAsyncCore(reader, transform, cancellationToken);

		static async IAsyncEnumerable<T> SelectAsyncCore(
			DbDataReader reader,
			Func<IDataRecord, T> transform,
			[EnumeratorCancellation] CancellationToken cancellationToken)
		{
			while (await reader
				.ReadAsync(cancellationToken)
				.ConfigureAwait(false))
			{
				yield return transform(reader);
			}
		}
	}

	/// <inheritdoc cref="SelectAsync{T}(DbDataReader, Func{IDataRecord, T}, CancellationToken)" path="/*[not(self::param)]"/>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <param name="transform">
	/// The asynchronous function to transform each <see cref="IDataRecord"/> into a result.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	public static IAsyncEnumerable<T> SelectAsync<T>(this IDataReader reader,
		Func<IDataRecord, ValueTask<T>> transform,
		CancellationToken cancellationToken = default)
	{
		return reader is null
			? throw new ArgumentNullException(nameof(reader))
			: transform is null
			? throw new ArgumentNullException(nameof(transform))
			: SelectAsyncCore(reader, transform, cancellationToken);

		static async IAsyncEnumerable<T> SelectAsyncCore(
			IDataReader reader,
			Func<IDataRecord, ValueTask<T>> transform,
			[EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (reader is DbDataReader r)
			{
				while (await r
					.ReadAsync(cancellationToken)
					.ConfigureAwait(false))
				{
					yield return await transform(r).ConfigureAwait(false);
				}
			}
			else
			{
				while (ReadNext(reader, cancellationToken))
					yield return await transform(reader).ConfigureAwait(false);
			}
		}
	}

	/// <summary>
	/// Asynchronously iterates the remaining records of the current result set and returns the results of an asynchronous transform as a list.
	/// </summary>
	/// <include file="Readers.docs.xml" path="docs/members[@name='transform-T']/*" />
	/// <param name="reader">The <see cref="DbDataReader"/> to read from.</param>
	/// <param name="transform">
	/// The asynchronous function to transform each <see cref="IDataRecord"/> into a result.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is a list of the transformed results.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="transform"/> is <see langword="null"/>.
	/// </exception>
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
	/// Loads the current result set of an <see cref="IDataReader"/> into a <see cref="DataTable"/>.
	/// The reader stays open and is left on that result set, so <see cref="IDataReader.NextResult"/> moves to the next one.
	/// </summary>
	/// <remarks>
	/// <see cref="DataTable.Load(IDataReader)"/> asks the reader for the next result set and closes it after the last one.
	/// A forwarder makes both a no-op, so the caller's <c>NextResult</c> still decides where the reader goes.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <returns>A <see cref="DataTable"/> that holds the result set.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static DataTable ToDataTable(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var table = new DataTable();
		// The forwarder keeps Load from moving or closing the reader.
		table.Load(new SingleResultSetReader(reader));
		return table;
	}

	/// <summary>
	/// Loads every result set of an <see cref="IDataReader"/>, from the current one on, into its own <see cref="DataTable"/>.
	/// </summary>
	/// <remarks>
	/// It calls <see cref="IDataReader.NextResult"/> to check for more results, so the reader is left past the last one.
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <returns>A list of tables, one for each result set.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
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
	/// Hands <see cref="DataTable.Load(IDataReader)"/> one result set of a reader without letting it move the real reader.
	/// </summary>
	/// <remarks>
	/// <c>Load</c> calls <c>NextResult</c> after the set and <c>Close</c> after the last one.
	/// Here <c>NextResult</c> says there is no more
	/// (returning <see langword="true"/> would make <c>Load</c> loop forever on a result set without columns)
	/// and <c>Close</c> does nothing,
	/// so the caller's <c>NextResult</c> and <c>Close</c> still work.
	/// Everything else goes to the real reader.
	/// </remarks>
	/// <param name="reader">The reader to forward to.</param>
	private sealed class SingleResultSetReader(IDataReader reader) : IDataReader
	{
		public object this[int i] => reader[i];
		public object this[string name] => reader[name];
		public int Depth => reader.Depth;
		public bool IsClosed => reader.IsClosed;
		public int RecordsAffected => reader.RecordsAffected;
		public int FieldCount => reader.FieldCount;
		public void Close() { }
		public void Dispose() { }
		public bool NextResult() => false;
		public bool Read() => reader.Read();
		public DataTable? GetSchemaTable() => reader.GetSchemaTable();
		public bool GetBoolean(int i) => reader.GetBoolean(i);
		public byte GetByte(int i) => reader.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length) => reader.GetBytes(i, fieldOffset, buffer, bufferOffset, length);
		public char GetChar(int i) => reader.GetChar(i);
		public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length) => reader.GetChars(i, fieldOffset, buffer, bufferOffset, length);
		public IDataReader GetData(int i) => reader.GetData(i);
		public string GetDataTypeName(int i) => reader.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => reader.GetDateTime(i);
		public decimal GetDecimal(int i) => reader.GetDecimal(i);
		public double GetDouble(int i) => reader.GetDouble(i);
		public Type GetFieldType(int i) => reader.GetFieldType(i);
		public float GetFloat(int i) => reader.GetFloat(i);
		public Guid GetGuid(int i) => reader.GetGuid(i);
		public short GetInt16(int i) => reader.GetInt16(i);
		public int GetInt32(int i) => reader.GetInt32(i);
		public long GetInt64(int i) => reader.GetInt64(i);
		public string GetName(int i) => reader.GetName(i);
		public int GetOrdinal(string name) => reader.GetOrdinal(name);
		public string GetString(int i) => reader.GetString(i);
		public object GetValue(int i) => reader.GetValue(i);
		public int GetValues(object[] values) => reader.GetValues(values);
		public bool IsDBNull(int i) => reader.IsDBNull(i);
	}

	/// <summary>
	/// Iterates the remaining records of the current result set of an <see cref="IDataReader"/> while a predicate returns <see langword="true"/>;
	/// the reader's connection is held while each row is processed.
	/// </summary>
	/// <remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='held']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='held-reader']/*" />
	/// <para>
	/// The token is checked before each read:
	/// a cancel throws, and the read is not made.
	/// A cancel during the predicate of a row lets that row finish,
	/// then throws at the next check, even if that row was the last.
	/// Once a read has returned no row the result is complete,
	/// and a cancel after that is not an error.
	/// Rows already handed out stay handed out.
	/// To stop early, return <see langword="false"/> from the predicate:
	/// that ends the iteration without an exception.
	/// </para>
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='predicate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <exception cref="ArgumentNullException">
	/// <paramref name="reader"/> or <paramref name="predicate"/> is <see langword="null"/>.
	/// </exception>
	/// <exception cref="OperationCanceledException">
	/// The token was canceled.
	/// </exception>
	public static void IterateWhile(
		this IDataReader reader,
		Func<IDataRecord, bool> predicate,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		if (cancellationToken.CanBeCanceled)
		{
			while (ReadNext(reader, cancellationToken))
			{
				if (!predicate(reader)) break;
			}
		}
		else
		{
			while (reader.Read() && predicate(reader)) { }
		}
	}

	/// <returns>A task that represents the asynchronous iteration.</returns>
	/// <inheritdoc
	/// cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, CancellationToken)"/>
	public static async ValueTask IterateWhileAsync(
		this DbDataReader reader,
		Func<IDataRecord, bool> predicate,
		CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		while (await reader
			.ReadAsync(cancellationToken)
			.ConfigureAwait(false)
			&& predicate(reader)) { }
	}

	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='predicate']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync-required']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous iteration.</returns>
	/// <inheritdoc
	/// cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, CancellationToken)"
	/// path="/*[not(self::param)]"/>
	public static async ValueTask IterateWhileAsync(this DbDataReader reader, Func<IDataRecord, bool> predicate, bool useReadAsync, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		if (useReadAsync)
		{
			while (await reader
				.ReadAsync(cancellationToken)
				.ConfigureAwait(false)
				&& predicate(reader)) { }
		}
		else
		{
			while (ReadNext(reader, cancellationToken))
			{
				if (!predicate(reader)) break;
			}
		}
	}

	static async ValueTask IterateWhileAsyncInternal(IDataReader reader, Func<IDataRecord, ValueTask<bool>> predicate, CancellationToken cancellationToken)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		if (predicate is null) throw new ArgumentNullException(nameof(predicate));
		Contract.EndContractBlock();

		while (ReadNext(reader, cancellationToken))
		{
			if (!await predicate(reader).ConfigureAwait(false)) break;
		}
	}

	/// <returns>A task that represents the asynchronous iteration.</returns>
	/// <inheritdoc
	/// cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, CancellationToken)"/>
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
			while (await r
				.ReadAsync(cancellationToken)
				.ConfigureAwait(false)
				&& await predicate(reader).ConfigureAwait(false)) { }
		}
		else
		{
			// Does not use .ReadAsync();
			await IterateWhileAsyncInternal(reader, predicate, cancellationToken).ConfigureAwait(false);
		}
	}

	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <param name="predicate">
	/// The asynchronous function to call for each <see cref="IDataRecord"/>.
	/// It returns <see langword="true"/> to continue with the next record,
	/// or <see langword="false"/> to stop.
	/// </param>
	/// <param name="useReadAsync">
	/// <see langword="true"/> to read the rows with <c>ReadAsync</c>, so the
	/// provider can cancel a read itself (a reader that is not a
	/// <see cref="DbDataReader"/> is always read with <c>Read</c>);
	/// <see langword="false"/> to read them with <c>Read</c>, checking the
	/// token before each read.
	/// </param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>A task that represents the asynchronous iteration.</returns>
	/// <inheritdoc
	/// cref="IterateWhile(IDataReader, Func{IDataRecord, bool}, CancellationToken)"
	/// path="/*[not(self::param)]"/>
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
	/// Reads the first column of every remaining record of the current result set.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <returns>An enumerable of the values of the first column.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static IEnumerable<object?> FirstOrdinalResults(this IDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		// Buffer the first-column values so the reader/connection can be released before the
		// (deferred) DBNull conversion runs during enumeration.
		var results = new Queue<object>();
		while (reader.Read())
			results.Enqueue(reader.GetValue(0));
		return results.DequeueEach().DBNullToNull();
	}

	/// <summary>
	/// Reads the first column of every remaining record of the current result set as type <typeparamref name="T0"/>.
	/// </summary>
	/// <remarks>
	/// <para>
	/// If the reader is a <see cref="DbDataReader"/>, a database null
	/// becomes <see langword="default"/> for <typeparamref name="T0"/>;
	/// otherwise <see cref="DBNull"/> values are converted to
	/// <see langword="null"/> and then cast to <typeparamref name="T0"/>.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <typeparam name="T0">The expected type of the first column.</typeparam>
	/// <include file="Readers.docs.xml" path="docs/members[@name='reader-iterate']/*" />
	/// <returns>An enumerable of the values of the first column, cast to <typeparamref name="T0"/>.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this IDataReader reader)
		=> reader is DbDataReader dbr
		? dbr.FirstOrdinalResults<T0>()
		: reader.FirstOrdinalResults().Cast<T0>();

	/// <summary>
	/// Reads the first column of every remaining record of the current result set as type <typeparamref name="T0"/>.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A database null becomes <see langword="default"/> for <typeparamref name="T0"/>.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <typeparam name="T0">The expected type of the first column.</typeparam>
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <returns>An enumerable of the values of the first column, as <typeparamref name="T0"/>.</returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static IEnumerable<T0> FirstOrdinalResults<T0>(this DbDataReader reader)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var results = new Queue<T0>();
		while (reader.Read())
			results.Enqueue(ColumnReader.Get<T0>(reader, 0));

		return results.DequeueEach();
	}

	/// <summary>
	/// Asynchronously reads the first column of every remaining record of the current result set.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="DBNull"/> values are converted to <see langword="null"/>.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is an enumerable of the values of the first column.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static async ValueTask<IEnumerable<object?>> FirstOrdinalResultsAsync(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var results = new Queue<object>();
		await reader.ForEachAsync(r => results.Enqueue(r.GetValue(0)), useReadAsync, cancellationToken).ConfigureAwait(false);
		return results.DequeueEach().DBNullToNull();
	}

	/// <summary>
	/// Asynchronously reads the first column of every remaining record of the current result set as type <typeparamref name="T0"/>.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A database null becomes <see langword="default"/> for <typeparamref name="T0"/>.
	/// </para>
	/// <include file="Readers.docs.xml" path="docs/members[@name='buffered-once']/*" />
	/// </remarks>
	/// <typeparam name="T0">The expected type of the first column.</typeparam>
	/// <param name="reader">The <see cref="DbDataReader"/> to iterate.</param>
	/// <include file="Readers.docs.xml" path="docs/members[@name='useReadAsync']/*" />
	/// <include file="Readers.docs.xml" path="docs/members[@name='cancellationToken']/*" />
	/// <returns>
	/// A task that represents the asynchronous operation.
	/// Its result is an enumerable of the values of the first column, as <typeparamref name="T0"/>.
	/// </returns>
	/// <include file="Readers.docs.xml" path="docs/members[@name='null-reader']/*" />
	public static async ValueTask<IEnumerable<T0>> FirstOrdinalResultsAsync<T0>(this DbDataReader reader, bool useReadAsync = true, CancellationToken cancellationToken = default)
	{
		if (reader is null) throw new ArgumentNullException(nameof(reader));
		Contract.EndContractBlock();

		var results = new Queue<T0>();
		if (useReadAsync)
		{
			while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
			{
				// Genuinely asynchronous column reads (matters for large values under SequentialAccess).
				results.Enqueue(
					await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false)
					? default!
					: await reader.GetFieldValueAsync<T0>(0, cancellationToken).ConfigureAwait(false));
			}
		}
		else if (cancellationToken.CanBeCanceled)
		{
			while (!cancellationToken.IsCancellationRequested && reader.Read())
				results.Enqueue(ColumnReader.Get<T0>(reader, 0));

			cancellationToken.ThrowIfCancellationRequested();
		}
		else
		{
			while (reader.Read())
				results.Enqueue(ColumnReader.Get<T0>(reader, 0));
		}

		return results.DequeueEach();
	}
}
