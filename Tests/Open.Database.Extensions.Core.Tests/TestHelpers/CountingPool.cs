using System.Buffers;
using System.Collections.Concurrent;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>
/// An <see cref="ArrayPool{T}"/> that counts what was rented and what came
/// back, counts a return of something that was not outstanding (a second
/// return of the same array) as unmatched, and can be told to give longer
/// arrays than asked for, to give shorter ones, or to throw when an array
/// is returned.
/// </summary>
/// <remarks>
/// <see cref="Return"/> does what a pool does with the flag: with
/// <c>clearArray: true</c> the whole array is cleared first, and that call
/// is counted in <see cref="FlagClears"/>. <see cref="IsDirty"/> is asked
/// for every returned array after that, so a test sees exactly what a
/// renter of the array would see.
/// </remarks>
[ExcludeFromCodeCoverage]
internal sealed class CountingPool<T> : ArrayPool<T>
{
	readonly ArrayPool<T> _inner = Create();

	readonly ConcurrentDictionary<T[], int> _out
		= new(ReferenceEqualityComparer.Instance);

	readonly ConcurrentQueue<T[]> _captured = new();

	readonly ConcurrentQueue<int> _requests = new();

	int _rents;
	int _returns;
	int _unmatched;
	int _dirty;
	int _flagClears;
	int _throwsLeft;
	int _inFlight;

	/// <summary>
	/// True: every array is longer than asked for (a pool that rounds up).
	/// </summary>
	public bool RoundUp { get; init; }

	/// <summary>
	/// True: every array is a new array of exactly the size asked for
	/// (less <see cref="Shortfall"/>).
	/// </summary>
	public bool Exact { get; init; }

	/// <summary>
	/// How many slots are missing from every array of an
	/// <see cref="Exact"/> pool (a pool that breaks its contract by giving
	/// less than was asked for).
	/// </summary>
	public int Shortfall { get; init; }

	/// <summary>
	/// A return fails (after it is counted), as a broken pool would.
	/// </summary>
	public bool ThrowOnReturn { get; set; }

	/// <summary>
	/// The first this many returns fail (after they are counted).
	/// </summary>
	public int ThrowOnFirstReturns
	{
		get => Volatile.Read(ref _throwsLeft);
		set => Volatile.Write(ref _throwsLeft, value);
	}

	/// <summary>
	/// Every rent after this many fails (-1: never).
	/// </summary>
	public int FailRentsAfter { get; init; } = -1;

	/// <summary>
	/// Fills every rented array with this value, so a test can see
	/// what was, and was not, cleared.
	/// </summary>
	public Func<T>? Sentinel { get; init; }

	/// <summary>
	/// Asked for every array when it comes back, after the flag has done
	/// its clear: true counts a dirty return.
	/// </summary>
	public Func<T[], bool>? IsDirty { get; init; }

	public int DirtyReturns => Volatile.Read(ref _dirty);

	/// <summary>How many returns asked the pool to clear (the whole array).</summary>
	public int FlagClears => Volatile.Read(ref _flagClears);

	public int Rents => Volatile.Read(ref _rents);

	public int Returns => Volatile.Read(ref _returns);

	public int Unmatched => Volatile.Read(ref _unmatched);

	// An array whose return is still running (the counters not yet updated)
	// is still outstanding: a test that waits for 0 sees every counter.
	public int Outstanding => _out.Count + Volatile.Read(ref _inFlight);

	/// <summary>Every array this pool ever rented, in order.</summary>
	public T[][] Captured => [.. _captured];

	/// <summary>The size asked for by every rent, in order.</summary>
	public int[] Requests => [.. _requests];

	public override T[] Rent(int minimumLength)
	{
		if (FailRentsAfter >= 0 && Rents >= FailRentsAfter)
			throw new InvalidOperationException("the pool failed");

		int size = Math.Max(minimumLength, 1);
		_requests.Enqueue(size);
		T[] array = Exact
			? new T[size - Shortfall]
			: _inner.Rent(RoundUp ? size + 5 : size);
		if (Sentinel is not null) Array.Fill(array, Sentinel());

		Interlocked.Increment(ref _rents);
		_out[array] = 0;
		_captured.Enqueue(array);
		return array;
	}

	public override void Return(T[] array, bool clearArray = false)
	{
		Interlocked.Increment(ref _inFlight);
		try
		{
			if (!_out.TryRemove(array, out _))
			{
				Interlocked.Increment(ref _unmatched);
				return;
			}

			if (clearArray)
			{
				Interlocked.Increment(ref _flagClears);
				Array.Clear(array);
			}

			if (IsDirty?.Invoke(array) == true)
				Interlocked.Increment(ref _dirty);

			Interlocked.Increment(ref _returns);
			if (!Exact) _inner.Return(array);

			if (ThrowOnReturn || Interlocked.Decrement(ref _throwsLeft) >= 0)
				throw new InvalidOperationException("pool return failed");
		}
		finally
		{
			Interlocked.Decrement(ref _inFlight);
		}
	}
}