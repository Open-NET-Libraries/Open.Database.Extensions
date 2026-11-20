using System.Buffers;
using System.Collections.Concurrent;

#nullable enable

namespace Open.Database.Extensions.Tests;

/// <summary>
/// A <see cref="MemoryPool{T}"/> over an <see cref="ArrayPool{T}"/>
/// that counts what was rented and what came back, counts a return of
/// something that was not outstanding (a second disposal) as unmatched,
/// and can be told to round every rent up (a custom pool that gives
/// more than was asked for), or to throw when an owner is disposed.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class CountingPool<T> : MemoryPool<T>
{
	readonly ArrayPool<T> _inner = ArrayPool<T>.Create();

	readonly ConcurrentDictionary<T[], int> _out
		= new(ReferenceEqualityComparer.Instance);

	readonly ConcurrentQueue<T[]> _captured = new();

	readonly ConcurrentQueue<int> _requests = new();

	int _rents;
	int _returns;
	int _unmatched;
	int _dirty;
	int _throwsLeft;

	/// <summary>
	/// True: every buffer is the whole, larger array.
	/// False: exactly the size asked for.
	/// </summary>
	public bool RoundUp { get; init; }

	/// <summary>
	/// True: every buffer is a new array of exactly the size asked for (so
	/// a whole array of that size, whatever the inner pool would round to).
	/// </summary>
	public bool Exact { get; init; }

	/// <summary>
	/// A disposal fails (after it is counted), as a broken pool
	/// would.
	/// </summary>
	public bool ThrowOnReturn { get; set; }

	/// <summary>
	/// The first this many disposals fail (after they are counted).
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
	/// Asked for the array of every owner at its disposal, before it
	/// goes back: true counts a dirty return.
	/// </summary>
	public Func<T[], bool>? IsDirty { get; init; }

	public int DirtyReturns => Volatile.Read(ref _dirty);

	public int Rents => Volatile.Read(ref _rents);

	public int Returns => Volatile.Read(ref _returns);

	public int Unmatched => Volatile.Read(ref _unmatched);

	public int Outstanding => _out.Count;

	/// <summary>Every array this pool ever rented, in order.</summary>
	public T[][] Captured => [.. _captured];

	/// <summary>The size asked for by every rent, in order.</summary>
	public int[] Requests => [.. _requests];

	public override int MaxBufferSize
		=> int.MaxValue;

	public override IMemoryOwner<T> Rent(int minBufferSize = -1)
	{
		if (FailRentsAfter >= 0 && Rents >= FailRentsAfter)
			throw new InvalidOperationException("the pool failed");

		int size = Math.Max(minBufferSize, 1);
		_requests.Enqueue(size);
		T[] array = Exact ? new T[size] : _inner.Rent(RoundUp ? size + 5 : size);
		if (Sentinel is not null) Array.Fill(array, Sentinel());

		Interlocked.Increment(ref _rents);
		_out[array] = 0;
		_captured.Enqueue(array);
		return new Owner(this, array, RoundUp ? array.Length : size);
	}

	protected override void Dispose(bool disposing) { }

	sealed class Owner(CountingPool<T> pool, T[] array, int length)
		: IMemoryOwner<T>
	{
		T[]? _array = array;

		public Memory<T> Memory
			=> Volatile.Read(ref _array) is { } items
				? new(items, 0, length)
				: throw new ObjectDisposedException(nameof(Owner));

		public void Dispose()
		{
			T[]? items = Interlocked.Exchange(ref _array, null);
			if (items is null)
			{
				Interlocked.Increment(ref pool._unmatched);
				return;
			}

			if (pool.IsDirty?.Invoke(items) == true)
				Interlocked.Increment(ref pool._dirty);

			Interlocked.Increment(ref pool._returns);
			pool._out.TryRemove(items, out _);
			pool._inner.Return(items);

			if (pool.ThrowOnReturn || Interlocked.Decrement(ref pool._throwsLeft) >= 0)
				throw new InvalidOperationException("pool return failed");
		}
	}
}
