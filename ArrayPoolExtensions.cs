using System;
using System.Buffers;

namespace Open.Database.Extensions
{
    static class LocalArrayPool<T>
    {
        public const int MaxArrayLength = 1024 * 1024;
        static LocalArrayPool()
        {
            Instance = ArrayPool<T>.Create(MaxArrayLength, 4);
        }
        public static readonly ArrayPool<T> Instance;

        public static void Rent(
            int minimumLength,
            Action<Memory<T>> handler)
        {
            if (minimumLength < 0)
                throw new ArgumentOutOfRangeException(nameof(minimumLength), minimumLength, "Must be at least 0.");

            if (minimumLength > MaxArrayLength)
            {
                handler(new T[minimumLength]);
            }
            else
            {
                var array = Instance.Rent(minimumLength);
                try
                {
                    handler(new Memory<T>(array, 0, minimumLength));
                }
                finally
                {
                    Instance.Return(array, false);
                }
            }
        }

        public static TResult Rent<TResult>(
            long minimumLength,
            bool clearAfter,
            Func<Memory<T>, TResult> handler)
        {
            if (minimumLength < 0)
                throw new ArgumentOutOfRangeException(nameof(minimumLength), minimumLength, "Must be at least 0.");

            if (minimumLength > MaxArrayLength)
            {
                return handler(new T[minimumLength]);
            }
            else
            {
                var array = Instance.Rent((int)(minimumLength));
                try
                {
                    return handler(array);
                }
                finally
                {
                    Instance.Return(array, clearAfter);
                }
            }
        }
    }

}
