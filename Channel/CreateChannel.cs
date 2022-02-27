using System;
using System.Threading.Channels;

namespace Open.Database.Extensions;

public static partial class ChannelDbExtensions
{
    internal static Channel<T> CreateChannel<T>(int capacity = -1, bool singleReader = false, bool singleWriter = true)
    {
        if (capacity == 0) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Cannot be zero.");
        if (capacity < -1) throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Must greater than zero or equal to negative one (unbounded).");

        return capacity > 0
            ? Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            {
                SingleWriter = singleWriter,
                SingleReader = singleReader,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait
            })
            : Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleWriter = singleWriter,
                SingleReader = singleReader,
                AllowSynchronousContinuations = true
            });
    }
}
