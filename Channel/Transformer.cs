using Open.ChannelExtensions;
using Open.Database.Extensions.Core;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	internal class Transformer<T> : Core.Transformer<T>
		where T : new()
	{
		public Transformer(CancellationToken cancellationToken, IEnumerable<(string Field, string? Column)>? overrides = null)
			: base(overrides)
		{
			CancellationToken = cancellationToken;
		}

		public static IEnumerable<object[]> AsEnumerable(this IDataReader reader)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			if (reader.Read())
			{
				var fieldCount = reader.FieldCount;
				do
				{
					var row = LocalPool.Rent(fieldCount);
					reader.GetValues(row);
					yield return row;
				} while (reader.Read());
			}
		}

		internal static IEnumerable<object[]> AsEnumerableInternal(IDataReader reader, IEnumerable<int> ordinals, bool readStarted)
		{
			if (reader is null) throw new ArgumentNullException(nameof(reader));
			if (ordinals is null) throw new ArgumentNullException(nameof(ordinals));
			Contract.EndContractBlock();

			if (readStarted || reader.Read())
			{
				var o = ordinals as IList<int> ?? ordinals.ToArray();
				var fieldCount = o.Count;

				do
				{
					var row = LocalPool.Rent(fieldCount);
					for (var i = 0; i < fieldCount; i++)
						row[i] = reader.GetValue(o[i]);
					yield return row;
				}
				while (reader.Read());
			}
		}

		public ChannelReader<T> Results(
			out Action<QueryResult<IEnumerable<object[]>>> deferred,
			int capacity = -1, bool singleReader = false)
		{
			var channel = ChannelExtensions.CreateChannel<object[]>(MaxArrayBuffer, true);
			var processor = new Processor(this);
			var x = channel.Pipe(processor.Transform, capacity, singleReader, CancellationToken);

			deferred = results =>
			{
				processor.SetNames(results.Names);
				var q = results.Result;
				foreach (var record in q) if (!x.Post(record)) break;
				x.Complete(); // May not be necessary, but we'll call it to ensure the .Completion occurs.
			};

			return x;
		}

		public ChannelReader<T> ResultsAsync(
			out Func<QueryResult<IEnumerable<object[]>>, ValueTask> deferred,
			ExecutionDataflowBlockOptions? options = null)
		{
			var processor = new Processor(this);
			var x = processor.GetBlock(options);

			deferred = async results =>
			{
				processor.SetNames(results.Names);
				var q = results.Result;
				foreach (var record in q) if (!await x.SendAsync(record).ConfigureAwait(false)) break;
				x.Complete(); // May not be necessary, but we'll call it to ensure the .Completion occurs.
			};

			return x;
		}


	}
}
