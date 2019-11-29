using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions.Dataflow
{
	internal class Transformer<T> : Core.Transformer<T>
		where T : new()
	{
		public Transformer(IEnumerable<(string Field, string Column)>? overrides = null)
			: base(overrides)
		{ }

		class DataflowProcessor : Processor
		{
			public DataflowProcessor(Transformer<T> transformer)
				: base(transformer)
			{

			}

			public DataflowProcessor(Transformer<T> transformer, ImmutableArray<string> names)
				: base(transformer, names)
			{

			}

			public TransformBlock<object[], T> GetBlock(
				ExecutionDataflowBlockOptions? options = null)
				=> options == null
					? new TransformBlock<object[], T>(Transform)
					: new TransformBlock<object[], T>(Transform, options);
		}

		public IReceivableSourceBlock<T> Results(
			out Action<QueryResult<IEnumerable<object[]>>> deferred,
			ExecutionDataflowBlockOptions? options = null)
		{
			var processor = new DataflowProcessor(this);
			var x = processor.GetBlock(options);

			deferred = results =>
			{
				processor.SetNames(results.Names);
				var q = results.Result;
				foreach (var record in q) if (!x.Post(record)) break;
				x.Complete(); // May not be necessary, but we'll call it to ensure the .Completion occurs.
			};

			return x;
		}

		public IReceivableSourceBlock<T> ResultsAsync(
			out Func<QueryResult<IEnumerable<object[]>>, ValueTask> deferred,
			ExecutionDataflowBlockOptions? options = null)
		{
			var processor = new DataflowProcessor(this);
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

		public IReceivableSourceBlock<T> Results(
			QueryResult<IReceivableSourceBlock<object[]>> source,
			ExecutionDataflowBlockOptions? options = null)
		{
			if (source is null) throw new ArgumentNullException(nameof(source));
			Contract.EndContractBlock();

			var processor = new DataflowProcessor(this, source.Names);
			var x = processor.GetBlock(options);
			var r = source.Result;
			r.LinkTo(x, new DataflowLinkOptions { PropagateCompletion = true });
			x.Completion.ContinueWith(
				t => r.Complete(), // just stop the flow if x has been completed externally.
				CancellationToken.None,
				TaskContinuationOptions.ExecuteSynchronously,
				TaskScheduler.Current); // Signal that no more results can be received.
			return x;
		}

		public TransformBlock<object[], T> ResultsBlock(
			out Action<ImmutableArray<string>> initColumnNames,
			ExecutionDataflowBlockOptions? options = null)
		{
			var processor = new DataflowProcessor(this);
			var x = processor.GetBlock(options);

			initColumnNames = results => processor.SetNames(results);

			return x;
		}

	}
}
