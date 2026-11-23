using Open.Database.Extensions.Core;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;
using Item = Open.Database.Extensions.Tests.RowMapTests.Item;

#nullable enable

namespace Open.Database.Extensions.Tests;

// Every override-taking public method through each receiver, through a map.
// The columns are renamed, so a wrapper that swaps Field and Column, or does not forward its map, fails here.
[ExcludeFromCodeCoverage]
public static class RowMapFamilyTests
{

	sealed class AsyncCommand(FakeExecuteReader inner) : IExecuteReaderAsync
	{
		public bool UseAsyncRead => false;
		public CancellationToken CancellationToken => inner.CancellationToken;
		public void ExecuteReader(Action<IDataReader> handler, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReader(handler, behavior);
		public T ExecuteReader<T>(Func<IDataReader, T> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReader(transform, behavior);
		public ValueTask ExecuteReaderAsync(Func<IDataReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(handler, behavior);
		public TResult ExecuteReader<TState, TResult>(TState state, Func<IDataReader, TState, TResult> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReader(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReaderAsync(reader => transform(reader, state), behavior);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
			=> ExecuteReaderAsync(state, transform, behavior);

		public ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(transform, behavior);
	}

	// ---- receivers ----
	static FakeExecuteReader Exec() => RowMapTests.Cmd(RowMapTests.Renamed, 2);

	static Task<List<Item>> Now(IEnumerable<Item> items) => Task.FromResult(items.ToList());

	// name -> the call, through a map
	static readonly Dictionary<string, Func<RowMap<Item>, Task<List<Item>>>> Cases = new()
	{
		// Core
		["Map(IExecuteReader)"] = m => Now(Exec().Map(m).ToList()),

	};

	public static TheoryData<string> Names => [.. Cases.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task RenamedColumns_AreMappedThroughTheOverload(string name)
		=> RowMapTests.AssertItems(await Cases[name](RowMapTests.Plain), 2);
}
