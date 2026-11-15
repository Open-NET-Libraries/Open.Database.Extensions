using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;

#nullable enable

namespace Open.Database.Extensions.Tests;

// IMappedQuery<T>.Select (and the ProjectedQuery behind it) is removed:
// a mapped view is mapped to its result type with Map(RowMap<TResult>.From(factory)),
// or projected after a terminal.
// The reader-level IDataReader.Select / SelectAsync stay (the caller opened the reader).
[ExcludeFromCodeCoverage]
public static class MappedQuerySelectRemovedTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	// A test cannot assert a compile error (CS1061 on `view.Select(...)`):
	// the same fact is asserted over the public surface by reflection.
	static readonly Type[] ViewTypes =
	[
		typeof(IMappedQuery<>),
		typeof(MappedQuery<>),
		typeof(MappedQueryWithSkip<>),
		typeof(MappedQueryWithTake<>),
		typeof(MappedReader<>),
		typeof(MappedCommand<>),
		typeof(MappedTable<>),
	];

	static bool IsView(Type type)
		=> type.IsGenericType
		&& Array.IndexOf(ViewTypes, type.GetGenericTypeDefinition()) >= 0;

	static IEnumerable<MethodInfo> PublicSelects(Assembly assembly)
		=> assembly.GetExportedTypes()
			.SelectMany(t => t.GetMethods(
				BindingFlags.Public
				| BindingFlags.Static
				| BindingFlags.Instance
				| BindingFlags.DeclaredOnly))
			.Where(m => m.Name == "Select");

	[Fact]
	public static void NoViewHasASelect_AsAnExtensionOrAMember()
	{
		Assembly core = typeof(IMappedQuery<>).Assembly;

		foreach (MethodInfo method in PublicSelects(core))
		{
			Type declaring = method.DeclaringType!;
			Assert.False(
				IsView(declaring),
				$"{declaring.FullName}.Select is a member of a mapped view");

			if (method.IsStatic
				&& method.GetParameters() is [{ } first, ..])
			{
				Assert.False(
					IsView(first.ParameterType),
					$"{declaring.FullName}.Select extends {first.ParameterType}");
			}
		}
	}

	[Fact]
	public static void TheReaderLevelSelect_StillExists_AndStillCompiles()
	{
		Assert.Contains(
			PublicSelects(typeof(IMappedQuery<>).Assembly),
			m => m.IsStatic
				&& m.GetParameters() is [{ } first, ..]
				&& first.ParameterType == typeof(IDataReader));

		// Compiles, so the overload binds on a reader the caller opened.
		using DataTable table = TwoRows();
		using DataTableReader reader = table.CreateDataReader();
		Assert.Equal([1, 2], reader.Select(r => r.GetInt32(0)).ToList());
	}

	// What the removal leaves as the migration forms (see migration-lines.md):
	// a factory map straight to the result type, and projecting after the terminal.
	sealed record Label(int Id, string Text);

	static DataTable TwoRows()
	{
		DataTable table = new();
		table.Columns.Add("Id", typeof(int));
		table.Columns.Add("Name", typeof(string));
		table.Rows.Add(1, "a");
		table.Rows.Add(2, "b");
		return table;
	}

	[Fact]
	public static void TheReplacementForms_GiveWhatSelectGave()
	{
		using DataTable table = TwoRows();
		RowMap<Label> map = RowMap<Label>.From(
			(int id, string name) => new Label(id, "n:" + name));

		Assert.Equal(
			[new Label(1, "n:a"), new Label(2, "n:b")],
			table.Map(map).ToList());

		Assert.Equal(
			["1:a", "2:b"],
			table.Map<Pair>()
				.ToList()
				.Select(x => $"{x.Id}:{x.Name}")
				.ToList());
	}

	sealed record Pair(int Id, string Name);

	#region A foreign IMappedQuery<T>: the LINQ fallback never resumes on the caller's context

	sealed class CountingContext : SynchronizationContext
	{
		readonly ConcurrentQueue<string> _posts = new();

		public int Posts => _posts.Count;

		// What was posted: the awaiter's continuation names its state machine.
		public string What => string.Join(" ; ", _posts);

		public override void Post(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			ThreadPool.QueueUserWorkItem(_ => d(state));
		}

		public override void Send(SendOrPostCallback d, object? state)
		{
			_posts.Enqueue(Describe(state));
			d(state);
		}

		static string Describe(object? state)
			=> state switch
			{
				null => "null",
				Delegate del => $"{del.Target?.GetType()} {del.Method.Name}",
				ITuple tuple => string.Join(
					" | ",
					Enumerable.Range(0, tuple.Length).Select(i => Describe(tuple[i]))),
				_ => state.GetType().ToString(),
			};
	}

	// A source of two rows whose one awaited step
	// (the first row, or the disposal) completes only when the gate opens.
	sealed class GatedQuery(TaskCompletionSource gate, string gated)
		: IMappedQuery<int>
	{
		sealed class Rows(TaskCompletionSource gate, string gated)
			: IAsyncEnumerable<int>, IAsyncEnumerator<int>
		{
			public int Current { get; private set; }

			public IAsyncEnumerator<int> GetAsyncEnumerator(
				CancellationToken cancellationToken = default)
				=> this;

			public ValueTask<bool> MoveNextAsync()
			{
				Current++;
				if (Current > 2) return new(false);
				return Current == 1 && gated == "move"
					? new(Opened(gate.Task))
					: new(true);
			}

			static async Task<bool> Opened(Task gate)
			{
				await gate.ConfigureAwait(false);
				return true;
			}

			public ValueTask DisposeAsync()
				=> gated == "dispose" ? new(gate.Task) : default;
		}

		public void Read(Action<IEnumerable<int>> handler)
			=> handler([1, 2]);

		public TResult Read<TResult>(Func<IEnumerable<int>, TResult> handler)
			=> handler([1, 2]);

		public ValueTask ReadAsync(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask> handler,
			CancellationToken cancellationToken)
			=> handler(new Rows(gate, gated), cancellationToken);

		public ValueTask<TResult> ReadAsync<TResult>(
			Func<IAsyncEnumerable<int>, CancellationToken, ValueTask<TResult>> handler,
			CancellationToken cancellationToken)
			=> handler(new Rows(gate, gated), cancellationToken);
	}

	// Runs the call under a context that counts posts; the gate is opened
	// after the call has returned an incomplete task.
	static async Task<CountingContext> PostsDuring(
		string gated,
		Func<IMappedQuery<int>, Task> terminal)
	{
		TaskCompletionSource gate = new(
			TaskCreationOptions.RunContinuationsAsynchronously);
		CountingContext context = new();
		SynchronizationContext? prior = SynchronizationContext.Current;
		IMappedQuery<int> query = new GatedQuery(gate, gated);

		Task task;
		SynchronizationContext.SetSynchronizationContext(context);
		try
		{
			task = terminal(query);
		}
		finally
		{
			SynchronizationContext.SetSynchronizationContext(prior);
		}

		Assert.False(task.IsCompleted, "the gated step must be genuinely asynchronous");
		gate.TrySetResult();
		await task;
		return context;
	}

	[Theory]
	[InlineData("move", "ToListAsync")]
	[InlineData("dispose", "ToListAsync")]
	[InlineData("move", "FirstOrDefaultAsync")]
	[InlineData("dispose", "FirstOrDefaultAsync")]
	public static async Task TheFallback_NeverResumesOnTheCallersContext(
		string gated,
		string terminal)
	{
		Task Run(IMappedQuery<int> q)
			=> terminal == "ToListAsync"
				? q.ToListAsync(Ct).AsTask()
				: q.FirstOrDefaultAsync(Ct).AsTask();

		CountingContext context = await PostsDuring(gated, Run);

		Assert.True(
			context.Posts == 0,
			$"{gated}/{terminal} posted {context.Posts} time(s) to the caller's context: {context.What}");
	}

	#endregion
}
