using Open.Database.Extensions.Core;
using System.Data.Common;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The `where T : new()` constraint is gone from every method that maps rows to a model, so a type that cannot be mapped is found out at run time.
// These tests pin where: AT THE CALL (the RowMap<T> constructor and RowMap<T>.Default, which every entry point passes), with an EMPTY result,
// so it never depends on there being a first row, and with an InvalidOperationException, never a TypeInitializationException.
// The two "cannot be mapped yet" messages are interim: constructor binding and `required` members arrive in later phases, which replace them.
[ExcludeFromCodeCoverage]
public static class UnmappableTypeTests
{
	// ---- the type shapes ----

	public interface IShapeless { int Id { get; set; } }

	public abstract class AbstractShape { public int Id { get; set; } }

	public delegate int ShapeCallback();

	public enum ShapeKind { None, Some }

	public sealed class PrivateConstructorShape
	{
		PrivateConstructorShape() { }
		public int Id { get; set; }
		public static PrivateConstructorShape Make() => new();
	}

	public sealed class TwoConstructorShape
	{
		public TwoConstructorShape(int id) => Id = id;
		public TwoConstructorShape(int id, string title) { Id = id; Title = title; }
		public int Id { get; set; }
		public string? Title { get; set; }
	}

	public sealed record PositionalShape(int Id, string Title);

	public readonly record struct PositionalStructShape(int Id, string Title);

	public sealed class RequiredShape { public required int Id { get; set; } }

	public class RequiredBase { public required int Id { get; set; } }

	public sealed class RequiredDerived : RequiredBase { public string? Title { get; set; } }

	// What maps today and must keep mapping: a parameterless constructor next to another one, a struct with none of its own, a struct whose only constructor is not public.
	public sealed class ParameterlessAndOther
	{
		public ParameterlessAndOther() { }
		public ParameterlessAndOther(int id) => Id = id + 1000;
		public int Id { get; set; }
	}

	public struct StructWithNoConstructor { public int Id { get; set; } }

	public struct StructWithPrivateConstructor
	{
		StructWithPrivateConstructor(int id) => Id = id;
		public int Id { get; set; }
		public static StructWithPrivateConstructor Make() => new(0);
	}

	const string Interim = "(constructor binding arrives in a later phase)";

	static string Single(string name) => $"'{name}' is a single value, not a row model. Use ExecuteScalar<T>() or FirstOrdinalResults<T>().";
	static string NotConcrete(string name, string kind) => $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
	static string NoParameterless(string name) => $"'{name}' cannot be mapped yet: it has no public parameterless constructor {Interim}.";
	static string Required(string name) => $"'{name}' cannot be mapped yet: it has required members (required-member support arrives in a later phase).";

	// Every way to meet a type: the default map, a map constructed for it, and a query over it with no rows. Each one throws the same exception.
	static void AssertUnmappable<T>(string message)
	{
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.Default).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.Default).Message); // Reading it again throws again: it is not a failed type initializer.
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => new RowMap<T>()).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => new RowMap<T>([])).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.From([])).Message);

		var command = new FakeExecuteReader(["Id"]);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => command.Map<T>()).Message);
		Assert.Equal(0, command.ExecuteCount); // Nothing ran: the call failed before the query was executed.

		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => FakeReader.Create(["Id"]).Results<T>()).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => FakeReader.Create(["Id"]).ResultsBuffered<T>()).Message);
	}

	// ---- the type-shape rules ----

	[Fact]
	public static void Interface_IsRejected() => AssertUnmappable<IShapeless>(NotConcrete("IShapeless", "an interface"));

	[Fact]
	public static void AbstractClass_IsRejected() => AssertUnmappable<AbstractShape>(NotConcrete("AbstractShape", "an abstract class"));

	[Fact]
	public static void Delegate_IsRejected() => AssertUnmappable<ShapeCallback>(NotConcrete("ShapeCallback", "a delegate"));

	[Fact]
	public static void Scalars_AreRejected_WithAPointerToTheScalarMethods()
	{
		AssertUnmappable<int>(Single("Int32"));
		AssertUnmappable<string>(Single("String"));
		AssertUnmappable<int?>(Single("Int32?"));
		AssertUnmappable<Guid>(Single("Guid"));
		AssertUnmappable<decimal>(Single("Decimal"));
		AssertUnmappable<DateTime>(Single("DateTime"));
		AssertUnmappable<DateTimeOffset?>(Single("DateTimeOffset?"));
		AssertUnmappable<TimeSpan>(Single("TimeSpan"));
		AssertUnmappable<bool>(Single("Boolean"));
		AssertUnmappable<ShapeKind>(Single("ShapeKind"));
		AssertUnmappable<ShapeKind?>(Single("ShapeKind?"));
		AssertUnmappable<byte[]>(Single("Byte[]"));
	}

	[Fact]
	public static void ClassWithOnlyAPrivateConstructor_IsRejected_UntilConstructorBinding()
	{
		AssertUnmappable<PrivateConstructorShape>(NoParameterless("PrivateConstructorShape"));
		Assert.NotNull(PrivateConstructorShape.Make()); // The type itself is fine: it is the mapper that cannot reach the constructor.
	}

	[Fact]
	public static void ClassWithTwoPublicConstructorsAndNoParameterlessOne_IsRejected_UntilConstructorBinding()
		=> AssertUnmappable<TwoConstructorShape>(NoParameterless("TwoConstructorShape"));

	[Fact]
	public static void PositionalRecord_IsRejected_UntilConstructorBinding()
		=> AssertUnmappable<PositionalShape>(NoParameterless("PositionalShape"));

	// A struct that declares a public constructor and no parameterless one is rejected like a class is, until constructor binding builds it through that constructor.
	[Fact]
	public static void PositionalRecordStruct_IsRejected_UntilConstructorBinding()
		=> AssertUnmappable<PositionalStructShape>(NoParameterless("PositionalStructShape"));

	[Fact]
	public static void TypeWithRequiredMembers_IsRejected_SoNoRequiredTypeIsEverMappedWithoutEnforcement()
	{
		AssertUnmappable<RequiredShape>(Required("RequiredShape"));
		AssertUnmappable<RequiredDerived>(Required("RequiredDerived")); // A required member of a base type counts.
	}

	[Fact]
	public static void WhatMapsToday_StillMaps()
	{
		Assert.Equal(1, new FakeExecuteReader(["Id"], [1]).Map<ParameterlessAndOther>().ToList().Single().Id);
		Assert.Equal(2, new FakeExecuteReader(["Id"], [2]).Map<StructWithNoConstructor>().ToList().Single().Id);
		Assert.Equal(3, new FakeExecuteReader(["Id"], [3]).Map<StructWithPrivateConstructor>().ToList().Single().Id);
		Assert.Equal(4, FakeReader.Create(["Id"], [4]).Results<ParameterlessAndOther>().Single().Id);

		Assert.Same(RowMap<ParameterlessAndOther>.Default, RowMap<ParameterlessAndOther>.Default); // The default map is one instance, as it was when it was a field.
		Assert.NotNull(new RowMap<StructWithNoConstructor>(new("Id", "Id")));
		Assert.Equal(0, StructWithPrivateConstructor.Make().Id); // Its constructor is not public, so it is built as it always was.
	}

	// ---- T14: an unmappable T throws at the call, with an EMPTY result, on each family of entry points ----

	static IDataReader Reader() => FakeReader.Create(["Id"]);
	static DbDataReader DbReader() => FakeReader.CreateDb(["Id"]);
	static FakeExecuteReader Exec() => new(["Id"]);

	static IDbCommand Command()
	{
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Open);
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => FakeReader.Create(["Id"]));
		return command;
	}

	static FakeDbCommand DbCmd()
	{
		var connection = new FakeDbConnection();
		connection.SetState(ConnectionState.Open);
		return new FakeDbCommand(connection, () => FakeReader.CreateDb(["Id"]));
	}

	static DataTable Table()
	{
		var table = new DataTable();
		table.Columns.Add("Id", typeof(int));
		return table;
	}

	static QueryResult<Queue<object?[]>> Queued() => new([0], ["Id"], new Queue<object?[]>());

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

		public ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(transform, behavior);
	}

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static AsyncCommand Async() => new(Exec());

	static Channel<IShapeless> Writer() => System.Threading.Channels.Channel.CreateUnbounded<IShapeless>();

	// A call that returns a reader reports a failure through its Completion, as documented; a call that returns a task reports it on the task.
	// The call itself, when it fails before it returns anything.
	static Task Run(Action call)
	{
		call();
		return Task.CompletedTask;
	}

	static async Task Done(ChannelReader<IShapeless> reader) => await reader.Completion;
	static async Task Done(IReceivableSourceBlock<IShapeless> block) => await block.Completion;
	static async Task Done<T>(ValueTask<T> task) => await task;
	static async Task Done(IAsyncEnumerable<IShapeless> source) { await foreach (IShapeless _ in source) { } }

	// name -> the call, for the interface IShapeless, over an empty result
	static readonly Dictionary<string, Func<Task>> Calls = new()
	{
		// Core: Map<T>() and its terminals (the call that fails is Map<T>() itself, so no terminal is ever reached)
		["Map<T>()"] = () => Run(() => Exec().Map<IShapeless>()),
		["Map<T>().ToList()"] = () => Run(() => Exec().Map<IShapeless>().ToList()),
		["Map<T>().FirstOrDefault()"] = () => Run(() => Exec().Map<IShapeless>().FirstOrDefault()),
		["Map<T>().Skip().Take().ToArray()"] = () => Run(() => Exec().Map<IShapeless>().Skip(1).Take(2).ToArray()),
		["Map<T>().ToListAsync()"] = async () => await Exec().Map<IShapeless>().ToListAsync(),
		["Map<T>().FirstOrDefaultAsync()"] = async () => await Exec().Map<IShapeless>().FirstOrDefaultAsync(),
		["Map<T>().ToDictionaryAsync()"] = async () => await Exec().Map<IShapeless>().ToDictionaryAsync(i => i.Id),
		["Map<T>(pair)"] = () => Run(() => Exec().Map<IShapeless>(new KeyValuePair<string, string?>("Id", "Id")).ToList()),
		["Map<T>(pairs)"] = () => Run(() => Exec().Map<IShapeless>(new KeyValuePair<string, string?>("Id", "Id"), new("Name", "Name")).ToList()),

		// Core: Results
		["Results<T>(IDataReader)"] = () => Run(() => Reader().Results<IShapeless>()),
		["Results<T>(IDataReader, null)"] = () => Run(() => Reader().Results<IShapeless>(null)),
		["ResultsBuffered<T>(IDataReader)"] = () => Run(() => Reader().ResultsBuffered<IShapeless>()),
		["Results<T>(IDbCommand)"] = () => Run(() => Command().Results<IShapeless>()),
		["Results<T>(IExecuteReader)"] = () => Run(() => Exec().Results<IShapeless>()),
		["ResultsAsync<T>(DbDataReader)"] = () => Done(DbReader().ResultsAsync<IShapeless>()),
		["ResultsBufferedAsync<T>(DbDataReader)"] = async () => await DbReader().ResultsBufferedAsync<IShapeless>(),
		["ResultsAsync<T>(DbCommand)"] = async () => await DbCmd().ResultsAsync<IShapeless>(),

		// Core: DataTable and Retrieve
		["To<T>(DataTable)"] = () => Run(() => Table().To<IShapeless>()),
		["DequeueAs<T>(QueryResult)"] = () => Run(() => Queued().DequeueAs<IShapeless>()),
		["DequeueAs<T>(Task)"] = async () => await Task.FromResult(Queued()).DequeueAs<IShapeless>(),
		["DequeueAs<T>(ValueTask)"] = async () => await new ValueTask<QueryResult<Queue<object?[]>>>(Queued()).DequeueAs<IShapeless>(),
		["new Transformer<T>()"] = () => Run(() => Transformer<IShapeless>.Create()),

		// Channel: a new channel
		["ToChannel<T>(IDataReader)"] = () => Done(Reader().ToChannel<IShapeless>(true)),
		["ToChannel<T>(IDataReader, null)"] = () => Done(Reader().ToChannel<IShapeless>(true, (RowMap<IShapeless>?)null)),
		["ToChannel<T>(IDbCommand)"] = () => Done(Command().ToChannel<IShapeless>(true)),
		["ToChannel<T>(IExecuteReader)"] = () => Done(Exec().ToChannel<IShapeless>(true)),
		["ToChannelAsync<T>(DbDataReader)"] = () => Done(DbReader().ToChannelAsync<IShapeless>(true)),
		["ToChannelAsync<T>(DbCommand)"] = () => Done(DbCmd().ToChannelAsync<IShapeless>(true)),
		["ToChannelAsync<T>(IExecuteReaderAsync)"] = () => Done(Async().ToChannelAsync<IShapeless>(true)),

		// Channel: the caller's writer
		["ToChannel<T>(writer, IDataReader)"] = () => Done(Reader().ToChannel<IShapeless>(Writer().Writer, true)),
		["ToChannel<T>(writer, IDataReader, null)"] = () => Done(Reader().ToChannel<IShapeless>(Writer().Writer, true, (RowMap<IShapeless>?)null)),
		["ToChannel<T>(writer, IDbCommand)"] = () => Done(Command().ToChannel<IShapeless>(Writer().Writer, true)),
		["ToChannel<T>(writer, IExecuteReader)"] = () => Done(Exec().ToChannel<IShapeless>(Writer().Writer, true)),
		["ToChannelAsync<T>(writer, DbDataReader)"] = () => Done(DbReader().ToChannelAsync<IShapeless>(Writer().Writer, true)),
		["ToChannelAsync<T>(writer, DbCommand)"] = () => Done(DbCmd().ToChannelAsync<IShapeless>(Writer().Writer, true)),
		["ToChannelAsync<T>(writer, IExecuteReaderAsync)"] = () => Done(Async().ToChannelAsync<IShapeless>(Writer().Writer, true)),

		// Dataflow: into a block
		["ToTargetBlock<T>(IDataReader)"] = () => Done(new ValueTask<long>(Reader().ToTargetBlock(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null))),
		["ToTargetBlock<T>(IDbCommand)"] = () => Done(new ValueTask<long>(Command().ToTargetBlock(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null))),
		["ToTargetBlock<T>(IExecuteReader)"] = () => Done(new ValueTask<long>(Exec().ToTargetBlock(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null))),
		["ToTargetBlockAsync<T>(IDataReader)"] = () => Done(Reader().ToTargetBlockAsync(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null)),
		["ToTargetBlockAsync<T>(IDbCommand)"] = () => Done(Command().ToTargetBlockAsync(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null)),
		["ToTargetBlockAsync<T>(IExecuteReader)"] = () => Done(Exec().ToTargetBlockAsync(new BufferBlock<IShapeless>(), true, (RowMap<IShapeless>?)null)),

		// Dataflow: a new source block
		["AsSourceBlock<T>(IDataReader)"] = () => Done(Reader().AsSourceBlock<IShapeless>()),
		["AsSourceBlock<T>(IDbCommand)"] = () => Done(Command().AsSourceBlock<IShapeless>()),
		["AsSourceBlock<T>(IExecuteReader)"] = () => Done(Exec().AsSourceBlock<IShapeless>()),
		["AsSourceBlockAsync<T>(IDataReader)"] = () => Done(Reader().AsSourceBlockAsync<IShapeless>()),
		["AsSourceBlockAsync<T>(IDbCommand)"] = () => Done(Command().AsSourceBlockAsync<IShapeless>()),
		["AsSourceBlockAsync<T>(IExecuteReader)"] = () => Done(Exec().AsSourceBlockAsync<IShapeless>()),
	};

	public static TheoryData<string> Names => [.. Calls.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task EveryEntryPoint_Throws_ForAnUnmappableType_WithAnEmptyResult(string name)
	{
		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Calls[name]().WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		Assert.Equal(NotConcrete("IShapeless", "an interface"), ex.Message);
	}

	// The failure is the type's, not the data's: with one row it is the same exception, and the query is not read.
	[Fact]
	public static void MapOfAnUnmappableType_FailsBeforeTheQueryRuns_WhateverTheResult()
	{
		var command = new FakeExecuteReader(["Id"], [1], [2]);
		Assert.Throws<InvalidOperationException>(() => command.Map<IShapeless>());
		Assert.Equal(0, command.ExecuteCount);
	}

	// A declared map exists only for a type that can be mapped, so a call that passes one never meets the failure.
	[Fact]
	public static void AMappableType_IsNotAffected_ByAnotherTypeBeingRejected()
	{
		_ = Assert.Throws<InvalidOperationException>(() => RowMap<IShapeless>.Default);
		RowMap<ParameterlessAndOther> map = [new("Id", "id")];
		Assert.Equal(7, new FakeExecuteReader(["id"], [7]).Map(map).ToList().Single().Id);
	}
}
