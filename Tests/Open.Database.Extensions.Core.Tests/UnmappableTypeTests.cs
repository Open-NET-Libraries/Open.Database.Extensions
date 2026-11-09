using Open.Database.Extensions.Core;
using System.Data.Common;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

#nullable enable

namespace Open.Database.Extensions.Tests;

// The `where T : new()` constraint is gone from every method that maps rows to a model, so a type that cannot be mapped is found out at run time.
// These tests pin where: AT THE CALL (the RowMap<T> constructor and RowMap<T>.Default, which every entry point passes), with an EMPTY result,
// so it never depends on there being a first row, and with an InvalidOperationException, never a TypeInitializationException.
// A `required` public field is a type-shape error too (T17): fields are never mapped, so nothing could set it. A `required` property maps (RequiredMemberTests).
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

	// A struct with no public constructor of its own, or with a public parameterless one, maps as it always did: its default value, then the settable members.
	// A struct that declares exactly one public constructor with parameters (a record struct) is built through it: the results are equal for a record struct, whose constructor only assigns.
	public record struct PositionalStructShape(int Id, string Title);

	public readonly record struct ReadonlyPositionalStructShape(int Id, string Title);

	public struct StructWithConstructorOnly(int id, string title)
	{
		public int Id { get; set; } = id + 1000; public string? Title { get; set; } = title + "!";
	}

	// The parameter names are part of what the test asserts (the error lists them), so each one is kept and read once to count as used.
	public sealed class ThreeConstructorShape
	{
		public ThreeConstructorShape(int a) => _ = a;
		public ThreeConstructorShape(string b, int a, int c) { _ = b; _ = a; _ = c; }
		public ThreeConstructorShape(int a, string b) { _ = a; _ = b; }
	}

	public struct TwoConstructorStructShape
	{
		public TwoConstructorStructShape(int id, string title) { Id = id; Title = title; }
		public TwoConstructorStructShape(int id) { Id = id; Title = null; }
		public int Id { get; set; }
		public string? Title { get; set; }
	}

	public sealed class ByReferenceShape(in int id)
	{
		public int Id { get; } = id;
	}

	public class OneOfSeveralShape
	{
		public OneOfSeveralShape(int id) { Id = id; Built = "public"; }
		internal OneOfSeveralShape(int id, string other) { Id = id; Built = other; }
		protected OneOfSeveralShape(string text) => Built = text;
		public int Id { get; }
		public string? Built { get; }
	}

	// A struct with its own public parameterless constructor is built as ever, whatever else it declares: that constructor is not called, as it never was.
	public struct StructWithPublicParameterlessConstructor
	{
		public StructWithPublicParameterlessConstructor() => Id = -1;
		public StructWithPublicParameterlessConstructor(int id) => Id = id + 1000;
		public int Id { get; set; }
	}

	public sealed class RequiredFieldShape { public required int Id; }

	public class RequiredFieldBase { public required string Title = ""; }

	public sealed class RequiredFieldDerived : RequiredFieldBase { public int Views { get; set; } }

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

	static string Single(string name) => $"'{name}' is a single value, not a row model. Use ExecuteScalar<T>() or FirstOrdinalResults<T>().";
	static string NotConcrete(string name, string kind) => $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
	static string NoConstructor(string name) => $"'{name}' cannot be mapped: it has no public constructor.";
	static string Ambiguous(string name, int count, string signatures) => $"'{name}' cannot be mapped: it has {count} public constructors, {signatures}, and none without parameters. Make one of them non-public, or use Map(selector).";
	static string ByReference(string name, string parameter) => $"'{name}' cannot be mapped: its constructor parameter '{parameter}' is passed by reference or is a pointer. Use Map(selector).";
	static string RequiredField(string name, string field) => $"'{name}' cannot be mapped: its required field '{field}' cannot be set. Make it a property.";
	static string Bag(string name) => $"'{name}' cannot be mapped: it is a dictionary or a dynamic object, which has no properties to set from the columns. Map to a class or record, or use Map(selector).";

	// A dictionary or a dynamic object is not a row model: it has no properties to set, so a map of it used to build one empty row and say nothing. Each of these kinds is reached by a different interface.
	public sealed class DictionaryOfMine : Dictionary<string, object?>;

	public sealed class OnlyReadOnlyDictionary : IReadOnlyDictionary<string, int>
	{
		public int this[string key] => throw new NotSupportedException();
		public IEnumerable<string> Keys => [];
		public IEnumerable<int> Values => [];
		public int Count => 0;
		public bool ContainsKey(string key) => false;
		public bool TryGetValue(string key, out int value) { value = 0; return false; }
		public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => Enumerable.Empty<KeyValuePair<string, int>>().GetEnumerator();
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
	}

	public sealed class OnlyMetaObjectProvider : IDynamicMetaObjectProvider
	{
		public DynamicMetaObject GetMetaObject(System.Linq.Expressions.Expression parameter) => throw new NotSupportedException();
	}

	public sealed class DynamicBag : DynamicObject;

	// A model that merely holds a dictionary is a model like any other.
	public sealed class HoldsADictionary
	{
		public int Id { get; set; }
		public Dictionary<string, object?>? Extra { get; set; }
	}

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
	public static void ClassWithOnlyAPrivateConstructor_IsRejected()
	{
		AssertUnmappable<PrivateConstructorShape>(NoConstructor("PrivateConstructorShape"));
		Assert.NotNull(PrivateConstructorShape.Make()); // The type itself is fine: it is the mapper that cannot reach the constructor.
	}

	[Fact]
	public static void ClassWithTwoPublicConstructorsAndNoParameterlessOne_IsRejected_NamingBothSignatures()
		=> AssertUnmappable<TwoConstructorShape>(Ambiguous("TwoConstructorShape", 2, "TwoConstructorShape(Int32 id) and TwoConstructorShape(Int32 id, String title)"));

	[Fact]
	public static void ThreeOrMorePublicConstructors_AreAllNamed_FewestParametersFirst_ForAClassAndAStruct()
	{
		AssertUnmappable<ThreeConstructorShape>(Ambiguous("ThreeConstructorShape", 3, "ThreeConstructorShape(Int32 a), ThreeConstructorShape(Int32 a, String b) and ThreeConstructorShape(String b, Int32 a, Int32 c)"));
		AssertUnmappable<TwoConstructorStructShape>(Ambiguous("TwoConstructorStructShape", 2, "TwoConstructorStructShape(Int32 id) and TwoConstructorStructShape(Int32 id, String title)"));
	}

	[Fact]
	public static void AConstructorParameterPassedByReference_IsRejected_AtTheCall()
		=> AssertUnmappable<ByReferenceShape>(ByReference("ByReferenceShape", "id"));

	// One public constructor binds, whatever else the type declares that is not public.
	[Fact]
	public static void AClassWithOnePublicConstructor_AndOthersThatAreNot_IsBuiltThroughThePublicOne()
	{
		OneOfSeveralShape item = new FakeExecuteReader(["Id"], [5]).Map<OneOfSeveralShape>().ToList().Single();
		Assert.Equal((5, "public"), (item.Id, item.Built));
	}

	[Fact]
	public static void APositionalRecord_IsMapped_ThroughItsConstructor()
	{
		Assert.Equal(new PositionalShape(1, "one"), new FakeExecuteReader(["Id", "Title"], [1, "one"]).Map<PositionalShape>().ToList().Single());
		Assert.Same(RowMap<PositionalShape>.Default, RowMap<PositionalShape>.Default);
	}

	// A record struct, whose constructor only assigns, maps to equal results through its constructor: T5 proves it in ConstructorBindingTests.
	[Fact]
	public static void RecordStructs_MapToTheSameResults_ThroughTheirConstructor()
	{
		Assert.Equal(new(1, "one"), new FakeExecuteReader(["Id", "Title"], [1, "one"]).Map<PositionalStructShape>().ToList().Single());
		Assert.Equal(new(2, "two"), new FakeExecuteReader(["Id", "Title"], [2, "two"]).Map<ReadonlyPositionalStructShape>().ToList().Single());
		Assert.Equal(4, FakeReader.Create(["Id", "Title"], [4, "four"]).Results<PositionalStructShape>().Single().Id);
		Assert.Same(RowMap<PositionalStructShape>.Default, RowMap<PositionalStructShape>.Default);
	}

	// A struct that declares exactly one public constructor with parameters is built through it, so what the constructor does is done: phase B kept such a struct as its default value
	// with its settable members set (3 and "three"), and the constructor was not called.
	[Fact]
	public static void AStructWithOneDeclaredConstructor_IsBuiltThroughIt()
	{
		StructWithConstructorOnly declared = new FakeExecuteReader(["Id", "Title"], [3, "three"]).Map<StructWithConstructorOnly>().ToList().Single();
		Assert.Equal((1003, "three!"), (declared.Id, declared.Title));

		StructWithConstructorOnly missing = new FakeExecuteReader(["Title"], ["x"]).Map<StructWithConstructorOnly>().ToList().Single();
		Assert.Equal((1000, "x!"), (missing.Id, missing.Title)); // A parameter with no column is its default, and then the constructor runs.
	}

	[Fact]
	public static void AStructWithAPublicParameterlessConstructor_IsBuiltAsEver_WithoutCallingIt()
		=> Assert.Equal(7, new FakeExecuteReader(["Id"], [7]).Map<StructWithPublicParameterlessConstructor>().ToList().Single().Id);

	[Fact]
	public static void ARequiredPublicField_IsRejected_AtTheCall_AnInheritedOneToo()
	{
		AssertUnmappable<RequiredFieldShape>(RequiredField("RequiredFieldShape", "Id"));
		AssertUnmappable<RequiredFieldDerived>(RequiredField("RequiredFieldDerived", "Title")); // A required field of a base type counts.
	}

	[Fact]
	public static void ADictionaryOrADynamicObject_IsRejected_AtTheCall_WhateverInterfaceMakesItOne()
	{
		AssertUnmappable<Dictionary<string, object?>>(Bag("Dictionary`2"));
		AssertUnmappable<System.Collections.Immutable.ImmutableDictionary<string, object?>>(Bag("ImmutableDictionary`2")); // it has no public constructor either: this is the reason given
		AssertUnmappable<System.Collections.Hashtable>(Bag("Hashtable")); // the non-generic IDictionary
		AssertUnmappable<DictionaryOfMine>(Bag("DictionaryOfMine")); // a type derived from one
		AssertUnmappable<OnlyReadOnlyDictionary>(Bag("OnlyReadOnlyDictionary")); // IReadOnlyDictionary<,> alone
		AssertUnmappable<ExpandoObject>(Bag("ExpandoObject"));
		AssertUnmappable<DynamicBag>(Bag("DynamicBag")); // a DynamicObject
		AssertUnmappable<OnlyMetaObjectProvider>(Bag("OnlyMetaObjectProvider")); // IDynamicMetaObjectProvider alone
	}

	[Fact]
	public static void AnInterfaceOrAbstractDictionaryType_KeepsItsOwnReason()
	{
		AssertUnmappable<IDictionary<string, object?>>(NotConcrete("IDictionary`2", "an interface"));
		AssertUnmappable<IReadOnlyDictionary<string, object?>>(NotConcrete("IReadOnlyDictionary`2", "an interface"));
		AssertUnmappable<IDynamicMetaObjectProvider>(NotConcrete("IDynamicMetaObjectProvider", "an interface"));
	}

	[Fact]
	public static void AModelThatHoldsADictionary_StillMaps()
		=> Assert.Equal(1, new FakeExecuteReader(["Id"], [1]).Map<HoldsADictionary>().ToList().Single().Id);

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

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior, CancellationToken cancellationToken)
			=> inner.ExecuteReaderAsync(state, transform, behavior, cancellationToken);

		public ValueTask<TResult> ExecuteReaderAsync<TState, TResult>(TState state, Func<IDataReader, TState, ValueTask<TResult>> transform, CommandBehavior behavior = CommandBehavior.Default)
			=> ExecuteReaderAsync(reader => transform(reader, state), behavior);

		public ValueTask<T> ExecuteReaderAsync<T>(Func<IDataReader, ValueTask<T>> transform, CommandBehavior behavior = CommandBehavior.Default) => inner.ExecuteReaderAsync(transform, behavior);
	}

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static AsyncCommand Async() => new(Exec());

	static Channel<T> Writer<T>() => System.Threading.Channels.Channel.CreateUnbounded<T>();

	// A call that returns a reader reports a failure through its Completion, as documented; a call that returns a task reports it on the task.
	// The call itself, when it fails before it returns anything.
	static Task Run(Action call)
	{
		call();
		return Task.CompletedTask;
	}

	static async Task Done<T>(ChannelReader<T> reader) => await reader.Completion;
	static async Task Done<T>(IReceivableSourceBlock<T> block) => await block.Completion;
	static async Task Done<T>(ValueTask<T> task) => await task;
	static async Task Done<T>(IAsyncEnumerable<T> source) { await foreach (T _ in source) { } }

	// name -> the call, for the model type T, over an empty result: one list serves every type-shape error, each family of entry points once.
	// (The calls are written for any T, so a type that cannot be mapped for another reason meets the same entry points: see the required field below.)
	static Dictionary<string, Func<Task>> CallsOf<T>() => new()
	{
		// Core: Map<T>() and its terminals (the call that fails is Map<T>() itself, so no terminal is ever reached)
		["Map<T>()"] = () => Run(() => Exec().Map<T>()),
		["Map<T>().ToList()"] = () => Run(() => Exec().Map<T>().ToList()),
		["Map<T>().FirstOrDefault()"] = () => Run(() => Exec().Map<T>().FirstOrDefault()),
		["Map<T>().Skip().Take().ToArray()"] = () => Run(() => Exec().Map<T>().Skip(1).Take(2).ToArray()),
		["Map<T>().ToListAsync()"] = async () => await Exec().Map<T>().ToListAsync(),
		["Map<T>().FirstOrDefaultAsync()"] = async () => await Exec().Map<T>().FirstOrDefaultAsync(),
		["Map<T>().ToDictionaryAsync()"] = async () => await Exec().Map<T>().ToDictionaryAsync(_ => 0),
		["Map<T>(pair)"] = () => Run(() => Exec().Map<T>(new KeyValuePair<string, string?>("Id", "Id")).ToList()),
		["Map<T>(pairs)"] = () => Run(() => Exec().Map<T>(new KeyValuePair<string, string?>("Id", "Id"), new("Name", "Name")).ToList()),

		// Core: Results
		["Results<T>(IDataReader)"] = () => Run(() => Reader().Results<T>()),
		["Results<T>(IDataReader, null)"] = () => Run(() => Reader().Results<T>(null)),
		["ResultsBuffered<T>(IDataReader)"] = () => Run(() => Reader().ResultsBuffered<T>()),
		["Results<T>(IDbCommand)"] = () => Run(() => Command().Results<T>()),
		["Results<T>(IExecuteReader)"] = () => Run(() => Exec().Results<T>()),
		["ResultsAsync<T>(DbDataReader)"] = () => Done(DbReader().ResultsAsync<T>()),
		["ResultsBufferedAsync<T>(DbDataReader)"] = async () => await DbReader().ResultsBufferedAsync<T>(),
		["ResultsAsync<T>(DbCommand)"] = async () => await DbCmd().ResultsAsync<T>(),

		// Core: DataTable and Retrieve
		["To<T>(DataTable)"] = () => Run(() => Table().To<T>()),
		["DequeueAs<T>(QueryResult)"] = () => Run(() => Queued().DequeueAs<T>()),
		["DequeueAs<T>(Task)"] = async () => await Task.FromResult(Queued()).DequeueAs<T>(),
		["DequeueAs<T>(ValueTask)"] = async () => await new ValueTask<QueryResult<Queue<object?[]>>>(Queued()).DequeueAs<T>(),
		["new Transformer<T>()"] = () => Run(() => Transformer<T>.Create()),

		// Channel: a new channel
		["ToChannel<T>(IDataReader)"] = () => Done(Reader().ToChannel<T>(true)),
		["ToChannel<T>(IDataReader, null)"] = () => Done(Reader().ToChannel<T>(true, (RowMap<T>?)null)),
		["ToChannel<T>(IDbCommand)"] = () => Done(Command().ToChannel<T>(true)),
		["ToChannel<T>(IExecuteReader)"] = () => Done(Exec().ToChannel<T>(true)),
		["ToChannelAsync<T>(DbDataReader)"] = () => Done(DbReader().ToChannelAsync<T>(true)),
		["ToChannelAsync<T>(DbCommand)"] = () => Done(DbCmd().ToChannelAsync<T>(true)),
		["ToChannelAsync<T>(IExecuteReaderAsync)"] = () => Done(Async().ToChannelAsync<T>(true)),

		// Channel: the caller's writer
		["ToChannel<T>(writer, IDataReader)"] = () => Done(Reader().ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannel<T>(writer, IDataReader, null)"] = () => Done(Reader().ToChannel<T>(Writer<T>().Writer, true, (RowMap<T>?)null)),
		["ToChannel<T>(writer, IDbCommand)"] = () => Done(Command().ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannel<T>(writer, IExecuteReader)"] = () => Done(Exec().ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(writer, DbDataReader)"] = () => Done(DbReader().ToChannelAsync<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(writer, DbCommand)"] = () => Done(DbCmd().ToChannelAsync<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(writer, IExecuteReaderAsync)"] = () => Done(Async().ToChannelAsync<T>(Writer<T>().Writer, true)),

		// Dataflow: into a block
		["ToTargetBlock<T>(IDataReader)"] = () => Done(new ValueTask<long>(Reader().ToTargetBlock(new BufferBlock<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlock<T>(IDbCommand)"] = () => Done(new ValueTask<long>(Command().ToTargetBlock(new BufferBlock<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlock<T>(IExecuteReader)"] = () => Done(new ValueTask<long>(Exec().ToTargetBlock(new BufferBlock<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlockAsync<T>(IDataReader)"] = () => Done(Reader().ToTargetBlockAsync(new BufferBlock<T>(), true, (RowMap<T>?)null)),
		["ToTargetBlockAsync<T>(IDbCommand)"] = () => Done(Command().ToTargetBlockAsync(new BufferBlock<T>(), true, (RowMap<T>?)null)),
		["ToTargetBlockAsync<T>(IExecuteReader)"] = () => Done(Exec().ToTargetBlockAsync(new BufferBlock<T>(), true, (RowMap<T>?)null)),

		// Dataflow: a new source block
		["AsSourceBlock<T>(IDataReader)"] = () => Done(Reader().AsSourceBlock<T>()),
		["AsSourceBlock<T>(IDbCommand)"] = () => Done(Command().AsSourceBlock<T>()),
		["AsSourceBlock<T>(IExecuteReader)"] = () => Done(Exec().AsSourceBlock<T>()),
		["AsSourceBlockAsync<T>(IDataReader)"] = () => Done(Reader().AsSourceBlockAsync<T>()),
		["AsSourceBlockAsync<T>(IDbCommand)"] = () => Done(Command().AsSourceBlockAsync<T>()),
		["AsSourceBlockAsync<T>(IExecuteReader)"] = () => Done(Exec().AsSourceBlockAsync<T>()),
	};

	static readonly Dictionary<string, Func<Task>> Calls = CallsOf<IShapeless>();

	public static TheoryData<string> Names => [.. Calls.Keys];

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task EveryEntryPoint_Throws_ForAnUnmappableType_WithAnEmptyResult(string name)
	{
		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Calls[name]().WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		Assert.Equal(NotConcrete("IShapeless", "an interface"), ex.Message);
	}

	// T17: a required public field, over the same entry points, with an empty result: the same exception at the call.
	static readonly Dictionary<string, Func<Task>> RequiredFieldCalls = CallsOf<RequiredFieldShape>();

	[Theory]
	[MemberData(nameof(Names))]
	public static async Task EveryEntryPoint_Throws_ForARequiredField_WithAnEmptyResult(string name)
	{
		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => RequiredFieldCalls[name]().WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		Assert.Equal(RequiredField("RequiredFieldShape", "Id"), ex.Message);
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

	// ---- the check runs BEFORE anything is executed: an unmappable T fails at the call with no side effect ----

	// What was done to a receiver: commands executed, connections opened, readers read. Zero is "nothing".
	sealed class Touched { public Func<int> Count { get; set; } = static () => 0; }

	static int CountCalls(object substitute, Func<string, bool> named) => substitute.ReceivedCalls().Count(c => named(c.GetMethodInfo().Name));

	static IDbCommand Command(Touched touched)
	{
		var command = Substitute.For<IDbCommand>();
		var connection = Substitute.For<IDbConnection>();
		command.Connection.Returns(connection);
		connection.State.Returns(ConnectionState.Closed); // An execution would have to open it.
		command.ExecuteReader(Arg.Any<CommandBehavior>()).Returns(_ => FakeReader.Create(["Id"]));

		int Total() => CountCalls(command, n => n.StartsWith("Execute", StringComparison.Ordinal)) + CountCalls(connection, n => n.StartsWith("Open", StringComparison.Ordinal));
		int baseline = Total();
		touched.Count = () => Total() - baseline;
		return command;
	}

	static FakeExecuteReader Exec(Touched touched)
	{
		FakeExecuteReader command = Exec();
		touched.Count = () => command.ExecuteCount;
		return command;
	}

	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static AsyncCommand Async(Touched touched) => new(Exec(touched));

	static FakeDbCommand DbCmd(Touched touched)
	{
		var connection = new FakeDbConnection(); // Closed: an execution would open it.
		var command = new FakeDbCommand(connection, () => FakeReader.CreateDb(["Id"]));
		touched.Count = () => command.Behaviors.Count + connection.OpenCalls;
		return command;
	}

	static bool ReadsOrCloses(string name) => name is "Read" or "ReadAsync" or "NextResult" or "NextResultAsync" or "GetSchemaTable" or "GetSchemaTableAsync" or "Close" or "Dispose" or "DisposeAsync";

	static IDataReader Reader(Touched touched)
	{
		IDataReader reader = Reader();
		touched.Count = () => CountCalls(reader, ReadsOrCloses);
		return reader;
	}

	static DbDataReader DbReader(Touched touched)
	{
		DbDataReader reader = DbReader();
		touched.Count = () => CountCalls(reader, ReadsOrCloses);
		return reader;
	}

	// A closed connection that hands out one fake command, for the ExpressiveDbCommand.
	sealed class ClosedConnection : DbConnection
	{
		public int OpenCalls { get; private set; }
		public FakeDbCommand? Created { get; private set; }

		[AllowNull]
		public override string ConnectionString { get; set; } = "";
		public override string Database => "fake";
		public override string DataSource => "fake";
		public override string ServerVersion => "1.0";
		public override ConnectionState State => ConnectionState.Closed;
		public override void ChangeDatabase(string databaseName) { }
		public override void Open() => OpenCalls++;
		public override void Close() { }
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
		protected override DbCommand CreateDbCommand() => Created = new FakeDbCommand(this, () => FakeReader.CreateDb(["Id"]));
	}

	static ExpressiveDbCommand Expressive(Touched touched)
	{
		var connection = new ClosedConnection();
		touched.Count = () => (connection.Created?.Behaviors.Count ?? 0) + connection.OpenCalls;
		return connection.Command("SELECT 1");
	}

	static BufferBlock<T> Target<T>() => new();

	// name -> the call, for the model type T: over a command that must never be executed or a reader that must never be read.
	static Dictionary<string, Func<Touched, Task>> BeforeExecutionOf<T>() => new()
	{
		// Core: a command
		["Results<T>(IDbCommand)"] = t => Run(() => Command(t).Results<T>()),
		["Results<T>(IDbCommand, null)"] = t => Run(() => Command(t).Results<T>(null)),
		["Results<T>(IExecuteReader)"] = t => Run(() => Exec(t).Results<T>()),
		["Results<T>(IExecuteReader, null)"] = t => Run(() => Exec(t).Results<T>(null)),
		["Map<T>() (IExecuteReader)"] = t => Run(() => Exec(t).Map<T>().ToList()),
		["ResultsAsync<T>(DbCommand)"] = async t => await DbCmd(t).ResultsAsync<T>(),
		["ResultsAsync<T>(DbCommand, null, token)"] = async t => await DbCmd(t).ResultsAsync<T>(null, CancellationToken.None),
		["ResultsAsync<T>(DbCommand, token)"] = async t => await DbCmd(t).ResultsAsync<T>(CancellationToken.None),
		["ResultsFromDataTable<T>(IDbCommand)"] = t => Run(() => Command(t).ResultsFromDataTable<T>()),
		["ExpressiveDbCommand.ResultsAsync<T>()"] = async t => await Expressive(t).ResultsAsync<T>(),
		["ExpressiveDbCommand.ResultsAsync<T>(null)"] = async t => await Expressive(t).ResultsAsync<T>(null),

		// Core: a reader
		["Results<T>(IDataReader)"] = t => Run(() => Reader(t).Results<T>()),
		["ResultsBuffered<T>(IDataReader)"] = t => Run(() => Reader(t).ResultsBuffered<T>()),
		["ResultsAsync<T>(DbDataReader)"] = t => Done(DbReader(t).ResultsAsync<T>()),
		["ResultsBufferedAsync<T>(DbDataReader)"] = async t => await DbReader(t).ResultsBufferedAsync<T>(),
		["ResultsFromDataTable<T>(IDataReader)"] = t => Run(() => Reader(t).ResultsFromDataTable<T>()),

		// Channel: a command, a new channel
		["ToChannel<T>(IDbCommand)"] = t => Done(Command(t).ToChannel<T>(true)),
		["ToChannel<T>(IDbCommand, null)"] = t => Done(Command(t).ToChannel<T>(true, (RowMap<T>?)null)),
		["ToChannel<T>(IExecuteReader)"] = t => Done(Exec(t).ToChannel<T>(true)),
		["ToChannelAsync<T>(DbCommand)"] = t => Done(DbCmd(t).ToChannelAsync<T>(true)),
		["ToChannelAsync<T>(IExecuteReaderAsync)"] = t => Done(Async(t).ToChannelAsync<T>(true)),

		// Channel: a command, the caller's writer
		["ToChannel<T>(writer, IDbCommand)"] = t => Done(Command(t).ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannel<T>(writer, IDbCommand, null)"] = t => Done(Command(t).ToChannel<T>(Writer<T>().Writer, true, (RowMap<T>?)null)),
		["ToChannel<T>(writer, IExecuteReader)"] = t => Done(Exec(t).ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(writer, DbCommand)"] = t => Done(DbCmd(t).ToChannelAsync<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(writer, IExecuteReaderAsync)"] = t => Done(Async(t).ToChannelAsync<T>(Writer<T>().Writer, true)),

		// Channel: a reader
		["ToChannel<T>(IDataReader)"] = t => Done(Reader(t).ToChannel<T>(true)),
		["ToChannel<T>(writer, IDataReader)"] = t => Done(Reader(t).ToChannel<T>(Writer<T>().Writer, true)),
		["ToChannelAsync<T>(DbDataReader)"] = t => Done(DbReader(t).ToChannelAsync<T>(true)),
		["ToChannelAsync<T>(writer, DbDataReader)"] = t => Done(DbReader(t).ToChannelAsync<T>(Writer<T>().Writer, true)),

		// Dataflow: a command
		["ToTargetBlock<T>(IDbCommand)"] = t => Done(new ValueTask<long>(Command(t).ToTargetBlock(Target<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlock<T>(IExecuteReader)"] = t => Done(new ValueTask<long>(Exec(t).ToTargetBlock(Target<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlockAsync<T>(IDbCommand)"] = t => Done(Command(t).ToTargetBlockAsync(Target<T>(), true, (RowMap<T>?)null)),
		["ToTargetBlockAsync<T>(IExecuteReader)"] = t => Done(Exec(t).ToTargetBlockAsync(Target<T>(), true, (RowMap<T>?)null)),
		["AsSourceBlock<T>(IDbCommand)"] = t => Done(Command(t).AsSourceBlock<T>()),
		["AsSourceBlock<T>(IExecuteReader)"] = t => Done(Exec(t).AsSourceBlock<T>()),
		["AsSourceBlockAsync<T>(IDbCommand)"] = t => Done(Command(t).AsSourceBlockAsync<T>()),
		["AsSourceBlockAsync<T>(IExecuteReader)"] = t => Done(Exec(t).AsSourceBlockAsync<T>()),

		// Dataflow: a reader
		["ToTargetBlock<T>(IDataReader)"] = t => Done(new ValueTask<long>(Reader(t).ToTargetBlock(Target<T>(), true, (RowMap<T>?)null))),
		["ToTargetBlockAsync<T>(IDataReader)"] = t => Done(Reader(t).ToTargetBlockAsync(Target<T>(), true, (RowMap<T>?)null)),
		["AsSourceBlock<T>(IDataReader)"] = t => Done(Reader(t).AsSourceBlock<T>()),
		["AsSourceBlockAsync<T>(IDataReader)"] = t => Done(Reader(t).AsSourceBlockAsync<T>()),
	};

	static readonly Dictionary<string, Func<Touched, Task>> BeforeExecution = BeforeExecutionOf<IShapeless>();

	public static TheoryData<string> BeforeExecutionNames => [.. BeforeExecution.Keys];

	[Theory]
	[MemberData(nameof(BeforeExecutionNames))]
	public static async Task AnUnmappableType_Fails_BeforeAnythingIsExecutedOrRead(string name)
	{
		var touched = new Touched();
		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => BeforeExecution[name](touched).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		Assert.Equal(NotConcrete("IShapeless", "an interface"), ex.Message);
		Assert.Equal(0, touched.Count()); // No command was executed, no connection opened, no reader read: the failure had no side effect.
	}

	static readonly Dictionary<string, Func<Touched, Task>> RequiredFieldBeforeExecution = BeforeExecutionOf<RequiredFieldShape>();

	[Theory]
	[MemberData(nameof(BeforeExecutionNames))]
	public static async Task ARequiredField_Fails_BeforeAnythingIsExecutedOrRead(string name)
	{
		var touched = new Touched();
		InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => RequiredFieldBeforeExecution[name](touched).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
		Assert.Equal(RequiredField("RequiredFieldShape", "Id"), ex.Message);
		Assert.Equal(0, touched.Count());
	}

	// The probes themselves: a mappable type does execute the command, so a zero above means something.
	[Fact]
	public static async Task TheProbes_SeeAnExecution_OfAMappableType()
	{
		var touched = new Touched();
		_ = Command(touched).Results<ParameterlessAndOther>().ToList();
		Assert.True(touched.Count() > 0);

		touched = new Touched();
		_ = await DbCmd(touched).ResultsAsync<ParameterlessAndOther>(TestContext.Current.CancellationToken);
		Assert.True(touched.Count() > 0);

		touched = new Touched();
		_ = await Expressive(touched).ResultsAsync<ParameterlessAndOther>();
		Assert.True(touched.Count() > 0);

		touched = new Touched();
		_ = Exec(touched).Results<ParameterlessAndOther>().ToList();
		Assert.True(touched.Count() > 0);
	}
}
