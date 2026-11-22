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

	// A struct with its own public parameterless constructor is built with new(), whatever else it declares: that constructor is called, then the properties are set.
	public struct StructWithPublicParameterlessConstructor
	{
		public StructWithPublicParameterlessConstructor() { Id = -1; Source = "parameterless constructor"; }
		public StructWithPublicParameterlessConstructor(int id) => Id = id + 1000;
		public int Id { get; set; }
		public string? Source { get; }
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

	static string Single(string name)
		=> $"'{name}' is a single value, not a row model. On a command use Map<T>(), which reads the first column; else ExecuteScalar<T>().";
	static string NotConcrete(string name, string kind) => $"'{name}' cannot be mapped: it is {kind}. Map to a class or struct, or use Map(selector).";
	static string NoConstructor(string name) => $"'{name}' cannot be mapped: it has no public constructor.";
	static string Ambiguous(string name, int count, string signatures) => $"'{name}' cannot be mapped: it has {count} public constructors, {signatures}, and none without parameters. Make one of them non-public, or use Map(selector).";
	static string ByReference(string name, string parameter) => $"'{name}' cannot be mapped: its constructor parameter '{parameter}' is passed by reference or is a pointer. Use Map(selector).";
	static string RequiredField(string name, string field) => $"'{name}' cannot be mapped: its required field '{field}' cannot be set. Make it a property.";
	static string Bag(string name) => $"'{name}' cannot be mapped: it is a dictionary or a dynamic object, which has no properties to set from the columns. Map to a class or record, use AsDictionaryRows() for the values with their column names, or use Map(selector).";

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
	static void AssertUnmappable<T>(
		string message,
		bool mapReadsTheFirstColumn = false)
	{
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.Default).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.Default).Message); // Reading it again throws again: it is not a failed type initializer.
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => new RowMap<T>()).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => new RowMap<T>([])).Message);
		Assert.Equal(message, Assert.Throws<InvalidOperationException>(() => RowMap<T>.From([])).Message);

		var command = new FakeExecuteReader(["Id"]);
		if (mapReadsTheFirstColumn)
		{
			_ = command.Map<T>(); // a single value is read from the first column: Map<T>() works, and runs nothing yet
		}
		else
		{
			Assert.Equal(
				message,
				Assert.Throws<InvalidOperationException>(
						() => command.Map<T>())
					.Message);
		}

		Assert.Equal(0, command.ExecuteCount); // Nothing ran: the call failed before the query was executed.

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
		AssertUnmappable<int>(Single("Int32"), true);
		AssertUnmappable<string>(Single("String"), true);
		AssertUnmappable<int?>(Single("Int32?"), true);
		AssertUnmappable<Guid>(Single("Guid"), true);
		AssertUnmappable<decimal>(Single("Decimal"), true);
		AssertUnmappable<DateTime>(Single("DateTime"), true);
		AssertUnmappable<DateTimeOffset?>(Single("DateTimeOffset?"), true);
		AssertUnmappable<TimeSpan>(Single("TimeSpan"), true);
		AssertUnmappable<bool>(Single("Boolean"), true);
		AssertUnmappable<ShapeKind>(Single("ShapeKind"), true);
		AssertUnmappable<ShapeKind?>(Single("ShapeKind?"), true);
		AssertUnmappable<byte[]>(Single("Byte[]"), true);
		AssertUnmappable<DateOnly>(Single("DateOnly"), true);
		AssertUnmappable<TimeOnly?>(Single("TimeOnly?"), true);
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
	public static void AStructWithAPublicParameterlessConstructor_IsBuiltWithIt()
	{
		StructWithPublicParameterlessConstructor mapped = new FakeExecuteReader(["Id"], [7]).Map<StructWithPublicParameterlessConstructor>().ToList().Single();
		Assert.Equal((7, "parameterless constructor"), (mapped.Id, mapped.Source)); // Source is set only by the constructor, so it ran, and the column was set after it.

		StructWithPublicParameterlessConstructor missing = new FakeExecuteReader(["Other"], [7]).Map<StructWithPublicParameterlessConstructor>().ToList().Single();
		Assert.Equal((-1, "parameterless constructor"), (missing.Id, missing.Source)); // No column feeds Id, so what the constructor gave it stays.
	}

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
		AssertUnmappable<ImmutableDictionary<string, object?>>(Bag("ImmutableDictionary`2")); // it has no public constructor either: this is the reason given
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
		["ToListAsync<T>()"] = async () => await Exec().Map<T>().ToListAsync(),
		["ToArrayAsync<T>()"] = async () => await Exec().Map<T>().ToArrayAsync(),
		["ToImmutableArrayAsync<T>()"]
			= async () => await Exec().Map<T>().ToImmutableArrayAsync(),
		["FirstOrDefaultAsync<T>()"]
			= async () => await Exec().Map<T>().FirstOrDefaultAsync(),
		["SingleOrDefaultAsync<T>()"]
			= async () => await Exec().Map<T>().SingleOrDefaultAsync(),
		["ToHashSetAsync<T>()"] = async () => await Exec().Map<T>().ToHashSetAsync(),
		["Map(new RowMap<T>(pair))"] = () => Run(() => Exec().Map(new RowMap<T>(new KeyValuePair<string, string?>("Id", "Id"))).ToList()),
		["Map(new RowMap<T>(pairs))"] = () => Run(() => Exec().Map(new RowMap<T>(new KeyValuePair<string, string?>("Id", "Id"), new("Name", "Name"))).ToList()),

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

	static BufferBlock<T> Target<T>() => new();

	// name -> the call, for the model type T: over a command that must never be executed or a reader that must never be read.
	static Dictionary<string, Func<Touched, Task>> BeforeExecutionOf<T>() => new()
	{
		["Map<T>() (IExecuteReader)"] = t => Run(() => Exec(t).Map<T>().ToList()),

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
}

