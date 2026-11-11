#nullable enable

namespace Open.Database.Extensions.Tests;

// Compile-only: every call form of the new Map<T> on IDataReader and
// IDbCommand binds to the type it returns,
// and the calls that existed bind as they did.
// Each call is pinned to the type it returns,
// so a change of binding breaks the build.
//
// A type that is both an IExecuteReader and an IDataReader (or IDbCommand)
// would make reader.Map<T>() ambiguous (CS0121).
// None exists in this library, and none of the expressive commands
// implements IDbCommand or IDataReader (checked by hand;
// a test cannot assert a compile error).
// The reader extensions have no Map of their own,
// so reader.Map(selector) has nothing else to bind to.
[ExcludeFromCodeCoverage]
public static class MappedViewBindingTests
{
	sealed record Item(int Id);

	static void Pin<T>(T _) { }

	static int Id(IDataRecord r)
		=> r.GetInt32(0);

	static readonly Action<
		IDataReader,
		IDbCommand,
		System.Data.Common.DbCommand,
		ExpressiveDbCommand,
		IExecuteReader,
		RowMap<Item>> Table = static (reader, command, dbCommand, expressive, exec, map) =>
	{
		// The reader.
		Pin(reader.Map<Item>());
		Pin(reader.Map(map));
		Pin(reader.Map((RowMap<Item>?)null));
		Pin(reader.Map<Item>(null));
		Pin(reader.Map<int>());
		Pin(reader.Map(Id));
		Pin(reader.Map(r => r.GetInt32(0)));
		Pin(
			reader.Map<Item>(new KeyValuePair<string, string?>("Id", "N")));
		Pin(
			reader.Map<Item>(
				new KeyValuePair<string, string?>("Id", "N"),
				new KeyValuePair<string, string?>("Id", "M")));
		Pin(reader.Map<Item>([]));

		// The command, and a DbCommand, which is also an IDbCommand.
		Pin(command.Map<Item>());
		Pin(command.Map(map));
		Pin(command.Map((RowMap<Item>?)null));
		Pin(command.Map<Item>(null));
		Pin(command.Map<int>());
		Pin(command.Map(Id));
		Pin(
			command.Map<Item>(new KeyValuePair<string, string?>("Id", "N")));
		Pin(
			command.Map<Item>(
				new KeyValuePair<string, string?>("Id", "N"),
				new KeyValuePair<string, string?>("Id", "M")));
		Pin(dbCommand.Map<Item>());
		Pin(dbCommand.Map(Id));

		// What existed still binds as it did.
		Pin(exec.Map<Item>());
		Pin(exec.Map(map));
		Pin(exec.Map((RowMap<Item>?)null));
		Pin(exec.Map<Item>(null));
		Pin(exec.Map(Id));
		Pin(expressive.Map<int>());
		Pin(expressive.Map(map));
		Pin(expressive.Map(r => r.GetInt32(0)));
	};

	[Fact]
	public static void TheTableCompiles()
		=> Assert.NotNull(Table);

	// A bare null: which overload takes it is pinned by what it does.
	// It reads like the default map, by column name.
	[Fact]
	public static void ABareNull_ReadsLikeTheDefaultMap()
	{
		static IDataReader Rd() => ReaderFixtures.Reader([1, "a", null]);

		Assert.Equal(1, Rd().Map<Item>(null).FirstOrDefault()!.Id);
		Assert.Equal(
			1,
			new FakeExecuteReader(() => ReaderFixtures.Reader([1, "a", null]))
				.Map<Item>(null)
				.FirstOrDefault()!.Id);
		Assert.Equal(
			1,
			new FakeDbCommand(new FakeDbConnection(), () => ReaderFixtures.Reader([1, "a", null]))
				.Map<Item>(null)
				.FirstOrDefault()!.Id);
	}
}
