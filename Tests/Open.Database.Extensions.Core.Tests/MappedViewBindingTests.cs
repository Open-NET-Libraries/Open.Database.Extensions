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
		Pin<MappedReader<Item>>(reader.Map<Item>());
		Pin<MappedReader<Item>>(reader.Map(map));
		Pin<MappedReader<Item>>(reader.Map<Item>((RowMap<Item>?)null));
		Pin<MappedReader<Item>>(reader.Map<Item>(null));
		Pin<MappedReader<int>>(reader.Map<int>());
		Pin<MappedReader<int>>(reader.Map(Id));
		Pin<MappedReader<int>>(reader.Map(r => r.GetInt32(0)));
		Pin<MappedReader<Item>>(
			reader.Map<Item>(new KeyValuePair<string, string?>("Id", "N")));
		Pin<MappedReader<Item>>(
			reader.Map<Item>(
				new KeyValuePair<string, string?>("Id", "N"),
				new KeyValuePair<string, string?>("Id", "M")));
		Pin<MappedReader<Item>>(reader.Map<Item>([]));

		// The command, and a DbCommand, which is also an IDbCommand.
		Pin<MappedCommand<Item>>(command.Map<Item>());
		Pin<MappedCommand<Item>>(command.Map(map));
		Pin<MappedCommand<Item>>(command.Map<Item>((RowMap<Item>?)null));
		Pin<MappedCommand<Item>>(command.Map<Item>(null));
		Pin<MappedCommand<int>>(command.Map<int>());
		Pin<MappedCommand<int>>(command.Map(Id));
		Pin<MappedCommand<Item>>(
			command.Map<Item>(new KeyValuePair<string, string?>("Id", "N")));
		Pin<MappedCommand<Item>>(
			command.Map<Item>(
				new KeyValuePair<string, string?>("Id", "N"),
				new KeyValuePair<string, string?>("Id", "M")));
		Pin<MappedCommand<Item>>(dbCommand.Map<Item>());
		Pin<MappedCommand<int>>(dbCommand.Map(Id));

		// What existed still binds as it did.
		Pin<MappedQuery<Item>>(exec.Map<Item>());
		Pin<MappedQuery<Item>>(exec.Map(map));
		Pin<MappedQuery<Item>>(exec.Map<Item>((RowMap<Item>?)null));
		Pin<MappedQuery<Item>>(exec.Map<Item>(null));
		Pin<MappedQuery<int>>(exec.Map(Id));
		Pin<MappedQuery<int>>(expressive.Map<int>());
		Pin<MappedQuery<Item>>(expressive.Map(map));
		Pin<MappedQuery<int>>(expressive.Map(r => r.GetInt32(0)));
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
