#pragma warning disable CS0618 // The transform overloads are being deprecated; this file pins that they still bind as they did.

#nullable enable

namespace Open.Database.Extensions.Tests;

// The terminals that take the type are extensions on IExecuteReader next to the transform overloads of the same names. Every call that compiled before binds as it did;
// the new calls bind to the new members. The compile-only part is the table: each call is pinned to the type it returns, so a change of binding breaks the build.
//
// These must NOT compile (checked by hand when the table changes; a test cannot assert a compile error):
//   cmd.ToList<int>();                  CS7036 (only the async terminals take the type; a sync caller writes cmd.Map<int>().ToList())
//   cmd.ToList();                       CS7036
//   cmd.ToHashSetAsync<int>(ct);        CS1503 (the comparer comes first, as on MappedQuery<T>)
//   iExecuteReader.ToListAsync(ct);     CS0411 (no T)
//   iExecuteReader.ToListAsync(r => 1); CS1660 (a lambda where a CancellationToken is expected)
//   rawDbCommand.ToList<int>();         CS1061 (nothing new on IDbCommand, DbCommand or IDataReader)
[ExcludeFromCodeCoverage]
public static class SingleValueBindingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static void Pin<T>(T _) { }
	static void PinAsync<T>(ValueTask<T> _) { }

	static int Id(IDataRecord r) => r.GetInt32(0);

	// Compiled, never run: a lambda in a field keeps the table free of unused variables.
	static readonly Action<ExpressiveDbCommand, IExecuteReader, CancellationToken> Table = static (cmd, exec, ct) =>
	{
		// The transform overloads, as before.
		Pin<List<int>>(cmd.ToList(r => r.GetInt32(0)));
		Pin<List<int>>(cmd.ToList(Id));
		Pin<List<int>>(cmd.ToList(Id, CommandBehavior.SequentialAccess));
		Pin<List<int>>(cmd.ToList<int>(null!));
		Pin<int[]>(cmd.ToArray(r => r.GetInt32(0)));
		Pin<int[]>(cmd.ToArray(Id, CommandBehavior.SequentialAccess));
		Pin<ImmutableArray<int>>(cmd.ToImmutableArray(Id));
		Pin<ImmutableArray<int>>(cmd.ToImmutableArray(Id, CommandBehavior.SequentialAccess));
		Pin<int>(cmd.FirstOrDefault(Id));
		Pin<int>(cmd.SingleOrDefault(Id));
		Pin<int>(cmd.First(Id));
		Pin<int>(cmd.Single(Id));
		Pin<List<int>>(exec.ToList(Id));
		PinAsync<List<int>>(cmd.ToListAsync(r => r.GetInt32(0)));
		PinAsync<List<int>>(cmd.ToListAsync(r => r.GetInt32(0), CommandBehavior.SequentialAccess));

		// The new terminals.

		PinAsync<List<int>>(cmd.ToListAsync<int>());
		PinAsync<List<int>>(cmd.ToListAsync<int>(ct));
		PinAsync<List<int>>(cmd.ToListAsync<int>(cancellationToken: ct));
		PinAsync<int[]>(cmd.ToArrayAsync<int>());
		PinAsync<int[]>(cmd.ToArrayAsync<int>(ct));
		PinAsync<int[]>(cmd.ToArrayAsync<int>(cancellationToken: ct));
		PinAsync<ImmutableArray<int>>(cmd.ToImmutableArrayAsync<int>(ct));
		PinAsync<int>(cmd.FirstOrDefaultAsync<int>(ct));
		PinAsync<int>(cmd.SingleOrDefaultAsync<int>(ct));
		PinAsync<HashSet<int>>(cmd.ToHashSetAsync<int>());
		PinAsync<HashSet<int>>(cmd.ToHashSetAsync<int>(EqualityComparer<int>.Default, ct));
		PinAsync<HashSet<int>>(cmd.ToHashSetAsync<int>(cancellationToken: ct));
		PinAsync<List<int>>(exec.ToListAsync<int>(ct));
		PinAsync<int[]>(exec.ToArrayAsync<int>(ct));

		// A row model, and the Map twin (a MappedQuery is not an IExecuteReader, so its own terminals are untouched).
		PinAsync<List<SingleValueTerminalTests.PostRow>>(cmd.ToListAsync<SingleValueTerminalTests.PostRow>(ct));
		Pin<List<int>>(cmd.Map<int>().ToList());
		Pin<List<int>>(cmd.Map(Id).ToList());
	};

	[Fact]
	public static void TheTableCompiles() => Assert.NotNull(Table);

	// The one trap: ToListAsync<int>(default!) (or null) still binds to the instance transform overload of an expressive command, not to the new extension.
	// With no transform that overload has no guard: an empty result returns [] and a row throws.
	[Fact]
	public static async Task ToListAsyncOfIntWithDefault_BindsToTheInstanceTransformOverload_OfAnExpressiveCommand()
	{
		var rig = new TokenRig { Rows = 1 };
		static ExpressiveDbCommand Command(TokenRig r) => new(new TokenConnection(r), CommandType.Text, "SELECT Id");

		Assert.Equal([1], await Command(rig).ToListAsync<int>(Ct)); // the token: the new extension
		await Assert.ThrowsAsync<NullReferenceException>(async () => await Command(rig).ToListAsync<int>(default!));

		rig.Rows = 0;
		Assert.Empty(await Command(rig).ToListAsync<int>(default!)); // no row, so the missing transform is never called
	}
}
