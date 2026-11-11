#pragma warning disable CS0618 // The transform overloads are being deprecated; this file pins that they still bind as they did.

#nullable enable

namespace Open.Database.Extensions.Tests;

// The terminals that take the type are extensions on IExecuteReader
// next to the transform overloads of the same names.
// Every call that compiled before binds as it did;
// the new calls bind to the new members.
// The compile-only part is the table:
// each call is pinned to the type it returns,
// so a change of binding breaks the build.
//
// These must NOT compile (checked by hand when the table changes;
// a test cannot assert a compile error):
//   cmd.ToList<int>();                  CS7036
//     (only the async terminals take the type;
//     a sync caller writes cmd.Map<int>().ToList())
//   cmd.ToList();                       CS7036
//   cmd.ToHashSetAsync<int>(ct);        CS1503
//     (the comparer comes first, as on MappedQuery<T>)
//   iExecuteReader.ToListAsync(ct);     CS0411 (no T)
//   iExecuteReader.ToListAsync(r => 1); CS1660
//     (a lambda where a CancellationToken is expected)
//   rawDbCommand.ToList<int>();         CS1061
//     (nothing new on IDbCommand, DbCommand or IDataReader)
[ExcludeFromCodeCoverage]
public static class SingleValueBindingTests
{
	static CancellationToken Ct => TestContext.Current.CancellationToken;

	static void Pin<T>(T _) { }
	[SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'")]
	static void PinAsync<T>(ValueTask<T> _) { }

	static int Id(IDataRecord r)
		=> r.GetInt32(0);

	// Compiled, never run:
	// a lambda in a field keeps the table free of unused variables.
	static readonly Action<
		ExpressiveDbCommand,
		IExecuteReader,
		CancellationToken> Table = static (cmd, exec, ct) =>
	{
		// The transform overloads, as before.
		Pin(cmd.ToList(r => r.GetInt32(0)));
		Pin(cmd.ToList(Id));
		Pin(cmd.ToList(Id, CommandBehavior.SequentialAccess));
		Pin(cmd.ToList<int>(null!));
		Pin(cmd.ToArray(r => r.GetInt32(0)));
		Pin(cmd.ToArray(Id, CommandBehavior.SequentialAccess));
		Pin(cmd.ToImmutableArray(Id));
		Pin(
			cmd.ToImmutableArray(Id, CommandBehavior.SequentialAccess));
		Pin(cmd.FirstOrDefault(Id));
		Pin(cmd.SingleOrDefault(Id));
		Pin(cmd.First(Id));
		Pin(cmd.Single(Id));
		Pin(exec.ToList(Id));
		PinAsync(cmd.ToListAsync(r => r.GetInt32(0)));
		PinAsync(
			cmd.ToListAsync(
				r => r.GetInt32(0),
				CommandBehavior.SequentialAccess));

		// The new terminals.

		PinAsync(cmd.ToListAsync<int>());
		PinAsync(cmd.ToListAsync<int>(ct));
		PinAsync(cmd.ToListAsync<int>(cancellationToken: ct));
		PinAsync(cmd.ToArrayAsync<int>());
		PinAsync(cmd.ToArrayAsync<int>(ct));
		PinAsync(cmd.ToArrayAsync<int>(cancellationToken: ct));
		PinAsync(cmd.ToImmutableArrayAsync<int>(ct));
		PinAsync<int>(cmd.FirstOrDefaultAsync<int>(ct));
		PinAsync<int>(cmd.SingleOrDefaultAsync<int>(ct));
		PinAsync(cmd.ToHashSetAsync<int>());
		PinAsync(
			cmd.ToHashSetAsync(EqualityComparer<int>.Default, ct));
		PinAsync(cmd.ToHashSetAsync<int>(cancellationToken: ct));
		PinAsync(exec.ToListAsync<int>(ct));
		PinAsync(exec.ToArrayAsync<int>(ct));

		// A row model,
		// and the Map twin (a MappedQuery is not an IExecuteReader,
		// so its own terminals are untouched).
		PinAsync(
			cmd.ToListAsync<SingleValueTerminalTests.PostRow>(ct));
		Pin(cmd.Map<int>().ToList());
		Pin(cmd.Map(Id).ToList());
	};

	[Fact]
	public static void TheTableCompiles()
		=> Assert.NotNull(Table);

	// The one trap:
	// ToListAsync<int>(default!)
	// (or null) still binds to the instance transform overload
	// of an expressive command,
	// not to the new extension.
	// With no transform that overload has no guard:
	// an empty result returns [] and a row throws.
	[Fact]
	public static async Task ToListAsyncOfIntWithDefault_BindsToTheInstanceTransformOverload_OfAnExpressiveCommand()
	{
		var rig = new TokenRig { Rows = 1 };
		static ExpressiveDbCommand Command(TokenRig r)
			=> new(new TokenConnection(r), CommandType.Text, "SELECT Id");

		Assert.Equal([1], await Command(rig).ToListAsync<int>(Ct)); // the token: the new extension
		await Assert.ThrowsAsync<NullReferenceException>(
			async () => await Command(rig).ToListAsync<int>(default!));

		rig.Rows = 0;
		Assert.Empty(await Command(rig).ToListAsync<int>(default!)); // no row, so the missing transform is never called
	}
}
