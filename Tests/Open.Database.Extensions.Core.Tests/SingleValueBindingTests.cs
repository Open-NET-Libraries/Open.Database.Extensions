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

}
