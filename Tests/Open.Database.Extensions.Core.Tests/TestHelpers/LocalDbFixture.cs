using Microsoft.Data.SqlClient;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// SQL Server LocalDB for the few tests that need a real server (the SQL Server package's typed <c>SqlCommand</c> and <c>SqlTransaction</c> extensions, which fakes cannot stand in for).
/// Call <see cref="SkipUnlessAvailable"/> first in such a test: it reports the test as skipped, not as passed or failed, when LocalDB cannot be reached, so the suite stays green on a machine or build agent without it.
/// </summary>
/// <remarks>
/// The reachability check runs once per test run. It is the same connection every test uses, so a machine without LocalDB fails fast (the instance does not exist) instead of waiting for a timeout in every test.
/// Setting the environment variable named by <see cref="ForceSkipVariable"/> skips every server test without looking for LocalDB, which is how to see the suite as a machine without LocalDB sees it.
/// Everything the tests create lives in <c>tempdb</c> under a name made unique by a new <see cref="Guid"/>, and is dropped when its <see cref="Scratch"/> is disposed.
/// </remarks>
[ExcludeFromCodeCoverage]
internal static class LocalDbFixture
{
	/// <summary>The environment variable that, when it has any value, skips every test that needs LocalDB.</summary>
	public const string ForceSkipVariable = "ODE_TEST_NO_LOCALDB";

	/// <summary>The default instance every Visual Studio and SQL Server Express LocalDB install creates, working in <c>tempdb</c>.</summary>
	public const string ConnectionString = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Integrated Security=true;Encrypt=False;Connect Timeout=20";

	static readonly Lazy<string?> Unavailable = new(Probe, LazyThreadSafetyMode.ExecutionAndPublication);

	// null when the server answered, else the reason the tests are skipped.
	static string? Probe()
	{
		if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ForceSkipVariable)))
			return $"{ForceSkipVariable} is set: the tests that need SQL Server LocalDB are skipped.";

		try
		{
			using var connection = new SqlConnection(ConnectionString);
			connection.Open();
			return null;
		}
		catch (Exception ex) when (ex is SqlException or InvalidOperationException or PlatformNotSupportedException or TimeoutException or ArgumentException)
		{
			return $"SQL Server LocalDB is not available on this machine ({ex.GetType().Name}); the tests that need it are skipped.";
		}
	}

	/// <summary>Skips the calling test (it is reported as skipped) when LocalDB cannot be reached.</summary>
	public static void SkipUnlessAvailable()
	{
		if (Unavailable.Value is { } reason) Assert.Skip(reason);
	}

	/// <summary>A new, closed connection to LocalDB's <c>tempdb</c>. The caller disposes it.</summary>
	public static SqlConnection Create() => new(ConnectionString);

	/// <summary>A name no other test, and no earlier run, uses.</summary>
	public static string UniqueName(string prefix) => $"{prefix}_{Guid.NewGuid():N}";

	/// <summary>Runs one statement on a connection of its own.</summary>
	public static void Execute(string sql)
	{
		using SqlConnection connection = Create();
		connection.Open();
		using SqlCommand command = connection.CreateCommand();
		command.CommandText = sql;
		command.ExecuteNonQuery();
	}

	/// <summary>Runs one query on a connection of its own, so what it sees is what any other connection sees: nothing that is still uncommitted.</summary>
	public static int Count(string table)
	{
		using SqlConnection connection = Create();
		connection.Open();
		using SqlCommand command = connection.CreateCommand();
		command.CommandText = $"select count(*) from dbo.[{table}]";
		return (int)command.ExecuteScalar()!;
	}

	/// <summary>A table with one <c>int</c> column named <c>id</c>.</summary>
	public static Scratch CreateTable(string prefix = "ode_t")
	{
		string name = UniqueName(prefix);
		Execute($"create table dbo.[{name}] (id int not null)");
		return new Scratch("table", name);
	}

	/// <summary>A stored procedure with the given parameter list (for example <c>@a int, @b varchar(10) = 'x'</c>) whose body selects 1.</summary>
	public static Scratch CreateProcedure(string parameters, string prefix = "ode_p")
	{
		string name = UniqueName(prefix);
		Execute($"create procedure dbo.[{name}] {parameters} as select 1 as one");
		return new Scratch("procedure", name);
	}

	/// <summary>An object created in <c>tempdb</c> for one test. Disposing it drops the object.</summary>
	public sealed class Scratch(string kind, string name) : IDisposable
	{
		/// <summary>The object's name, without the schema.</summary>
		public string Name { get; } = name;

		public void Dispose() => Execute($"drop {kind} if exists dbo.[{Name}]");
	}
}
