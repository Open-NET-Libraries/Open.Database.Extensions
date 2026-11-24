using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;

namespace Open.Database.Extensions.Generator.Tests;

/// <summary>
/// What a real provider's typed getter does on a NULL cell, for each getter the generator emits (ModelReader.Getters), and whether
/// <c>IsDBNull</c> on the same column still answers after that read failed (in default and in SequentialAccess mode).
/// The generated reader trusts a non-nullable member's typed read (no IsDBNull) and relies on exactly this: a NULL read either throws
/// something that is not a DbException, or silently returns the type's default. Never a value other than the default, never a DbException.
/// </summary>
public class ProviderNullTests
{
	/// <summary>A getter, the SQL type it is read from on SQL Server, and the default its CLR type has.</summary>
	sealed record Getter(string Name, string SqlType, Func<DbDataReader, int, object?> Read, object? Default);

	static readonly Getter[] Getters =
	[
		new("GetBoolean", "bit", (r, i) => r.GetBoolean(i), false),
		new("GetByte", "tinyint", (r, i) => r.GetByte(i), (byte)0),
		new("GetChar", "nchar(1)", (r, i) => r.GetChar(i), '\0'),
		new("GetInt16", "smallint", (r, i) => r.GetInt16(i), (short)0),
		new("GetInt32", "int", (r, i) => r.GetInt32(i), 0),
		new("GetInt64", "bigint", (r, i) => r.GetInt64(i), 0L),
		new("GetFloat", "real", (r, i) => r.GetFloat(i), 0f),
		new("GetDouble", "float", (r, i) => r.GetDouble(i), 0d),
		new("GetDecimal", "decimal(18,2)", (r, i) => r.GetDecimal(i), 0m),
		new("GetDateTime", "datetime2", (r, i) => r.GetDateTime(i), default(DateTime)),
		new("GetGuid", "uniqueidentifier", (r, i) => r.GetGuid(i), Guid.Empty),
		new("GetString", "nvarchar(10)", (r, i) => r.GetString(i), null),
	];

	/// <summary>What one NULL read did: the exception type, or "returns default", or "returns X" (a value other than the default: a failure of the claim).</summary>
	static string Outcome(Getter g, DbDataReader r, int ordinal, out bool isNullAfter)
	{
		string outcome;
		try
		{
			object? value = g.Read(r, ordinal);
			outcome = Equals(value, g.Default) ? "default" : "VALUE " + value;
		}
		catch (Exception ex)
		{
			outcome = (ex is DbException ? "DbException " : "") + ex.GetType().FullName;
		}

		isNullAfter = r.IsDBNull(ordinal);
		return outcome;
	}

	static void Check(string provider, Func<Getter, DbDataReader> open, Func<Getter, string> expected)
	{
		var failures = new List<string>();
		foreach (Getter g in Getters)
		{
			using DbDataReader r = open(g);
			Assert.True(r.Read());
			string outcome;
			bool isNull;
			try
			{
				outcome = Outcome(g, r, 0, out isNull);
			}
			catch (Exception ex)
			{
				failures.Add($"{provider} {g.Name}: IsDBNull after the read threw {ex.GetType().FullName}: {ex.Message}");
				continue;
			}

			// The claim: never a non-default value, never a DbException; and the NULL is still seen after the failed read.
			if (outcome.StartsWith("VALUE ", StringComparison.Ordinal) || outcome.StartsWith("DbException ", StringComparison.Ordinal))
				failures.Add($"{provider} {g.Name}: {outcome} (breaks the claim)");
			if (!isNull) failures.Add($"{provider} {g.Name}: IsDBNull after the read is false");
			if (outcome != expected(g)) failures.Add($"{provider} {g.Name}: {outcome}, expected {expected(g)}");
		}

		Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
	}

	// Microsoft.Data.Sqlite: every typed getter on a NULL cell throws InvalidOperationException ("The data is NULL at ordinal 0...").
	static string SqliteExpected(Getter g) => "System.InvalidOperationException";

	[Fact]
	public void Sqlite_NullTypedRead()
	{
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		Check("Sqlite", g =>
		{
			SqliteCommand c = connection.CreateCommand();
			c.CommandText = "SELECT NULL AS v";
			return c.ExecuteReader();
		}, SqliteExpected);
	}

	[Fact]
	public void Sqlite_NullTypedRead_DeclaredColumn()
	{
		// A typed column (the type SQLite reports from the declaration), not a bare NULL literal.
		using var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		using (SqliteCommand setup = connection.CreateCommand())
		{
			setup.CommandText = "CREATE TABLE t (i INTEGER, r REAL, s TEXT, b BLOB); INSERT INTO t VALUES (NULL, NULL, NULL, NULL);";
			setup.ExecuteNonQuery();
		}

		Check("Sqlite(declared)", g =>
		{
			SqliteCommand c = connection.CreateCommand();
			string column = g.Name switch
			{
				"GetFloat" or "GetDouble" => "r",
				"GetString" or "GetChar" or "GetDecimal" or "GetDateTime" => "s",
				"GetGuid" => "b",
				_ => "i",
			};
			c.CommandText = $"SELECT {column} FROM t";
			return c.ExecuteReader();
		}, SqliteExpected);
	}

	// Microsoft.Data.SqlClient: every typed getter on a NULL cell throws System.Data.SqlTypes.SqlNullValueException (not a DbException);
	// GetChar is not supported at all (NotSupportedException, for a value too), so the runtime and the generated read both attribute it.
	static string SqlExpected(Getter g) => g.Name == "GetChar" ? "System.NotSupportedException" : "System.Data.SqlTypes.SqlNullValueException";

	static void SqlServer(CommandBehavior behavior)
	{
		LocalDb.SkipUnlessAvailable();
		using var connection = new SqlConnection(LocalDb.ConnectionString);
		connection.Open();
		Check("SqlClient " + behavior, g =>
		{
			SqlCommand c = connection.CreateCommand();
			// A second column after it: in SequentialAccess the reader must not have moved past the failed column.
			c.CommandText = $"SELECT CAST(NULL AS {g.SqlType}) AS v, 1 AS w";
			return c.ExecuteReader(behavior);
		}, SqlExpected);
	}

	[Fact]
	public void SqlClient_NullTypedRead() => SqlServer(CommandBehavior.Default);

	[Fact]
	public void SqlClient_NullTypedRead_Sequential() => SqlServer(CommandBehavior.SequentialAccess);
}

/// <summary>SQL Server LocalDB, as the Core tests reach it (LocalDbFixture): skipped when it cannot be reached, a failure when ODX_REQUIRE_SQL is set.</summary>
internal static class LocalDb
{
	public const string ConnectionString = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Integrated Security=true;Encrypt=False;Connect Timeout=20";

	static readonly Lazy<string?> Unavailable = new(() =>
	{
		try
		{
			using var connection = new SqlConnection(ConnectionString);
			connection.Open();
			return null;
		}
		catch (Exception ex) when (ex is SqlException or InvalidOperationException or PlatformNotSupportedException or TimeoutException or ArgumentException)
		{
			return $"SQL Server LocalDB is not available on this machine ({ex.GetType().Name}).";
		}
	});

	public static void SkipUnlessAvailable()
	{
		if (Unavailable.Value is not { } reason) return;
		if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ODX_REQUIRE_SQL")))
			Assert.Fail("ODX_REQUIRE_SQL is set, so a missing SQL Server is a failure, not a skip: " + reason);

		Assert.Skip(reason);
	}
}
