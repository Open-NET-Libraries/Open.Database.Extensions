using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

/// <summary>A tiny shared in-memory SQLite database that every sample runs against.</summary>
public static class Db
{
	// A named, shared-cache in-memory database lives as long as at least one connection to it is open.
	public const string ConnectionString = "Data Source=DocSamples;Mode=Memory;Cache=Shared";

	#region factory_definition
	// A connection factory: each command built from it opens its own connection and disposes it afterwards.
	// Held in a lazily created field (the C# 14 `field` keyword). The factory is stateless, so a benign race costs nothing.
	public static IDbConnectionFactory<SqliteConnection> Factory => field ??= new DbConnectionFactory<SqliteConnection>(() => new SqliteConnection(ConnectionString));
	#endregion

	/// <summary>Opens the connection that keeps the in-memory database alive between factory-built commands.</summary>
	public static SqliteConnection OpenKeeper()
	{
		var keeper = new SqliteConnection(ConnectionString);
		keeper.Open();
		return keeper;
	}

	/// <summary>(Re)creates and fills the sample tables.</summary>
	public static void Seed()
	{
		Factory.Command("drop table if exists posts").ExecuteNonQuery();
		Factory.Command("drop table if exists people").ExecuteNonQuery();
		Factory.Command("create table posts (id integer primary key, author integer not null, title text not null, body text null, views integer not null)").ExecuteNonQuery();
		Factory.Command("create table people (person_id integer primary key, first_name text not null, last_name text not null)").ExecuteNonQuery();

		for (int i = 1; i <= 5; i++)
		{
			Factory.Command("insert into posts (id, author, title, body, views) values (@id, @author, @title, @body, @views)")
				.AddParam("id", i)
				.AddParam("author", i <= 3 ? 42 : 7)
				.AddParam("title", "Post " + i)
				.AddParam("body", i % 2 == 0 ? null : "Body " + i)
				.AddParam("views", i * 10)
				.ExecuteNonQuery();
		}

		Factory.Command("insert into people (person_id, first_name, last_name) values (1, 'Ada', 'Lovelace'), (2, 'Alan', 'Turing')").ExecuteNonQuery();
	}
}
