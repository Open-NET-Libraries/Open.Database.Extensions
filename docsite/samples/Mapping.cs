using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class Mapping
{
	public static Post[] MapDefault(SqliteConnection connection)
	{
		#region map_default
		// Columns are matched to public settable properties by name, ignoring case.
		// The extra "views" column has no property, so it is ignored.
		Post[] posts = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToArray();
		#endregion

		return posts;
	}

	public static Person[] MapOverrides(SqliteConnection connection)
	{
		#region map_overrides
		// Field-to-column overrides for columns that do not share the property name.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(("Id", "person_id"), ("FirstName", "first_name"), ("LastName", "last_name"))
			.ToArray();
		#endregion

		return people;
	}

	public static Person[] MapIgnore(SqliteConnection connection)
	{
		#region map_ignore
		// A null column ignores the field: LastName stays at its default.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(("Id", "person_id"), ("FirstName", "first_name"), ("LastName", null))
			.ToArray();
		#endregion

		return people;
	}

	public static string MapUnknownField(SqliteConnection connection)
	{
		try
		{
			#region map_unknown_field
			// A field name that is not a settable public property throws ArgumentException.
			connection
				.Command("select * from posts")
				.Map<Post>(("Headline", "title"))
				.ToArray();
			#endregion

			return "no exception";
		}
		catch (ArgumentException ex)
		{
			return ex.Message;
		}
	}

	public static Person[] MapMap(SqliteConnection connection)
	{
		#region map_declared_use
		// Pass the declared map. Nothing is built, keyed or looked up per call.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonMap)
			.ToArray();
		#endregion

		return people;
	}

	public static Person[] MapMapIgnore(SqliteConnection connection)
	{
		// LastName is not read at all, so it stays at its default although the result set has a last_name column.
		return connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonNamesOnlyMap)
			.ToArray();
	}

	public static Person[] MapInline(SqliteConnection connection)
	{
		#region map_inline
		// A one-off: the pairs are inline, so a new map is built and the row reader is compiled on every call
		// (about 0.43 ms and 29 KB for a one-row read in the library's in-memory benchmark). Declare a RowMap for anything that runs more than occasionally.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(new(nameof(Person.Id), "person_id"), new(nameof(Person.FirstName), "first_name"), new(nameof(Person.LastName), "last_name"))
			.ToArray();
		#endregion

		return people;
	}

	public static string MapUnknownProperty()
	{
		try
		{
			#region map_declared_unknown
			// A map is checked where it is declared: this throws ArgumentException here, not on the first query.
			RowMap<Post> map = [new("Headline", "title")];
			#endregion

			return "no exception: " + map.GetType().Name;
		}
		catch (ArgumentException ex)
		{
			return ex.Message;
		}
	}

	public static Post[] MapPaging(SqliteConnection connection)
	{
		#region map_paging
		// Skipped rows are advanced without being mapped, and reading stops once the take is satisfied.
		Post[] page = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.Skip(1)
			.Take(2)
			.ToArray();
		#endregion

		return page;
	}

	public static (Post? First, Post? Single) FirstAndSingle(SqliteConnection connection)
	{
		#region map_first_single
		Post? first = connection.Command("select * from posts order by id").Map<Post>().FirstOrDefault();

		// SingleOrDefault returns null for no rows and throws if there is more than one row.
		Post? single = connection.Command("select * from posts where id = @id").AddParam("id", 2).Map<Post>().SingleOrDefault();
		#endregion

		return (first, single);
	}

	public static string[] MapSelector(SqliteConnection connection)
	{
		#region map_selector
		// Map(selector) is the escape hatch: you read the columns yourself.
		string[] titles = connection
			.Command("select title, views from posts order by id")
			.Map(record => $"{record.GetString(0)} ({record.GetInt64(1)} views)")
			.ToArray();
		#endregion

		return titles;
	}

	public static int StreamRows(SqliteConnection connection)
	{
		#region map_stream
		// Read hands you a lazy sequence that is only valid inside the handler, while the reader is open.
		int total = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.Read(posts => posts.Sum(post => post.Id));
		#endregion

		return total;
	}

	public static async Task<Dictionary<int, Post>> ByIdAsync(SqliteConnection connection, CancellationToken cancellationToken)
	{
		#region map_dictionary_async
		Dictionary<int, Post> byId = await connection
			.Command("select * from posts")
			.Map<Post>()
			.ToDictionaryAsync(post => post.Id, cancellationToken: cancellationToken);
		#endregion

		return byId;
	}
}

// Declared once and built on first use: a large application only ever builds the maps it actually uses.
// The property name is written with nameof, so a rename is caught by the compiler; only the column is a string.
public static class PersonMaps
{
	#region map_declared
	public static RowMap<Person> PersonMap => field ??= [
		new(nameof(Person.Id), "person_id"),
		new(nameof(Person.FirstName), "first_name"),
		new(nameof(Person.LastName), "last_name"),
	];
	#endregion

	#region map_declared_ignore
	// A null column means "do not map this property": LastName is not read at all, not even by name,
	// so it keeps its default value even if the result set has a column called LastName.
	public static RowMap<Person> PersonNamesOnlyMap => field ??= [
		new(nameof(Person.Id), "person_id"),
		new(nameof(Person.FirstName), "first_name"),
		new(nameof(Person.LastName), null),
	];
	#endregion
}
