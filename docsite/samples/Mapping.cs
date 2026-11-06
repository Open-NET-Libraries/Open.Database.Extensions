using System.Data;
using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

public static class Mapping
{
	public static Post[] MapDefault(SqliteConnection connection)
	{
		#region map_default
		// Columns are matched to public settable properties by name, ignoring case and underscores.
		// The extra "views" column has no property, so it is ignored.
		Post[] posts = connection
			.Command("select * from posts order by id")
			.Map<Post>()
			.ToArray();
		#endregion

		return posts;
	}

	public static Person[] MapDeclared(SqliteConnection connection)
	{
		#region map_declared_use
		// Pass the declared map. It is built once; each call only checks that the result-set layout matches the one its reader was compiled for.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonMap)
			.ToArray();
		#endregion

		return people;
	}

	public static Person[] MapNamesOnly(SqliteConnection connection)
		// LastName is not read at all, so it keeps its initial value although the result set has a last_name column.
		=> connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(PersonMaps.PersonNamesOnlyMap)
			.ToArray();


	public static string MapUnknownProperty()
	{
		try
		{
			#region map_declared_unknown
			// "Headline" is deliberately not a property of Post, so creating the map throws ArgumentException.
			RowMap<Post> map = new RowMap<Post>([new("Headline", "title")]);
			#endregion

			return "no exception: " + map.GetType().Name;
		}
		catch (ArgumentException ex)
		{
			return ex.Message;
		}
	}

	public static Person[] MapCallSite(SqliteConnection connection)
	{
		#region map_call_site
		// A one-off: the pairs are written at the call site, so a new map is built, and the row reader compiled, on every call.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(new RowMap<Person>([new(nameof(Person.Id), "person_id"), new(nameof(Person.FirstName), "first_name"), new(nameof(Person.LastName), "last_name")]))
			.ToArray();
		#endregion

		return people;
	}

	public static Person[] MapPairs(SqliteConnection connection)
	{
		#region map_pairs
		// Map<T> also takes the pairs themselves: the same one-off, a new map built, and the row reader compiled, on every call.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(new(nameof(Person.Id), "person_id"), new(nameof(Person.FirstName), "first_name"), new(nameof(Person.LastName), "last_name"))
			.ToArray();
		#endregion

		return people;
	}

	public static Person[] MapSinglePair(SqliteConnection connection)
	{
		#region map_single_pair
		// One pair still needs the brackets: new RowMap<Person>(new(...)) does not compile.
		// The other properties still match by name (first_name feeds FirstName).
		RowMap<Person> map = new([new(nameof(Person.Id), "person_id")]);
		#endregion

		return connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>(map)
			.ToArray();
	}

	public static Person[] MapSnakeCase(SqliteConnection connection)
	{
		#region map_snake_case
		// No RowMap<T>: first_name feeds FirstName and last_name feeds LastName (case and underscores ignored).
		// person_id matches no property, so Id keeps its default.
		Person[] people = connection
			.Command("select person_id, first_name, last_name from people order by person_id")
			.Map<Person>()
			.ToArray();
		#endregion

		return people;
	}

	public static OrderLine? MapNulls(SqliteConnection connection)
		=> connection
			.Command("select null as quantity, null as discount, null as note, null as title")
			.Map<OrderLine>()
			.FirstOrDefault();

	public static ValueMappingException? MapValueError(SqliteConnection connection)
	{
		#region map_value_error
		try
		{
			// 3000000000 does not fit the int property Post.Id.
			_ = connection.Command("select 3000000000 as id").Map<Post>().ToArray();
			return null;
		}
		catch (ValueMappingException ex)
		{
			// ex.PropertyName is "Id", ex.ColumnName is "id", ex.TargetType is typeof(int); the overflow is ex.InnerException.
			return ex;
		}
		#endregion
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
	public static RowMap<Person> PersonMap => field ??= new([
		new(nameof(Person.Id), "person_id"),
		new(nameof(Person.FirstName), "first_name"),
		new(nameof(Person.LastName), "last_name"),
	]);
	#endregion

	#region map_declared_ignore
	// A null column means "do not map this property": LastName is not read at all, not even by name,
	// so it keeps its initial value even if the result set has a column called LastName.
	public static RowMap<Person> PersonNamesOnlyMap => field ??= new([
		new(nameof(Person.Id), "person_id"),
		new(nameof(Person.FirstName), "first_name"),
		new(nameof(Person.LastName), null),
	]);
	#endregion
}

// A map held without the C# 14 field keyword.
public static class HoldingAMap
{
	#region map_declared_backing
	// Before C# 14 there is no field keyword: write the backing field yourself.
	internal static RowMap<Person>? _backingMap;

	public static RowMap<Person> BackingMap => _backingMap ??= new([
		new(nameof(Person.Id), "person_id"),
		new(nameof(Person.FirstName), "first_name"),
		new(nameof(Person.LastName), "last_name"),
	]);
	#endregion
}
