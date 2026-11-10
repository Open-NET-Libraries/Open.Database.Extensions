namespace Samples;

// Maps from "select * from posts": the extra "views" column has no property and is ignored.
#region post_model
public class Post
{
	public required int Id { get; init; }
	public int Author { get; init; }
	public string Title { get; init; } = "";
	public string? Body { get; init; }
}
#endregion

// Columns are matched to properties exactly, else ignoring case, else ignoring case and underscores: first_name and last_name feed FirstName and LastName,
// but person_id matches no property, so Id needs a pair in a RowMap<Person>.
public class Person
{
	public int Id { get; set; }
	public string FirstName { get; set; } = "";
	public string LastName { get; set; } = "";
}

#region order_line_model
public class OrderLine
{
	public int Quantity { get; set; } = 1;   // NULL leaves 1: a non-nullable value type keeps its initializer, else its default
	public int? Discount { get; set; }       // NULL gives null
	public string? Note { get; set; }        // NULL gives null
	public string Title { get; set; } = "";  // NULL leaves "": a non-nullable reference (nullable annotations on) keeps its initializer too
}
#endregion

// Dapper and Map<T>() both bind the constructor of a positional record.
// (SQLite reports INTEGER columns as long, and Dapper matches constructor parameters by type, so the record uses long; Map<T>() converts integers, so int works there too.)
#region record_post_model
public record PostRecord(long Id, long Author, string Title, string? Body);
#endregion

// The constructor runs first, then the other settable and init properties are set from their own columns.
#region record_extra_model
public record PostSummary(int Id, string Title)
{
	public int Views { get; init; }                   // an init property: set from the views column
	public string Section { get; init; } = "general"; // no section column, so it keeps its initializer
}
#endregion

// A required member must be fed by a column in every result shape.
#region required_model
public class PostInput
{
	public required string Title { get; init; }
	public string? Body { get; init; }
}
#endregion

// A parameter no column feeds, and a NULL into a non-nullable parameter, take the declared default; a NULL into a nullable parameter is null.
#region review_model
public record Review(int Id, string Title = "untitled", int Stars = 3, int? Rating = 4);
#endregion

// Two public constructors and none without parameters: Map<T>() cannot choose, and says so at the call.
#region ambiguous_constructor_model
public class PostCard
{
	public PostCard(string title) => Title = title;
	public PostCard(string title, string? body) => (Title, Body) = (title, body);
	public string Title { get; }
	public string? Body { get; }
}
#endregion

// A search model: parameters can be named from its properties and fields.
public sealed class PostSearch
{
	public int Author { get; set; } = 7;
	public string? TitleContains; // a field works the same as a property
}
