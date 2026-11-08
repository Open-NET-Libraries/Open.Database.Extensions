namespace Samples;

// Maps from "select * from posts": the extra "views" column has no property and is ignored.
#region post_model
public class Post
{
	public int Id { get; set; }
	public int Author { get; set; }
	public string Title { get; set; } = "";
	public string? Body { get; set; }
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
	public string Title { get; set; } = "";  // NULL gives null too: a reference type always does
}
#endregion

// Dapper can bind constructors (including positional records). Map<T>() cannot: it needs new T() and settable properties.
// (SQLite reports INTEGER columns as long, and Dapper matches constructor parameters by type, so the record uses long.)
public record PostRecord(long Id, long Author, string Title, string? Body);

// A search model: parameters can be named from its properties and fields.
public sealed class PostSearch
{
	public int Author { get; set; } = 7;
	public string? TitleContains; // a field works the same as a property
}
