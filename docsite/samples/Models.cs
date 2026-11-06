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

// Columns are matched to properties exactly, else ignoring case, else ignoring underscores: first_name and last_name feed FirstName and LastName,
// but person_id matches no property, so this model needs a column override for Id.
public class Person
{
	public int Id { get; set; }
	public string FirstName { get; set; } = "";
	public string LastName { get; set; } = "";
}

// Dapper can bind constructors (including positional records). Map<T>() cannot: it needs new T() and settable properties.
// (SQLite reports INTEGER columns as long, and Dapper matches constructor parameters by type, so the record uses long.)
public record PostRecord(long Id, long Author, string Title, string? Body);
