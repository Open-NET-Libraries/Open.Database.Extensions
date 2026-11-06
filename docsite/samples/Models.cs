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

// Columns are person_id, first_name, last_name, so this model needs field-to-column overrides.
public class Person
{
	public int Id { get; set; }
	public string FirstName { get; set; } = "";
	public string LastName { get; set; } = "";
}

// Dapper can bind constructors (including positional records). Map<T>() cannot: it needs new T() and settable properties.
// (SQLite reports INTEGER columns as long, and Dapper matches constructor parameters by type, so the record uses long.)
public record PostRecord(long Id, long Author, string Title, string? Body);
