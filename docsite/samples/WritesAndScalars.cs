using Microsoft.Data.Sqlite;
using Open.Database.Extensions;

namespace Samples;

// A write and a scalar, each declared once and run wherever it is needed.

#region writes_and_scalars
public class PostWrites(IDbConnectionFactory<SqliteConnection> connectionFactory)
{
	static readonly DbNonQuery SetTitle
		= DbNonQuery.Text("update posts set title = @title where id = @id");

	static readonly ScalarDbQuery<int> PostCount
		= DbQuery.Text("select count(*) from posts where author = @author")
			.Scalar<int>();

	// A nullable type: null when there is no post.
	static readonly ScalarDbQuery<long?> LastId
		= DbQuery.Text("select max(id) from posts where author = @author")
			.Scalar<long?>();

	// The new id is a value to read, so the insert is a scalar.
	static readonly ScalarDbQuery<long> Add
		= DbQuery.Text(
			"insert into posts (author, title, views) values (@author, @title, 0); "
			+ "select last_insert_rowid()")
			.Scalar<long>();

	// Returns the number of rows affected.
	public ValueTask<int> SetTitleAsync(
		int id,
		string title,
		CancellationToken cancellationToken)
		=> SetTitle
			.Using(connectionFactory)
			.AddParam(id)
			.AddParam(title)
			.ExecuteNonQueryAsync(cancellationToken);

	public ValueTask<int> CountAsync(
		int author,
		CancellationToken cancellationToken)
		=> PostCount
			.Using(connectionFactory)
			.AddParam(author)
			.ExecuteScalarAsync(cancellationToken);

	public ValueTask<long?> LastIdAsync(
		int author,
		CancellationToken cancellationToken)
		=> LastId
			.Using(connectionFactory)
			.AddParam(author)
			.ExecuteScalarAsync(cancellationToken);

	public ValueTask<long> AddAsync(
		int author,
		string title,
		CancellationToken cancellationToken)
		=> Add
			.Using(connectionFactory)
			.AddParams(new { author, title })
			.ExecuteScalarAsync(cancellationToken);
}
#endregion
