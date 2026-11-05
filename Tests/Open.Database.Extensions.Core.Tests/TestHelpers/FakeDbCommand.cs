using System.Data.Common;

namespace Open.Database.Extensions.Tests;

#nullable enable

/// <summary>
/// The smallest <see cref="DbCommand"/> that lets the async <see cref="DbCommand"/> extensions run:
/// it only records the <see cref="CommandBehavior"/> it is asked to execute with and hands back a fresh reader.
/// (<see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/> is not virtual,
/// so it can't be configured on an NSubstitute substitute.)
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class FakeDbCommand(DbConnection connection, Func<DbDataReader> readerFactory) : DbCommand
{
	public List<CommandBehavior> Behaviors { get; } = [];

	[AllowNull]
	public override string CommandText { get; set; } = string.Empty;

	public override int CommandTimeout { get; set; }

	public override CommandType CommandType { get; set; }

	public override bool DesignTimeVisible { get; set; }

	public override UpdateRowSource UpdatedRowSource { get; set; }

	protected override DbConnection? DbConnection { get; set; } = connection;

	protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();

	protected override DbTransaction? DbTransaction { get; set; }

	public override void Cancel() { }

	public override int ExecuteNonQuery() => throw new NotSupportedException();

	public override object? ExecuteScalar() => throw new NotSupportedException();

	public override void Prepare() { }

	protected override DbParameter CreateDbParameter() => throw new NotSupportedException();

	protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
	{
		Behaviors.Add(behavior);
		return readerFactory();
	}
}
