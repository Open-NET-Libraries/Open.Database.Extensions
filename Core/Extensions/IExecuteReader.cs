using Open.Database.Extensions.Core;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading.Tasks;

namespace Open.Database.Extensions;

/// <summary>
/// Core non-DB-specific extensions for retrieving data from a command using best practices.
/// </summary>
public static class IExecuteReaderExtensions
{
    /// <summary>
    /// Iterates a reader on a command with a handler function.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    public static void IterateReader(this IExecuteReader command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        Contract.EndContractBlock();

        command.ExecuteReader(
            reader => reader.ForEach(handler),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates a reader on a command while the handler function returns true.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    public static void IterateReaderWhile(this IExecuteReader command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        Contract.EndContractBlock();

        command.ExecuteReader(
            reader => reader.IterateWhile(handler),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates a reader on a command with a handler function.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    public static ValueTask IterateReaderAsync(this IExecuteReaderAsync command, Action<IDataRecord> handler, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        Contract.EndContractBlock();

        return command.ExecuteReaderAsync(
            reader =>
            {
                if (reader is DbDataReader r)
                    return r.ForEachAsync(handler, command.UseAsyncRead, command.CancellationToken);

                reader.ForEach(handler, true, command.CancellationToken);
                return new ValueTask();
            },
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates a reader on a command while the handler function returns true.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    public static ValueTask IterateReaderWhileAsync(this IExecuteReaderAsync command, Func<IDataRecord, bool> handler, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        Contract.EndContractBlock();

        return command.ExecuteReaderAsync(
            reader =>
            {
                if (reader is DbDataReader r)
                    return r.IterateWhileAsync(handler, command.UseAsyncRead, command.CancellationToken);

                reader.IterateWhile(handler, true, command.CancellationToken);
                return new ValueTask();
            },
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates a reader on a command while the handler function returns true.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="handler">The handler function for each IDataRecord.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    public static ValueTask IterateReaderWhileAsync(this IExecuteReaderAsync command, Func<IDataRecord, ValueTask<bool>> handler, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        Contract.EndContractBlock();

        return command.ExecuteReaderAsync(
            reader => reader.IterateWhileAsync(handler, command.CancellationToken),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Executes a reader on a command with a transform function.
    /// </summary>
    /// <typeparam name="TEntity">The return type of the transform function applied to each record.</typeparam>
    /// <typeparam name="TResult">The type returned by the selector.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="selector">Provides an IEnumerable&lt;TEntity&gt; to select individual results by.</param>
    /// <param name="behavior">The behavior to use with the data reader.</param>
    /// <returns>The result of the transform.</returns>
    public static TResult IterateReader<TEntity, TResult>(
        this IExecuteReader command,
        Func<IDataRecord, TEntity> transform,
        Func<IEnumerable<TEntity>, TResult> selector, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => selector(reader.Select(transform)),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and returns the first result through a transform function.  Throws if none.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The value from the transform.</returns>
    public static T First<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).First(),
            CommandBehavior.SingleRow | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The value from the transform.</returns>
    public static T FirstOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).FirstOrDefault(),
            CommandBehavior.SingleRow | CommandBehavior.SingleResult);
    }

	/// <summary>
	/// Iterates a IDataReader and returns the first result through a transform function.  Throws if none or more than one entry.
	/// </summary>
	/// <typeparam name="T">The return type of the transform function.</typeparam>
	/// <param name="command">The IExecuteReader to iterate.</param>
	/// <param name="transform">The transform function to process each IDataRecord.</param>
	/// <returns>The value from the transform.</returns>
	public static T Single<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).Single(),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and returns the first result through a transform function.  Returns default(T) if none.  Throws if more than one entry.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The value from the transform.</returns>
    public static T SingleOrDefault<T>(this IExecuteReader command, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).SingleOrDefault(),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and returns the first number of results defined by the count.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="count">The maximum number of records to return.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The results from the transform limited by the take count.</returns>
    public static List<T> Take<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
        Contract.EndContractBlock();

        return count == 0
            ? new List<T>()
            : command.ExecuteReader(
                reader => reader.Select(transform).Take(count).ToList(),
                CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and skips the first number of results defined by the count.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="count">The number of records to skip.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The results from the transform after the skip count.</returns>
    public static List<T> Skip<T>(this IExecuteReader command, int count, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Cannot be negative.");
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => count == 0
                ? reader.ToList(transform)
                : reader.Select(transform).Skip(count).ToList(),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates an IDataReader and skips by the skip parameter returns the maximum remaining defined by the take parameter.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="skip">The number of entries to skip before starting to take results.</param>
    /// <param name="take">The maximum number of records to return.</param>
    /// <param name="transform">The transform function to process each IDataRecord.</param>
    /// <returns>The results from the skip, transform and take operation.</returns>
    public static List<T> SkipThenTake<T>(this IExecuteReader command, int skip, int take, Func<IDataRecord, T> transform)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        if (skip < 0) throw new ArgumentOutOfRangeException(nameof(skip), skip, "Cannot be negative.");
        if (take < 0) throw new ArgumentOutOfRangeException(nameof(take), take, "Cannot be negative.");
        Contract.EndContractBlock();

        return take == 0
            ? new List<T>()
            : command.ExecuteReader(
                reader => reader.Select(transform).Skip(skip).Take(take).ToList(),
                CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Converts all IDataRecords into a list using a transform function.
    /// </summary>
    /// <typeparam name="T">The expected return type.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function.</param>
    /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
    /// <returns>The list of transformed records.</returns>
    public static List<T> ToList<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).ToList(),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Converts all IDataRecords into an array using a transform function.
    /// </summary>
    /// <typeparam name="T">The expected return type.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function.</param>
    /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
    /// <returns>The array of transformed records.</returns>
    public static T[] ToArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).ToArray(),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Converts all IDataRecords into an immutable array using a transform function.
    /// </summary>
    /// <typeparam name="T">The expected return type.</typeparam>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="transform">The transform function.</param>
    /// <param name="behavior">The command behavior for once the command the reader is complete.</param>
    /// <returns>The array of transformed records.</returns>
    public static ImmutableArray<T> ToImmutableArray<T>(this IExecuteReader command, Func<IDataRecord, T> transform, CommandBehavior behavior = CommandBehavior.Default)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (transform is null) throw new ArgumentNullException(nameof(transform));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Select(transform).ToImmutableArray(),
            behavior | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates all records within the first result set using an IDataReader and returns the results.
    /// <see cref="DBNull"/> values are left unchanged (retained).
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
    public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Retrieve(),
            CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates all records within the current result set using an IDataReader and returns the desired results.
    /// <see cref="DBNull"/> values are left unchanged (retained).
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="ordinals">The ordinals to request from the reader for each record.</param>
    /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
    public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<int> ordinals)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Retrieve(ordinals),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates all records within the current result set using an IDataReader and returns the desired results.
    /// <see cref="DBNull"/> values are left unchanged (retained).
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="n">The first ordinal to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining ordinals to request from the reader for each record.</param>
    /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
    public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, int n, params int[] others)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Retrieve(n, others),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates all records within the first result set using an IDataReader and returns the desired results as a list of Dictionaries containing only the specified column values.
    /// <see cref="DBNull"/> values are left unchanged (retained).
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="columnNames">The column names to select.</param>
    /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
    public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, IEnumerable<string> columnNames)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (columnNames is null) throw new ArgumentNullException(nameof(columnNames));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Retrieve(columnNames),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates all records within the current result set using an IDataReader and returns the desired results.
    /// <see cref="DBNull"/> values are left unchanged (retained).
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <param name="c">The first column name to include in the request to the reader for each record.</param>
    /// <param name="others">The remaining column names to request from the reader for each record.</param>
    /// <returns>The QueryResult that contains all the results and the column mappings.</returns>
    public static QueryResultQueue<object[]> Retrieve(this IExecuteReader command, string c, params string[] others)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        //if (c is null) throw new ArgumentNullException(nameof(c));
        //if (others.Any(e => e is null)) throw new ArgumentNullException(nameof(c));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.Retrieve(c, others),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates each record and attempts to map the fields to type T.
    /// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
    /// <returns>The enumerable to pull the transformed results from.</returns>
    public static IEnumerable<T> Results<T>(this IExecuteReader command, IEnumerable<KeyValuePair<string, string?>> fieldMappingOverrides)
        where T : new()
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (fieldMappingOverrides is null) throw new ArgumentNullException(nameof(fieldMappingOverrides));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.ResultsBuffered<T>(fieldMappingOverrides),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates each record and attempts to map the fields to type T.
    /// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
    /// <returns>The enumerable to pull the transformed results from.</returns>
    public static IEnumerable<T> Results<T>(this IExecuteReader command, IEnumerable<(string Field, string? Column)> fieldMappingOverrides)
        where T : new()
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (fieldMappingOverrides is null) throw new ArgumentNullException(nameof(fieldMappingOverrides));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.ResultsBuffered<T>(fieldMappingOverrides),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Iterates each record and attempts to map the fields to type T.
    /// Data is temporarily stored (buffered in entirety) in a queue before applying the transform for each iteration.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <typeparam name="T">The model type to map the values to (using reflection).</typeparam>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names where the keys are the property names, and values are the column names.</param>
    /// <returns>The enumerable to pull the transformed results from.</returns>
    public static IEnumerable<T> Results<T>(this IExecuteReader command, params (string Field, string? Column)[] fieldMappingOverrides)
        where T : new()
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        if (fieldMappingOverrides is null) throw new ArgumentNullException(nameof(fieldMappingOverrides));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.ResultsBuffered<T>(fieldMappingOverrides),
            CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Reads the first column from every record and returns the results as a list..
    /// <see cref="DBNull"/> values are converted to null.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <returns>The list of transformed records.</returns>
    public static IEnumerable<object?> FirstOrdinalResults(this IExecuteReader command)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.FirstOrdinalResults(),
            CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Reads the first column from every record..
    /// <see cref="DBNull"/> values are converted to null.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <returns>The enumerable of casted values.</returns>
    public static IEnumerable<T0> FirstOrdinalResults<T0>(this IExecuteReader command)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.FirstOrdinalResults<T0>(),
            CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Imports all data using an IDataReader into a DataTable.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <returns>The resultant DataTable.</returns>
    public static DataTable LoadTable(this IExecuteReader command)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.ToDataTable(),
            CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
    }

    /// <summary>
    /// Loads all data from a command through an IDataReader into a DataTables.
    /// Calls .NextResult() to check for more results.
    /// </summary>
    /// <param name="command">The IExecuteReader to iterate.</param>
    /// <returns>The resultant list of DataTables.</returns>
    public static List<DataTable> LoadTables(this IExecuteReader command)
    {
        if (command is null) throw new ArgumentNullException(nameof(command));
        Contract.EndContractBlock();

        return command.ExecuteReader(
            reader => reader.ToDataTables(),
            CommandBehavior.SequentialAccess);
    }
}
