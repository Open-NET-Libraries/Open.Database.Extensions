using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions;

/// <summary>
/// Extensions for pipelining data with Dataflow blocks.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2012:Use ValueTasks correctly", Justification = "Intentionally running in the background.")]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Roslynator", "RCS1047:Non-asynchronous method name should not end with 'Async'.", Justification = "<Pending>")]
public static partial class DataflowExtensions
{
    static BufferBlock<T> GetBufferBlock<T>(DataflowBlockOptions? options = null)
        => options == null ? new BufferBlock<T>() : new BufferBlock<T>(options);

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// </summary>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDataReader reader, DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object[]>(options);
        ToTargetBlock(reader, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlock(this IDataReader reader,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        ToTargetBlock(reader, buffer, true, arrayPool);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(reader, buffer, true, transform);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(reader, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDataReader reader,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(reader, buffer, true, fieldMappingOverrides);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object[]> AsSourceBlock(this IDbCommand command,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object[]>(options);
        ToTargetBlock(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlock(this IDbCommand command,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        ToTargetBlock(command, buffer, true, arrayPool);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true, transform);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IDbCommand command,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true, fieldMappingOverrides);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object[]> AsSourceBlock(this IExecuteReader command,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object[]>(options);
        ToTargetBlock(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> and posts each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlock(this IExecuteReader command,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        ToTargetBlock(command, buffer, true, arrayPool);
        return buffer;
    }

    /// <summary>
    /// Iterates an <see cref="IDataReader"/> through the transform function and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true, transform);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader mapping the results to classes of type <typeparamref name="T"/> and posts each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlock<T>(this IExecuteReader command,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        ToTargetBlock(command, buffer, true, fieldMappingOverrides);
        return buffer;
    }

	/// <summary>
	/// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
	/// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
	/// </summary>
	/// <param name="reader">The IDataReader to iterate.</param>
	/// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
	/// <param name="cancellationToken">An optional cancellation token.</param>
	/// <returns>The source block containing the results.</returns>
	public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDataReader reader,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<object[]>(options);
        _ = ToTargetBlockAsync(reader, buffer, true, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlockAsync(this IDataReader reader,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        _ = ToTargetBlockAsync(reader, buffer, true, arrayPool, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(reader, buffer, true, transform, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(reader, buffer, true, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// Will stop reading if the target rejects (is complete). If rejected, the current record will be the rejected record.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="reader">The IDataReader to iterate.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDataReader reader,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(reader, buffer, true, fieldMappingOverrides, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IDbCommand command,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<object[]>(options);
        _ = ToTargetBlockAsync(command, buffer, true, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlockAsync(this IDbCommand command,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        _ = ToTargetBlockAsync(command, buffer, true, arrayPool, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true, transform, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    /// <returns>The number of records processed.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>The source block containing the results.</returns>
    /// <returns>The number of records processed.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IDbCommand command,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true, fieldMappingOverrides, cancellationToken);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object[]> AsSourceBlockAsync(this IExecuteReader command,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object[]>(options);
        _ = ToTargetBlockAsync(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) and asynchronously sends each record as an array to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="arrayPool">The array pool to acquire buffers from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<object?[]> AsSourceBlockAsync(this IExecuteReader command,
        ArrayPool<object?> arrayPool,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<object?[]>(options);
        _ = ToTargetBlockAsync(command, buffer, true, arrayPool);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) through the transform function and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="transform">The transform function for each IDataRecord.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
        Func<IDataRecord, T> transform,
        DataflowBlockOptions? options = null)
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true, transform);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true);
        return buffer;
    }

    /// <summary>
    /// Iterates a data reader (asynchronous read if possible) mapping the results to classes of type <typeparamref name="T"/> and asynchronously sends each record to the block.
    /// If a connection is desired to remain open after completion, you must open the connection before calling this method.
    /// If the connection is already open, the reading will commence immediately.  Otherwise this will yield to the caller.
    /// </summary>
    /// <typeparam name="T">The return type of the transform function.</typeparam>
    /// <param name="command">The command to generate a reader from.</param>
    /// <param name="fieldMappingOverrides">An optional override map of field names to column names.</param>
    /// <param name="options">Optional DataflowBlockOptions for configuring the source block.</param>
    /// <returns>The source block containing the results.</returns>
    public static IReceivableSourceBlock<T> AsSourceBlockAsync<T>(this IExecuteReader command,
        IEnumerable<(string Field, string? Column)> fieldMappingOverrides,
        DataflowBlockOptions? options = null)
        where T : new()
    {
        var buffer = GetBufferBlock<T>(options);
        _ = ToTargetBlockAsync(command, buffer, true, fieldMappingOverrides);
        return buffer;
    }
}
