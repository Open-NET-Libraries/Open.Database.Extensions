using System;
using System.Data;
using System.Threading.Tasks;

namespace Open.Database.Extensions
{
	/// <summary>
	/// Common interface used for expressive commands when dealing with a data reader.
	/// </summary>
	/// <typeparam name="TReader">The type of the data reader.</typeparam>
	public interface IExecuteReader<TReader>
		where TReader : IDataReader
	{
		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for the data reader.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public void ExecuteReader(Action<TReader> handler, CommandBehavior behavior = CommandBehavior.Default);
		/// <summary>
		/// Executes a reader on a command with a transform function.
		/// </summary>
		/// <typeparam name="T">The return type of the transform function.</typeparam>
		/// <param name="transform">The transform function for each IDataRecord.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		/// <returns>The result of the transform.</returns>
		public T ExecuteReader<T>(Func<TReader, T> transform, CommandBehavior behavior = CommandBehavior.Default);

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for the data reader.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public ValueTask ExecuteReaderAsync(Func<TReader, ValueTask> handler, CommandBehavior behavior = CommandBehavior.Default);

		/// <summary>
		/// Executes a reader on a command with a handler function.
		/// </summary>
		/// <param name="handler">The handler function for the data reader.</param>
		/// <param name="behavior">The command behavior for once the command the reader is complete.</param>
		public ValueTask<T> ExecuteReaderAsync<T>(Func<TReader, ValueTask<T>> handler, CommandBehavior behavior = CommandBehavior.Default);
	}

	/// <inheritdoc />
	public interface IExecuteReader : IExecuteReader<IDataReader>
	{

	}
}
