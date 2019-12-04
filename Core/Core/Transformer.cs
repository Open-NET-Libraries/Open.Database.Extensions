using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Open.Database.Extensions.Core
{
	/// <summary>
	/// Utility of transforming retrieved data into models matching the type parameter provided.
	/// </summary>
	/// <typeparam name="T">The type of the model to create from the data.</typeparam>
	public class Transformer<T>
		where T : new()
	{
		/// <summary>
		/// Maximum number of arrays to hold in the local array pool per bucket.
		/// May also define how many records are pre buffered before transforming.
		/// </summary>
		protected const int MaxArrayBuffer = 1024;

		/// <summary>
		/// Buffers for transforming.
		/// </summary>
		protected internal static readonly ArrayPool<object> LocalPool = ArrayPool<object>.Create(1024, MaxArrayBuffer);

		/// <summary>
		/// The type of <typeparamref name="T"/>.
		/// </summary>
		public Type Type { get; }

		private readonly PropertyInfo[] Properties;

		// Allow mapping key = object property, value = column name.
		readonly Dictionary<string, string> PropertyMap;
		readonly Dictionary<string, PropertyInfo> ColumnToPropertyMap;

		/// <summary>
		/// The property names.
		/// </summary>
		public IEnumerable<string> PropertyNames => PropertyMap.Keys;

		/// <summary>
		/// The column names.
		/// </summary>
		public IEnumerable<string> ColumnNames => PropertyMap.Values;

		/// <summary>
		/// Constructs a transformer using the optional field overrides.
		/// </summary>
		/// <param name="overrides"></param>
		protected internal Transformer(IEnumerable<(string Field, string? Column)>? overrides = null)
		{
			Type = typeof(T);
			Properties = Type.GetProperties();
			PropertyMap = Properties.Select(p => p.Name).ToDictionary(n => n);

			var pm = Properties.ToDictionary(p => p.Name);

			if (overrides != null)
			{
				foreach (var (Field, Column) in overrides)
				{
					var cn = Column;
					if (cn == null) PropertyMap.Remove(Field); // Null values indicate a desire to 'ignore' a field.
					else PropertyMap[Field] = cn;
				}
			}

			ColumnToPropertyMap = PropertyMap.ToDictionary(kvp => kvp.Value.ToUpperInvariant(), kvp => pm[kvp.Key]);

		}


		/// <summary>
		/// Static utility for creating a Transformer <typeparamref name="T"/>.
		/// </summary>
		/// <param name="overrides"></param>
		/// <returns></returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1000:Do not declare static members on generic types", Justification = "This is simply an expressive helper that would seem odd to make another static class to handle.")]
		public static Transformer<T> Create(IEnumerable<(string Field, string? Column)>? overrides = null)
			=> new Transformer<T>(overrides);

		/// <summary>
		/// A sub class for processing the transformer results.
		/// </summary>
		protected class Processor
		{
			/// <summary>
			/// Constructs a processor.
			/// </summary>
			/// <param name="transformer">The transformer to use.</param>
			public Processor(Transformer<T> transformer)
			{
				Transformer = transformer;
				Transform = record =>
				{
					var model = new T();
					var count = _names.Length;
					for (var i = 0; i < count; i++)
					{
						var p = _propertySetters[i];
						if (p != null)
						{
							object? value = record[i];
							if (value == DBNull.Value) value = null;
							try
							{
								p(model, value);
							}
							catch (Exception ex)
							{
								throw new InvalidOperationException($"Unable to set value of property '{_names[i]}'.", ex);
							}
						}
					}

					return model;
				};
			}

			/// <summary>
			/// Constructs a processor.
			/// </summary>
			/// <param name="transformer">The transformer to use.</param>
			/// <param name="names">The names of columns/properties to acquire.</param>
			public Processor(Transformer<T> transformer, ImmutableArray<string> names)
				: this(transformer)
			{
				SetNames(names);
			}

			/// <summary>
			/// The transformer being used.
			/// </summary>
			public Transformer<T> Transformer { get; }

			ImmutableArray<string> _names = ImmutableArray<string>.Empty;
			Action<T, object?>?[] _propertySetters = Array.Empty<Action<T, object?>?>();

			/// <summary>
			/// The resultant transofrm function.
			/// </summary>
			public Func<object[], T> Transform { get; } // Using a Func<object[],T> for better type inference.

			/// <summary>
			/// Allows for deferred initialization.
			/// </summary>
			/// <param name="names">The column/property names to process.</param>
			public void SetNames(ImmutableArray<string> names)
			{
				var map = Transformer.ColumnToPropertyMap;
				_names = names;
				_propertySetters = names
					.Select(n => map.TryGetValue(n.ToUpperInvariant(), out var p) ? p.BuildUntypedSetter<T>() : null)
					.ToArray();
			}
		}

		/// <summary>
		/// Dequeues the results and transforms each one by one during enumeration.
		/// </summary>
		/// <param name="results">The results to process.</param>
		/// <returns>A dequeuing enumerable of the transformed results.</returns>
		public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results)
		{
			if (results is null) throw new ArgumentNullException(nameof(results));
			Contract.EndContractBlock();

			var processor = new Processor(this, results.Names);
			var q = results.Result;

			return q.DequeueEach().Select(processor.Transform);
		}

		/// <summary>
		/// Dequeues the results and transforms each one by one during enumeration.
		/// </summary>
		/// <param name="results">The results to process.</param>
		/// <param name="arrayPool">The array pool to return the buffers to.</param>
		/// <returns>A dequeuing enumerable of the transformed results.</returns>
		public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results, ArrayPool<object> arrayPool)
		{
			if (results is null) throw new ArgumentNullException(nameof(results));
			Contract.EndContractBlock();

			var processor = new Processor(this, results.Names);
			var q = results.Result;

			return q.DequeueEach().Select(a =>
			{
				try
				{
					return processor.Transform(a);
				}
				finally
				{
					arrayPool.Return(a);
				}
			});
		}

		/// <summary>
		/// Transforms the results
		/// </summary>
		/// <param name="reader">The reader to read from.</param>
		/// <returns>An enumerable that transforms the results.</returns>
		internal IEnumerable<T> Results(IDataReader reader)
		{
			if (reader is null) throw new System.ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			// Ignore missing columns.
			var columns = reader.GetMatchingOrdinals(PropertyMap.Values, true);
			var processor = new Processor(this, columns.Select(m => m.Name).ToImmutableArray());

			return reader
				.AsEnumerable(columns.Select(m => m.Ordinal), LocalPool)
				.Select(a =>
				{
					try
					{
						return processor.Transform(a);
					}
					finally
					{
						LocalPool.Return(a);
					}
				});
		}

		/// <summary>
		/// Transforms the results
		/// </summary>
		/// <param name="reader">The reader to read from.</param>
		/// <param name="readStarted"></param>
		/// <returns>An enumerable that transforms the results.</returns>
		internal IEnumerable<T> ResultsBuffered(IDataReader reader, bool readStarted)
		{
			if (reader is null) throw new System.ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			if (!readStarted && !reader.Read())
				return Enumerable.Empty<T>();

			// Ignore missing columns.
			var columns = reader.GetMatchingOrdinals(PropertyMap.Values, true);

			return AsDequeueingEnumerable(
				CoreExtensions.RetrieveInternal(
					reader,
					columns.Select(c => c.Ordinal),
					columns.Select(c => c.Name),
					readStarted: readStarted,
					arrayPool: LocalPool),
				LocalPool);
		}

#if NETSTANDARD2_1
		/// <summary>
		/// Transforms the results
		/// </summary>
		/// <param name="reader">The reader to read from.</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns>An enumerable that transforms the results.</returns>
		internal async IAsyncEnumerable<T> ResultsAsync(DbDataReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
		{
			if (reader is null) throw new System.ArgumentNullException(nameof(reader));
			Contract.EndContractBlock();

			// Ignore missing columns.
			var columns = reader.GetMatchingOrdinals(PropertyMap.Values, true);
			var processor = new Processor(this, columns.Select(m => m.Name).ToImmutableArray());

			await foreach (var a in reader.AsAsyncEnumerable(columns.Select(m => m.Ordinal), LocalPool, cancellationToken))
			{
				try
				{
					yield return processor.Transform(a);
				}
				finally
				{
					LocalPool.Return(a);
				}
			}
		}
#endif

		/// <summary>
		/// Processes the data from the data table into a queue. Then dequeues the results and transforms each one by one during enumeration.
		/// </summary>
		/// <param name="table">The data to process.</param>
		/// <param name="clearTable">If true, will clear the table after buffering the data.</param>
		/// <returns>A dequeuing enumerable of the transformed results.</returns>
		public IEnumerable<T> Results(DataTable table, bool clearTable)
		{
			if (table is null) throw new ArgumentNullException(nameof(table));
			Contract.EndContractBlock();

			var columnCount = table.Columns.Count;
			var columns = table.Columns.AsEnumerable();
			var results = new QueryResult<Queue<object[]>>(
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.ColumnName),
				new Queue<object[]>(table.Rows.AsEnumerable().Select(r =>
				{
					var a = LocalPool.Rent(columnCount);
					for (var i = 0; i < columnCount; i++) a[i] = r[i];
					return a;
				})));

			if (clearTable) table.Rows.Clear();
			return AsDequeueingEnumerable(results, LocalPool);
		}

	}
}
