using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reflection;

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
		public Transformer(IEnumerable<(string Field, string? Column)>? overrides = null)
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

			while (q.Count != 0)
				yield return processor.Transform(q.Dequeue());

			// By using the above routine, we guarantee as enumeration occurs, references are released (dequeued).
		}

		/// <summary>
		/// Processes the data from the data table inot a queue. Then dequeues the results and transforms each one by one during enumeration.
		/// </summary>
		/// <param name="table">The data to process.</param>
		/// <param name="clearTable">If true, will clear the table after buffering the data.</param>
		/// <returns>A dequeuing enumerable of the transformed results.</returns>
		public IEnumerable<T> Results(DataTable table, bool clearTable)
		{
			if (table is null) throw new ArgumentNullException(nameof(table));
			Contract.EndContractBlock();

			var columns = table.Columns.AsEnumerable();
			var results = new QueryResult<Queue<object[]>>(
				columns.Select(c => c.Ordinal),
				columns.Select(c => c.ColumnName),
				new Queue<object[]>(table.Rows.AsEnumerable().Select(r => r.ItemArray)));
			if (clearTable) table.Rows.Clear();
			return AsDequeueingEnumerable(results);
		}

	}
}
