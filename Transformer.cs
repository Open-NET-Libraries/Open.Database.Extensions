using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
	class Transformer<T>
		where T : new()
	{
		public readonly Type Type;
		public readonly PropertyInfo[] Properties;

		// Allow mapping key = object property, value = column name.
		readonly Dictionary<string, string> PropertyMap;
		readonly Dictionary<string, PropertyInfo> ColumnToPropertyMap;
		public HashSet<string> PropertyNames => new HashSet<string>(PropertyMap.Keys);
		public HashSet<string> ColumnNames => new HashSet<string>(PropertyMap.Values);

		public Transformer(IEnumerable<(string Field, string Column)> overrides = null)
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

			ColumnToPropertyMap = PropertyMap.ToDictionary(kvp => kvp.Value, kvp => pm[kvp.Key]);

		}

		class Processor
		{
			public Processor(Transformer<T> transformer, string[] names = null)
			{
				Transformer = transformer;
				Transform = record =>
				{
					var model = new T();
					var count = _names.Length;
					for (var i = 0; i < count; i++)
					{
						var p = _properties[i];
						if (p != null)
						{
							var name = _names[i];
							var value = record[i];
							if (value == DBNull.Value) value = null;
							p.SetValue(model, value);
						}
					}

					return model;
				};

				if (names != null) SetNames(names);
			}

			public readonly Transformer<T> Transformer;

			string[] _names;
			PropertyInfo[] _properties;

			public readonly Func<object[], T> Transform; // Using a Func<object[],T> for better type inferrence.

			public void SetNames(string[] names)
			{
				var map = Transformer.ColumnToPropertyMap;
				_names = names;
				_properties = names
					.Select(n => map.TryGetValue(n, out PropertyInfo p) ? p : null)
					.ToArray();
			}

			public TransformBlock<object[], T> GetBlock()
				=> new TransformBlock<object[], T>(Transform);
		}

		public IEnumerable<T> AsDequeueingEnumerable(QueryResult<Queue<object[]>> results)
		{
			var processor = new Processor(this, results.Names);
			var q = results.Result;

			while (q.Count != 0)
				yield return processor.Transform(q.Dequeue());

			// By using the above routine, we guarantee as enumeration occurs, references are released (dequeued).
		}

		public ISourceBlock<T> Results(out Action<QueryResult<IEnumerable<object[]>>> deferred)
		{
			var processor = new Processor(this);
			var x = processor.GetBlock();

			deferred = results =>
			{
				processor.SetNames(results.Names);
				var q = results.Result;
				foreach (var record in q) if (!x.Post(record)) break;
				x.Complete(); // May not be necessary, but we'll call it to ensure the .Completion occurs.
			};

			return x;
		}

        public ISourceBlock<T> Results(QueryResult<ISourceBlock<object[]>> source)
        {
            var processor = new Processor(this, source.Names);
            var x = processor.GetBlock();
            var r = source.Result;
            r.LinkTo(x);
            r.Completion.ContinueWith(t => x.Complete()); // Signal that no more results will be coming.
            x.Completion.ContinueWith(t => r.Complete()); // Signal that no more results can be recieved.
            return x;
        }

        public TransformBlock<object[], T> ResultsBlock(
			out Action<string[]> initColumnNames)
		{
			var processor = new Processor(this);
			var x = processor.GetBlock();

			initColumnNames = results => processor.SetNames(results);

			return x;
		}

		public IEnumerable<T> Results(DataTable table, bool clearTable)
		{
			var columns = table.Columns.AsEnumerable();
			var results = new QueryResult<Queue<object[]>>(
				columns.Select(c => c.Ordinal).ToArray(),
				columns.Select(c => c.ColumnName).ToArray(),
				new Queue<object[]>(table.Rows.AsEnumerable().Select(r => r.ItemArray)));
			if (clearTable) table.Rows.Clear();
			return AsDequeueingEnumerable(results);
		}

	}
}
