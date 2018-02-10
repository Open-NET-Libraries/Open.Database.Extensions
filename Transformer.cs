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

        public Transformer(IEnumerable<KeyValuePair<string, string>> overrides = null)
        {
            Type = typeof(T);
            Properties = Type.GetProperties();
            PropertyMap = Properties.Select(p=>p.Name).ToDictionary(n=>n);

			var pm = Properties.ToDictionary(p => p.Name);

            if(overrides!=null)
            {
                foreach (var o in overrides)
                    PropertyMap[o.Key] = o.Value;
            }

			ColumnToPropertyMap = PropertyMap.ToDictionary(kvp => kvp.Value, kvp => pm[kvp.Key]);

		}

		public IEnumerable<T> DequeueResults(QueryResult<Queue<object[]>> results)
		{
			var q = results.Result;
			var names = results.Names;
			var count = names.Length;
			var properties = names
				.Select(n => ColumnToPropertyMap.TryGetValue(n, out PropertyInfo p) ? p : null)
				.ToArray();

			while (q.Count != 0)
			{
				var record = q.Dequeue();
				var model = new T();
				for(var i = 0;i< count; i++)
				{
					var p = properties[i];
					if (p != null)
					{
						var name = names[i];
						var value = record[i];
						if (value == DBNull.Value) value = null;
						p.SetValue(model, value);
					}
				}
				
				yield return model;
			}

			// By using the above routine, we guarantee as enumeration occurs, references are released (dequeued).
		}

		public ISourceBlock<T> Results(out Action<QueryResult<IEnumerable<object[]>>> deferred)
		{
			IEnumerable<object[]> q = null;
			string[] names = null;
			int count = 0;
			PropertyInfo[] properties = null;

			var x = new TransformBlock<object[], T>(record =>
			{
				var model = new T();
				for (var i = 0; i < count; i++)
				{
					var p = properties[i];
					if (p != null)
					{
						var name = names[i];
						var value = record[i];
						if (value == DBNull.Value) value = null;
						p.SetValue(model, value);
					}
				}

				return model;
			});

			deferred = results =>
			{
				q = results.Result;
				names = results.Names;
				count = names.Length;
				properties = names
					.Select(n => ColumnToPropertyMap.TryGetValue(n, out PropertyInfo p) ? p : null)
					.ToArray();

				foreach (var record in q) if (!x.Post(record)) break;
				x.Complete();
			};

			return x;
		}

		public ISourceBlock<T> Results(out Action<QueryResult<ISourceBlock<object[]>>> deferred)
		{
			ISourceBlock<object[]> q = null;
			string[] names = null;
			int count = 0;
			PropertyInfo[] properties = null;

			var x = new TransformBlock<object[], T>(record =>
			{
				var model = new T();
				for (var i = 0; i < count; i++)
				{
					var p = properties[i];
					if (p != null)
					{
						var name = names[i];
						var value = record[i];
						if (value == DBNull.Value) value = null;
						p.SetValue(model, value);
					}
				}

				return model;
			});

			deferred = results =>
			{
				q = results.Result;
				names = results.Names;
				count = names.Length;
				properties = names
					.Select(n => ColumnToPropertyMap.TryGetValue(n, out PropertyInfo p) ? p : null)
					.ToArray();

				q.LinkTo(x);
				q.Completion.ContinueWith(t => x.Complete());
				x.Completion.ContinueWith(t => q.Complete());
			};

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
			return DequeueResults(results);
		}

	}
}
