using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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


		public IEnumerable<T> Transform(DataReaderResults results)
		{
			var q = results.Values;
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
					var name = names[i];
					var value = record[i];
					var p = properties[i];
					if (p == null) continue;
					if (value == DBNull.Value) value = null;
					p.SetValue(model, value);
				}
				
				yield return model;

			}

			// By using the above routine, we guarantee as enumeration occurs, references are released.
		}

	}
}
