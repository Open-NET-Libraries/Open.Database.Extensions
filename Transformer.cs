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
        public HashSet<string> PropertyNames => new HashSet<string>(PropertyMap.Keys);
        public HashSet<string> ColumnNames => new HashSet<string>(PropertyMap.Values);

        public Transformer(IEnumerable<KeyValuePair<string, string>> overrides = null)
        {
            Type = typeof(T);
            Properties = Type.GetProperties();
            PropertyMap = Properties.Select(p=>p.Name).ToDictionary(n=>n);

            if(overrides!=null)
            {
                foreach (var o in overrides)
                    PropertyMap[o.Key] = o.Value;
            }
        }

        public T TransformAndClear(IDictionary<string, object> entry)
        {
            var model = new T();
            foreach (var p in Properties)
            {
                var columnName = PropertyMap[p.Name];
                if (entry.ContainsKey(columnName))
                {
                    var value = entry[columnName];
                    if (value == DBNull.Value) value = null;
                    p.SetValue(model, value);
                }
            }
			entry.Clear();
            return model;
        }

		public IEnumerable<T> Transform(Queue<Dictionary<string,object>> q)
		{
			while (q.Count != 0)
			{
				yield return TransformAndClear(q.Dequeue());
			}

			// By using the above routine, we guarantee as enumeration occurs, references are released.
		}

	}
}
