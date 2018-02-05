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
		public readonly string[] _propertyNames;
		public HashSet<string> PropertyNames => new HashSet<string>(_propertyNames);


        public Transformer()
        {
            Type = typeof(T);
            Properties = Type.GetProperties();
			_propertyNames = Properties.Select(p => p.Name).ToArray();
        }

        public T TransformAndClear(IDictionary<string,object> entry)
        {
            var model = new T();
            foreach (var p in Properties)
            {
                var name = p.Name;
                if (entry.ContainsKey(name))
                {
                    var value = entry[name];
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
