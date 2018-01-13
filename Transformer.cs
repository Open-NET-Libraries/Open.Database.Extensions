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
        public readonly IReadOnlyList<string> PropertyNames;

        public Transformer()
        {
            Type = typeof(T);
            Properties = Type.GetProperties();
            PropertyNames = Properties.Select(p => p.Name).ToList().AsReadOnly();
        }

        public T Transform(IDictionary<string,object> entry)
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
            return model;
        }

    }
}
