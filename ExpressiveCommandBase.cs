using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Open.Database.Extensions
{
	public abstract class ExpressiveCommandBase<TConn, TDbType, TThis>
		where TConn : class, IDbConnection
		where TDbType : struct
		where TThis : ExpressiveCommandBase<TConn, TDbType, TThis>
	{
		protected IDbConnectionFactory<TConn> ConnectionFactory;

		protected ExpressiveCommandBase(
			IDbConnectionFactory<TConn> connFactory,
			CommandType type,
			string command,
			List<Param> @params = null)
		{
			ConnectionFactory = connFactory ?? throw new ArgumentNullException("connFactory");
			Type = type;
			Command = command ?? throw new ArgumentNullException("command");
			Params = @params ?? new List<Param>();
			Timeout = 30;
		}

		protected ExpressiveCommandBase(
			IDbConnectionFactory<TConn> connFactory,
			CommandType type,
			string command,
			params Param[] @params)
			: this(connFactory, type, command, @params.ToList())
		{

		}

		public string Command { get; set; }
		public CommandType Type { get; set; }

		public List<Param> Params { get; set; }

		public int Timeout { get; set; }

		public struct Param
		{
			public string Name { get; set; }
			public object Value { get; set; }
			public TDbType? Type { get; set; }
		}

		public TThis AddParam(string name, object value, TDbType type)
		{
			Params.Add(new Param
			{
				Name = name,
				Value = value,
				Type = type
			});

			return (TThis)this;
		}

		public TThis AddParam(string name, object value)
		{
			var p = new Param { Name = name };
			if (value != null) p.Value = value;
			else p.Value = DBNull.Value;

			Params.Add(p);
			return (TThis)this;
		}

		public TThis AddParam<T>(string name, T? value, TDbType type)
			where T : struct
		{
			var p = new Param { Name = name, Type = type };
			if (value.HasValue) p.Value = value.Value;
			else p.Value = DBNull.Value;

			Params.Add(p);
			return (TThis)this;
		}

		public TThis AddParam<T>(string name, T? value)
			where T : struct
		{
			var p = new Param { Name = name };
			if (value.HasValue) p.Value = value.Value;
			else p.Value = DBNull.Value;

			Params.Add(p);
			return (TThis)this;
		}

		public TThis AddParam(string name)
		{
			Params.Add(new Param
			{
				Name = name
			});

			return (TThis)this;
		}

		public TThis SetTimeout(int seconds)
		{
			Timeout = seconds;
			return (TThis)this;
		}

		protected abstract void AddParams(IDbCommand command);
		public T ExecuteReader<T>(Func<IDataReader, T> handler)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				con.Open();
				using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
					return handler(reader);
			}
		}

		public void ExecuteReader(Action<IDataReader> handler)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				con.Open();
				using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
					handler(reader);
			}
		}

		public void IterateReader(Action<IDataRecord> handler)
			=> ExecuteReader(reader =>
			{
				while (reader.Read())
					handler(reader);
			});

		IEnumerable<T> IterateReaderInternal<T>(Func<IDataRecord, T> transform)
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				con.Open();
				using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
				{
					while (reader.Read())
					{
						yield return transform(reader);
					}
				}
			}
		}

		public List<T> ToList<T>(Func<IDataRecord, T> transform)
		{
			return IterateReaderInternal(transform).ToList();
		}

		public T[] ToArray<T>(Func<IDataRecord, T> transform)
		{
			return IterateReaderInternal(transform).ToArray();
		}

		static T Transform<T>(IDataRecord r)
			where T : new()
		{
			var type = typeof(T);
			var e = new T();
			for (var i = 0; i < r.FieldCount; i++)
			{
				var n = r.GetName(i);
				var value = r.GetValue(i);
				if (value == DBNull.Value) value = null;
				type.GetProperty(n).SetValue(e, value);
			}
			return e;
		}

		//protected class Transformer<T>
		//	where T : new()
		//{
		//	readonly Type _type;
		//	public Transformer()
		//	{
		//		_type = typeof(T);
		//	}

		//	public T Transform(IDataRecord r)
		//	{
		//		var e = new T();
		//		for (var i = 0; i < r.FieldCount; i++)
		//		{
		//			var n = r.GetName(i);
		//			var f = _type.GetProperty(n);
		//			if (f == null) continue;
		//			var value = r.GetValue(i);
		//			if (value == DBNull.Value) value = null;
		//			f.SetValue(e, value);
		//		}
		//		return e;
		//	}
		//}

		protected class DeferredTransformer<T>
			where T : new()
		{
			readonly Type _type;
			readonly HashSet<string> _properties;
			public readonly ConcurrentQueue<Dictionary<string, object>> Values;

			public DeferredTransformer()
			{
				_type = typeof(T);
				_properties = new HashSet<string>(
					_type.GetProperties().Select(p => p.Name));
				Values = new ConcurrentQueue<Dictionary<string, object>>();
			}

			public void Queue(IDataRecord r)
			{
				var e = new Dictionary<string, object>();
				for (var i = 0; i < r.FieldCount; i++)
				{
					var n = r.GetName(i);
					if(_properties.Contains(n))
						e.Add(n, r.GetValue(i));
				}
				Values.Enqueue(e);
			}

			public IEnumerable<T> DequeueAndTransform()
			{
				while(Values.TryDequeue(out Dictionary<string,object> e))
				{
					var r = new T();
					foreach(var n in _properties)
					{
						if(e.ContainsKey(n))
						{
							var value = e[n];
							if (value == DBNull.Value) value = null;
							_type.GetProperty(n).SetValue(r, value);
						}
					}

					yield return r;
				}
			}
		}

		public List<T> ToList<T>()
			where T : new()
		{
			var x = new DeferredTransformer<T>();
			IterateReader(x.Queue);
			return x.DequeueAndTransform().ToList();
		}

		public T[] ToArray<T>()
			where T : new()
		{
			var x = new DeferredTransformer<T>();
			IterateReader(x.Queue);
			return x.DequeueAndTransform().ToArray();
		}

		public int ExecuteNonQuery()
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				con.Open();
				return cmd.ExecuteNonQuery();
			}
		}

		public object ExecuteScalar()
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(Type, Command, Timeout))
			{
				AddParams(cmd);
				con.Open();
				return cmd.ExecuteScalar();
			}
		}

		public T ExecuteScalar<T>()
		{
			return (T)ExecuteScalar();
		}

		public DataTable LoadTable()
		{
			var table = new DataTable();
			ExecuteReader(reader => table.Load(reader));
			return table;
		}

	}
}
