using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Open.Database.Extensions
{
	public abstract class StoredProcedureBase<TConnFactory, TDbType, TThis>
		where TConnFactory : class, IDbConnectionFactory
		where TDbType : struct
		where TThis : StoredProcedureBase<TConnFactory, TDbType, TThis>
	{
		protected TConnFactory ConnectionFactory;

		protected StoredProcedureBase(TConnFactory connFactory, string name, List<Param> @params = null)
		{
			ConnectionFactory = connFactory ?? throw new ArgumentNullException("connFactory");
			Name = name ?? throw new ArgumentNullException("name");
			Params = @params ?? new List<Param>();
			UseScalar = false;
			Timeout = 30;
		}

		protected StoredProcedureBase(TConnFactory connFactory, string name, params Param[] @params) : this(connFactory, name, @params.ToList())
		{

		}

		public string Name { get; set; }
		public List<Param> Params { get; set; }
		public bool UseScalar { get; set; }

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

		public TThis SetUseScalar(bool useScalar = true)
		{
			UseScalar = useScalar;
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
			using (var cmd = con.CreateCommand(CommandType.StoredProcedure, Name, Timeout))
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
			using (var cmd = con.CreateCommand(CommandType.StoredProcedure, Name, Timeout))
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
			using (var cmd = con.CreateCommand(CommandType.StoredProcedure, Name, Timeout))
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

		protected class Transformer<T>
			where T : new()
		{
			readonly Type _type;
			public Transformer()
			{
				_type = typeof(T);
			}

			public T Transform(IDataRecord r)
			{
				var e = new T();
				for (var i = 0; i < r.FieldCount; i++)
				{
					var n = r.GetName(i);
					var f = _type.GetProperty(n);
					if (f == null) continue;
					var value = r.GetValue(i);
					if (value == DBNull.Value) value = null;
					f.SetValue(e, value);
				}
				return e;
			}
		}

		public List<T> ToList<T>()
			where T : new()
		{
			var x = new Transformer<T>();
			return IterateReaderInternal(x.Transform).ToList();
		}

		public T[] ToArray<T>()
			where T : new()
		{
			var x = new Transformer<T>();
			return IterateReaderInternal(x.Transform).ToArray();
		}

		public int ExecuteNonQuery()
		{
			using (var con = ConnectionFactory.Create())
			using (var cmd = con.CreateCommand(CommandType.StoredProcedure, Name, Timeout))
			{
				AddParams(cmd);
				con.Open();
				return UseScalar
					? (int)cmd.ExecuteScalar()
					: cmd.ExecuteNonQuery();
			}
		}

		public DataTable LoadTable()
		{
			var table = new DataTable();
			ExecuteReader(reader => table.Load(reader));
			return table;
		}



	}
}
