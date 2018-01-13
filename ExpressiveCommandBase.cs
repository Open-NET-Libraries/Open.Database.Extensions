using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace Open.Database.Extensions
{
    public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
        where TConnection : class, IDbConnection
        where TCommand : class, IDbCommand
        where TDbType : struct
        where TThis : ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
    {
        protected IDbConnectionFactory<TConnection> ConnectionFactory;

        protected const int DEFAULT_SECONDS_TIMEOUT = 30;

        protected ExpressiveCommandBase(
            IDbConnectionFactory<TConnection> connFactory,
            CommandType type,
            string command,
            List<Param> @params = null)
        {
            ConnectionFactory = connFactory ?? throw new ArgumentNullException(nameof(connFactory));
            Type = type;
            Command = command ?? throw new ArgumentNullException(nameof(command));
            Params = @params ?? new List<Param>();
            Timeout = DEFAULT_SECONDS_TIMEOUT;
        }

        protected ExpressiveCommandBase(
            IDbConnectionFactory<TConnection> connFactory,
            CommandType type,
            string command,
            params Param[] @params)
            : this(connFactory, type, command, @params.ToList())
        {

        }

        public string Command { get; set; }
        public CommandType Type { get; set; }

        public List<Param> Params { get; protected set; }

        public ushort Timeout { get; set; }

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

        public TThis SetTimeout(ushort seconds)
        {
            Timeout = seconds;
            return (TThis)this;
        }

        protected abstract void AddParams(TCommand command);

        public void Execute(Action<TCommand> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                con.Open();
                handler(c);
            }
        }

        public T Execute<T>(Func<TCommand, T> handler)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                con.Open();
                return handler(c);
            }
        }

        // ** This should remain protected as there is a high risk of holding a connection open if left publicly accessible.
        protected IEnumerable<T> IterateReaderInternal<T>(Func<IDataRecord, T> transform)
        {
            using (var con = ConnectionFactory.Create())
            using (var cmd = con.CreateCommand(Type, Command, Timeout))
            {
                var c = cmd as TCommand;
                if (c == null) throw new InvalidCastException($"Actual command type ({cmd.GetType()}) is not compatible with expected command type ({typeof(TCommand)}).");
                AddParams(c);
                con.Open();
                using (var reader = c.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    while (reader.Read())
                    {
                        yield return transform(reader);
                    }
                }
            }
        }

        public void ExecuteReader(Action<IDataReader> handler)
            => Execute(command => command.ExecuteReader(handler));

        public T ExecuteReader<T>(Func<IDataReader, T> handler)
            => Execute(command => command.ExecuteReader(handler));

        public void IterateReader(Action<IDataRecord> handler)
            => Execute(command => command.IterateReader(handler));

        public void IterateReaderWhile(Func<IDataRecord, bool> handler)
            => Execute(command => command.IterateReaderWhile(handler));

        public int ExecuteNonQuery()
            => Execute(command => command.ExecuteNonQuery());

        public object ExecuteScalar()
            => Execute(command => command.ExecuteScalar());

        public T ExecuteScalar<T>()
            => (T)ExecuteScalar();

        public DataTable LoadTable()
            => Execute(command => command.ToDataTable());

        // ** This should remain protected as there is a high risk of holding a connection open if left publicly accessible.
        protected IEnumerable<Dictionary<string, object>> IterateReaderInternal()
            => IterateReaderInternal(r => r.ToDictionary());

        // ** This should remain protected as there is a high risk of holding a connection open if left publicly accessible.
        protected IEnumerable<Dictionary<string, object>> IterateReaderInternal(HashSet<string> columnNames)
             => IterateReaderInternal(r => r.ToDictionary(columnNames));

        public List<Dictionary<string, object>> ToList(HashSet<string> columnNames)
            => IterateReaderInternal(columnNames).ToList();

        public List<Dictionary<string, object>> ToList(IEnumerable<string> columnNames)
            => ToList(new HashSet<string>(columnNames));

        public List<Dictionary<string, object>> ToList(params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal().ToList()
                : ToList(new HashSet<string>(columnNames));

        public Dictionary<string, object>[] ToArray(HashSet<string> columnNames)
            => IterateReaderInternal(columnNames).ToArray();

        public Dictionary<string, object>[] ToArray(IEnumerable<string> columnNames)
            => ToArray(new HashSet<string>(columnNames));

        public Dictionary<string, object>[] ToArray(params string[] columnNames)
            => columnNames.Length == 0
                ? IterateReaderInternal().ToArray()
                : ToArray(new HashSet<string>(columnNames));

        public void ToTargetBlock<T>(Func<IDataRecord, T> transform, ITargetBlock<T> target)
            => IterateReaderWhile(r => target.Post(transform(r)));

        public ISourceBlock<T> AsSourceBlock<T>(Func<IDataRecord, T> transform)
        {
            var source = new BufferBlock<T>();
            ToTargetBlock(transform, source);
            return source;
        }

        public IEnumerable<T> Results<T>()
            where T : new()
        {
            var x = new Transformer<T>();
            // ToArray pulls extracts all the data first.  Then we use the .Select to transform into the desired model T;
            return ToArray(x.PropertyNames)
                .Select(entry => x.Transform(entry));
        }
    }
}
