using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Open.Database.Extensions
{
	public class DbStoredProcedure : StoredProcedureBase<IDbConnectionFactory, DbType, DbStoredProcedure>
    {
        public DbStoredProcedure(IDbConnectionFactory connFactory, string name, List<Param> @params = null) : base(connFactory, name, @params)
        {
        }

        protected DbStoredProcedure(IDbConnectionFactory connFactory, string name, params Param[] @params) : this(connFactory, name, @params.ToList())
        {

        }

        protected override void AddParams(IDbCommand command)
        {
            foreach (var p in Params)
            {
                var np = command.AddParameter(p.Name, p.Value);
                if (p.Type.HasValue) np.DbType = p.Type.Value;
            }
        }
    }
}
