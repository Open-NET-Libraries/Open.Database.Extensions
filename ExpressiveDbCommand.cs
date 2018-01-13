using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Open.Database.Extensions
{
	public class ExpressiveDbCommand : ExpressiveCommandBase<IDbConnection, IDbCommand, DbType, ExpressiveDbCommand>
    {
        public ExpressiveDbCommand(
			IDbConnectionFactory<IDbConnection> connFactory,
			CommandType type,
			string name,
			List<Param> @params = null)
			: base(connFactory, type, name, @params)
        {
        }

        protected ExpressiveDbCommand(
			IDbConnectionFactory<IDbConnection> connFactory,
			CommandType type,
			string name,
			params Param[] @params)
			: this(connFactory, type, name, @params.ToList())
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
