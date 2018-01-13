using System.Data;

namespace Open.Database.Extensions
{
    public abstract partial class ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
		where TConnection : class, IDbConnection
        where TCommand : class, IDbCommand
        where TDbType : struct
		where TThis : ExpressiveCommandBase<TConnection, TCommand, TDbType, TThis>
	{
        public struct Param
		{
			public string Name { get; set; }
			public object Value { get; set; }
			public TDbType? Type { get; set; }
		}

	}
}
