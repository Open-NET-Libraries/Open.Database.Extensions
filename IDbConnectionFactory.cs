using System.Data;

namespace Open.Database.Extensions
{
    public interface IDbConnectionFactory<out TConn>
        where TConn : IDbConnection
    {
		TConn Create();
    }

    public interface IDbConnectionFactory
		: IDbConnectionFactory<IDbConnection>
	{
		
    }
}