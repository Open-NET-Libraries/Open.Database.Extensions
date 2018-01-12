using System.Data;
using System.Data.SqlClient;

namespace Open.Database.Extensions.SqlClient
{

    public class SqlConnectionFactory : IDbConnectionFactory, IDbConnectionFactory<SqlConnection>
    {
        string _connectionString;
        public SqlConnectionFactory(string connectionString)
        {
            _connectionString = connectionString;
        }

        public SqlConnection Create()
        {
            return new SqlConnection(_connectionString);
        }

		IDbConnection IDbConnectionFactory<IDbConnection>.Create()
		{
			throw new System.NotImplementedException();
		}
	}
}
