using System.Data;
using System.Data.SqlClient;

namespace Open.Database.Extensions.SqlClient
{

	/// <summary>
	/// Default SqlConnectionFactory for generating SqlConnections.
	/// </summary>
    public class SqlConnectionFactory : IDbConnectionFactory, IDbConnectionFactory<SqlConnection>
    {
        string _connectionString;

		/// <summary>
		/// Default injectable connection factory constructor.
		/// </summary>
		/// <param name="connectionString">Required connection string value.</param>
        public SqlConnectionFactory(string connectionString)
        {
            _connectionString = connectionString;
        }

		/// <summary>
		/// Method for generating SqlConnections.
		/// </summary>
		/// <returns></returns>
        public SqlConnection Create()
        {
            return new SqlConnection(_connectionString);
        }

		IDbConnection IDbConnectionFactory<IDbConnection>.Create()
		{
			return Create();
		}
	}
}
