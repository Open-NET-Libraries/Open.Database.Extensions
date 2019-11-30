using Open.Database.Extensions;
using System.Data.SqlClient;

public static partial class Examples
{
	public readonly static DbConnectionFactory ConnectionFactory
		= new DbConnectionFactory(()=>new SqlConnection());

	// Returns true if the transaction is successful.
	public static bool TryTransaction()
	=> ConnectionFactory.Using(connection =>
		// Open a connection and start a transaction.
		connection.ExecuteTransactionConditional(transaction => {

			// First procedure does some updates.
			var count = transaction
				.StoredProcedure("[Updated Procedure]")
				.ExecuteNonQuery();

			// Second procedure validates the results.
			// If it returns true, then the transaction is committed.
			// If it returns false, then the transaction is rolled back.
			return transaction
				.StoredProcedure("[Validation Procedure]")
				.AddParam("@ExpectedCount", count)
				.ExecuteScalar<bool>();
		}));

}

