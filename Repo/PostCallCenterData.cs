using Microsoft.Data.SqlClient;

namespace PullTelax.Repo
{
    class PostCallCenterData
    {
        public int InsertRows(string jsonDocuments, string Sproc2Run)
        {
            var rowsaffected = 0;
            var connectionString = "Server=10.10.10.94;Database=Call_DW;Trusted_Connection=True;";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = Sproc2Run; 
                    cmd.Connection = connection;
                    cmd.Parameters.Add(new SqlParameter("@json", jsonDocuments));

                    connection.Open();
                    rowsaffected = cmd.ExecuteNonQuery();
                }
            }

            return rowsaffected;
        }
    }
}
