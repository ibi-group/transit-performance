using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace gtfs_realtime_trip_service
{
    class ArchiveManager
    {
        internal bool ArchiveData(ILog Log)
        {
            string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
           using(SqlConnection connection = new SqlConnection(SqlConnectionString))
           {
               connection.Open();

               string query1 = @"INSERT INTO dbo.event_rt_trip_archive SELECT * FROM dbo.event_rt_trip";
               SqlCommand cmd = new SqlCommand();
               cmd.Connection = connection;
               cmd.CommandText = query1;
               int rowsInserted = cmd.ExecuteNonQuery();
               int rowsDeleted = -2;

               if (rowsInserted > 0)
               {
                   string query2 = "DELETE FROM dbo.event_rt_trip";
                   cmd.CommandText = query2;
                   cmd.CommandTimeout = 30;
                   rowsDeleted = cmd.ExecuteNonQuery();
               }

               if (rowsInserted == rowsDeleted)
               {
                   Log.Debug("Moved "+rowsInserted + " events into archive table.");
                   Log.Debug("Archiving successful");
                   return true;
               }
           }
            return false;
        }
    }
}
