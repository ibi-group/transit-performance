using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;

namespace GTFS
{
    class GTFSMigrateProcess
    {
        string sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;
        string primarySchema = ConfigurationManager.AppSettings["PrimarySchemaName"];
        string secondarySchema = ConfigurationManager.AppSettings["SecondarySchemaName"];
        ILog Log;
        internal bool BeginMigration(ILog _Log)
        {
            Log = _Log;

            RunStoredProc();
            bool migrationSuccessful = false;
            CreateScehma();
            List<String> tableNames = GetTableNames(primarySchema);
            if (ExecuteDropTableQuery(tableNames))
            {
                migrationSuccessful = MigrateTables();

                /*
                 * Once the database has been updated, then only change the local feed info file.
                 * */
                string useFeedInfo = ConfigurationManager.AppSettings["UseFeedInfo"].ToUpper();

                if (migrationSuccessful && "TRUE".Equals(useFeedInfo))
                {
                    String feedInfoFileUrl = ConfigurationManager.AppSettings["FeedInfoFileUrl"];
                    DownloadFile("feed_info.txt", feedInfoFileUrl);
                    Log.Info("Updated feed info file with latest version");
                }
            }

            return migrationSuccessful;
        }

        private void RunStoredProc()
        {
            
                String connectionString1 = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
                //using (var conn = new SqlConnection(connectionString1))
                //using (var command = new SqlCommand("dbo.UpdateGTFSNextStep1", conn)
                //{
                //    CommandType = CommandType.Text
                //})
                //{
                //    conn.Open();
                   
                //    command.CommandTimeout = 3600;
                //    command.ExecuteNonQuery();
                //    conn.Close();
                //}
                //Log.Info("UpdateGTFSNextStep1 procedure completed");

                using (var conn = new SqlConnection(connectionString1))
                using (var command = new SqlCommand("dbo.UpdateGTFSNext", conn)
                {
                    CommandType = CommandType.Text
                })
                {
                    conn.Open();
                    command.CommandTimeout = 3600;
                    command.ExecuteNonQuery();
                    conn.Close();
                }
                Log.Info("UpdateGTFSNext procedure completed");
            
           
        }

        private void DownloadFile(string outputFileName, string Url)
        {
            using (WebClient Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }
            Log.Info("Download of file " + outputFileName + " successful.");
        }

        private Boolean ExecuteDropTableQuery(List<string> tableNames)
        {
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlTransaction trans = sqlConnection.BeginTransaction();
            try
            {
                foreach (string table in tableNames)
                {
                    string tableName = table;
                    string schemaName = primarySchema;
                    String sqlQuery = @"IF OBJECT_ID ('" + primarySchema + "." + tableName + @"', 'U') IS NOT NULL DROP TABLE " + schemaName + "." + tableName + ";";
                    SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection, trans);
                    cmd.ExecuteNonQuery();
                    Log.Info("Dropped table " + schemaName + "." + tableName + " from database.");
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                trans.Rollback();
                return false;
            }
            sqlConnection.Close();
            return true;
        }

        private bool MigrateTables()
        {
            List<String> queryList = new List<string>();
            List<String> tableNames = GetTableNames(secondarySchema);
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();

            foreach (string table in tableNames)
            {
                StringBuilder sbr = new StringBuilder();
                sbr.Append("ALTER SCHEMA ");
                sbr.Append(primarySchema);
                sbr.Append(" TRANSFER ");
                sbr.Append(secondarySchema);
                sbr.Append(".");
                sbr.Append(table);
                queryList.Add(sbr.ToString());
            }

            SqlTransaction trans = sqlConnection.BeginTransaction();
            try
            {
                foreach (string sqlquery in queryList)
                {
                    SqlCommand cmd1 = new SqlCommand(sqlquery, sqlConnection, trans);
                    cmd1.ExecuteNonQuery();
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                trans.Rollback();
                Log.Error(e.Message);
                return false;
            }

            sqlConnection.Close();
            Log.Info("Migration Successful");
            return true;
        }
        private List<string> GetTableNames(string schemaName)
        {
            string sqlQuery = @"SELECT TABLE_NAME 
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '" + schemaName + "'";
            List<String> tableNames = new List<string>();
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            SqlDataReader reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    tableNames.Add(reader.GetString(0));
                }
            }
            reader.Close();
            sqlConnection.Close();
            return tableNames;
        }

        /*
         * Check if the scehma exists in the database.
         * If it does not exist, create one.
         */
        private void CreateScehma()
        {
            String databaseName = ConfigurationManager.AppSettings["DatabaseName"];
            string sqlQuery = @"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + primarySchema + "') EXEC( 'CREATE SCHEMA " + primarySchema + "' )";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();

            sqlConnection.Close();
        }

    }
}
