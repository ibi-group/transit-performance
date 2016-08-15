using log4net;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GTFS
{
    class GTFSUpdateProcess
    {
        ILog Log;
        String sqlConnectionString;
        String secondarySchema;

        internal Boolean BeginGTFSUpdateProcess(ILog _log)
        {
            Log = _log;
            sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;
            secondarySchema = ConfigurationManager.AppSettings["SecondarySchemaName"];
            Boolean updateGTFSSuccessful = false;
            try
            {
                GTFSTableCollection tableCollection = ConvertGTFSFileToJsonObject();
                CreateGTFSTables(tableCollection);
                bool PopulateGTFSTableSuccessful = PopulateGTFSTable(tableCollection);
                if (PopulateGTFSTableSuccessful)
                {
                    Parallel.ForEach(tableCollection, gtfsTable => ExecuteCreateIndex(gtfsTable));
                    updateGTFSSuccessful = true;
                }
                else
                    updateGTFSSuccessful = false;
            }
            catch (AggregateException ex)
            {
                Log.Error("Exception in GTFS update process. \n" + ex.StackTrace);
                foreach(var e in ex.Flatten().InnerExceptions)
                { Log.Error(e.Message); }
                Log.Error(ex.Message);
            }
            return updateGTFSSuccessful;
        }

        private bool PopulateGTFSTable(GTFSTableCollection tableCollection)
        {
            Log.Info("Start populating gtfs_next schema tables.");
            bool requiredFileExists = CheckIfAllRequiredFilesExists(tableCollection);
            if (requiredFileExists)
            {
                List<String> requiredFileList = GetRequiredFileList(tableCollection);
                UpdateAnyNewColumn(requiredFileList);
                Parallel.ForEach(requiredFileList, requiredFile => UploadData(requiredFile));
                return true;
            }
            return false;
        }

        private void UploadData(string requiredFile)
        {
            DataTable datatable = GetCorrespondingTable(requiredFile);
            CopyDataIntoTable(datatable, requiredFile);
        }

        private void UploadData(List<string> requiredFileList)
        {
            foreach (string requiredFile in requiredFileList)
            {
                DataTable datatable = GetCorrespondingTable(requiredFile);
                CopyDataIntoTable(datatable, requiredFile);
            }
        }

        private void CopyDataIntoTable(DataTable datatable, string fileName)
        {
            string GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            List<String> columnList = GetColumnList(GTFSPath + "/" + fileName + ".txt");
            List<String> dateColumns = new List<string>();
            Log.Info(datatable.TableName);
            using (StreamReader sr = new StreamReader(GTFSPath + "/" + fileName + ".txt"))
            {
                sr.ReadLine();
                int batchSize = 0;
                while (sr.Peek() > -1)
                {
                    string line = sr.ReadLine();
                    List<string> dataRowValues = ParseLine(line);
                    AddDataRow(columnList, datatable, dataRowValues);
                    batchSize++;
                    if (batchSize == 10000)
                    {
                        BulkInsertIntoDatabase(datatable);
                        batchSize = 0;
                        datatable.Clear();
                    }
                }
            }

            BulkInsertIntoDatabase(datatable);
        }

        /*
         * Copy the datatable to the database. 
         */
        private void BulkInsertIntoDatabase(DataTable datatable)
        {
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            using (SqlBulkCopy s = new SqlBulkCopy(sqlConnection))
            {
                s.DestinationTableName = datatable.TableName;
                foreach (var column in datatable.Columns)
                    s.ColumnMappings.Add(column.ToString(), column.ToString());

                Log.Info("BulkInsert for " + datatable.TableName);

                s.BulkCopyTimeout = 180;
                s.WriteToServer(datatable);
            }
            sqlConnection.Close();
        }

        /*
         *  Add a new data row.
         */
        private void AddDataRow(List<String> columnList, DataTable datatable, List<string> dataRowValues)
        {
            DataRow newDataRow = datatable.NewRow();
            int i = 0;

            foreach (string data in dataRowValues)
            {
                Boolean flag = false;
                String columnName = columnList[i];
                Type columnType = datatable.Columns[columnName].DataType;

                if (columnType == typeof(DateTime))
                {
                    DateTime dateTime;
                    if (!DateTime.TryParse(data, out dateTime))
                    {
                        if (!DateTime.TryParseExact(data, "yyyyMMdd", new CultureInfo("en-US"), DateTimeStyles.None, out dateTime))
                        {
                            throw new Exception(data + " is not in a valid date format.");
                        }

                    }
                    newDataRow[columnName] = dateTime;
                    flag = true;
                }
                if (columnType == typeof(Boolean))
                {
                    if (data.Equals("1"))
                        newDataRow[columnName] = true;
                    if (data.Equals("0"))
                        newDataRow[columnName] = false;
                    flag = true;

                }
                if (!flag)
                {
                    if(String.IsNullOrEmpty(data))
                    { newDataRow[columnName] = DBNull.Value; }
                    else
                    { newDataRow[columnName] = data; }
                }
                i++;
            }


            datatable.Rows.Add(newDataRow);
        }

        /*
         *  Given a file name, return a corresponding database table. 
         */
        private DataTable GetCorrespondingTable(string requiredFile)
        {
            DataTable datatable = new DataTable(secondarySchema + "." + requiredFile);
            String sqlQuery = @"select * from " + secondarySchema + "." + requiredFile;
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            datatable.Load(cmd.ExecuteReader());
            sqlConnection.Close();
            return datatable;

        }

        /*
         *  Get a list of column for a particular table from the database
         *  
         */
        private List<string> GetColumnListFromDatabase(string tableName)
        {
            string sqlQuery = @"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + secondarySchema + "." + tableName + "')";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            SqlDataReader reader = cmd.ExecuteReader();
            List<String> sqlColumnList = new List<string>();
            while (reader.Read())
            {
                string columnName = reader.GetString(0);
                sqlColumnList.Add(columnName);
            }
            reader.Close();
            sqlConnection.Close();
            return sqlColumnList;
        }

        /*
         *  This method returns the list of column from the file
         *  Column name are present in the first line of GTFS data file
         */
        private List<String> GetColumnList(string file)
        {
            string line = null;
            using (StreamReader sr = new StreamReader(file))
            {
                line = sr.ReadLine();
            }
            return ParseLine(line);
        }

        /*
         *  Parse the comma seperated line, and return the list of strings 
         */
        private List<String> ParseLine(string line)
        {
            String[] feedValues = string.IsNullOrEmpty(line) ? null : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))").Matches(line).Cast<Match>().Select(match => match.Groups[4].Success ? match.Groups[4].Value : ((match.Groups[2].Success ? match.Groups[2].Value : "") + (match.Groups[3].Success ? match.Groups[3].Value : ""))).ToArray();
            return feedValues.ToList<String>();
        }

        /*
         * If a column in present in the data file but not in the corresponfding 
         * database table, add the column to the table in the database.
         */
        private void UpdateAnyNewColumn(List<string> requiredFileList)
        {
            string GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            foreach (string file in requiredFileList)
            {
                List<string> fileColumnList = GetColumnList(GTFSPath + "/" + file + ".txt");
                List<String> sqlColumnList = GetColumnListFromDatabase(file);
                foreach (string column in fileColumnList)
                {
                    if (!sqlColumnList.Contains(column))
                    {
                        AddColumnToTable(column, file);
                    }
                }
            }
        }

        /*
         *  Add a column to an existing table. 
         */
        private void AddColumnToTable(string columnName, string file)
        {
            string sqlQuery = @"ALTER TABLE " + secondarySchema + "." + file + " ADD " + columnName + " VARCHAR(MAX) ";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            SqlDataReader reader = cmd.ExecuteReader();
            sqlConnection.Close();
        }

        /*
         *  From the GTFS table collection return the list of the 
         *  rewquired tables ie files.
         */
        private List<string> GetRequiredFileList(GTFSTableCollection tableCollection)
        {
            List<String> requiredFileList = new List<string>();
            foreach (GTFSTable gtfsTable in tableCollection)
            {
                if (gtfsTable.required)
                {
                    requiredFileList.Add(gtfsTable.name);
                }
            }
            return requiredFileList;
        }

        /*
         *  From the downloaded folder, check if all the required files, mentioned in
         *  the gtfs_file_structure are present.
         */
        private Boolean CheckIfAllRequiredFilesExists(GTFSTableCollection tableCollection)
        {
            string GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            List<String> fileNames = GetFileNames(GTFSPath, "*.txt");
            bool requiredFileExists = false;
            foreach (GTFSTable gtfsTable in tableCollection)
            {
                if (gtfsTable.required)
                {
                    if (fileNames.Contains(gtfsTable.name))
                    {
                        requiredFileExists = true;
                    }
                    else
                    {
                        requiredFileExists = false;
                        Log.Info("Required file " + gtfsTable.name + " does not exist.");
                        return requiredFileExists;
                    }
                }
            }
            return requiredFileExists;
        }

        /*
         * This method returns a list of file names based on filter.
         * If the filter is .txt, it will return list of filenames of type text.
         * The file names are stripped off from their extension.
         * */
        private List<String> GetFileNames(string path, string filter)
        {
            DirectoryInfo d = new DirectoryInfo(path);
            FileInfo[] fileInfoList = d.GetFiles(filter);
            List<String> fileNames = new List<string>();
            foreach (FileInfo fileInfo in fileInfoList)
            {
                string fileName = Path.GetFileNameWithoutExtension(fileInfo.FullName);
                fileNames.Add(fileName);
            }
            return fileNames;
        }

        private void CreateGTFSTables(GTFSTableCollection tableCollection)
        {
            CreateTablesFromCollection(tableCollection);
            Log.Info("Table creation successful in database.");
        }



        private GTFSTableCollection ConvertGTFSFileToJsonObject()
        {
            Log.Info("Parse the gtfs_file_structure and identify the schema tables.");
            string gtfs_file_structure = ConfigurationManager.AppSettings["GTFSFileStructure"];
            String jsonString = null;
            using (StreamReader sr = new StreamReader(gtfs_file_structure))
            {
                jsonString = sr.ReadToEnd();
            }
            GTFSTableCollection tableCollection = SchemaContainer.GetTables(jsonString).tables;
            return tableCollection;
        }

        /*
         *  Create schema if it does not exists.
         *  For each table, drop the table from the database.
         *  Create a new table in the database
         *  Create indices on the column of tables as required.
         */
        private void CreateTablesFromCollection(GTFSTableCollection tableCollection)
        {
            CreateScehma();
            foreach (GTFSTable gtfsTable in tableCollection)
            {
                ExecuteDropTableQuery(gtfsTable);
                ExecuteCreateTableQuery(gtfsTable);
            }
        }

        /*
         * Check if the scehma exists in the database.
         * If it does not exist, create one.
         */
        private void CreateScehma()
        {
            String databaseName = ConfigurationManager.AppSettings["DatabaseName"];
            string sqlQuery = @"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + secondarySchema + "') EXEC( 'CREATE SCHEMA " + secondarySchema + "' )";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
        }

        /*
         *  Create the indexes 
         */
        private void ExecuteCreateIndex(GTFSTable gtfsTable)
        {
            string tableName = gtfsTable.name;
            foreach (GTFSColumn column in gtfsTable.columns)
            {
                String columnName = column.name;
                if (column.index)
                {
                    string sqlQuery = @"CREATE NONCLUSTERED INDEX IX_" + tableName + "_" + columnName + " ON " + secondarySchema + "." + tableName + " ( " + columnName + " ) ";
                    SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
                    sqlConnection.Open();
                    SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
                    cmd.CommandTimeout = 120;
                    int value = cmd.ExecuteNonQuery();
                    sqlConnection.Close();
                    Log.Info("Created NONCLUSTERED INDEX IX_" + tableName + "_" + columnName + " ON " + secondarySchema + "." + tableName + " (" + columnName + ") ");
                }
            }
        }

        /*
         *  Create the table in the database 
         */
        private void ExecuteCreateTableQuery(GTFSTable gtfsTable)
        {
            String tableName = gtfsTable.name;
            List<String> columnsList = GetColumns(gtfsTable.columns);
            List<String> primaryKeys = GetPrimaryKeys(gtfsTable.columns);
            String sqlQuery = @"CREATE TABLE " + secondarySchema + "." +
                tableName +
                " ( " +
                String.Join(" , ", columnsList) +
                (primaryKeys.Count > 0 ? @" PRIMARY KEY (" + string.Join(", ", primaryKeys) + " )" : "") + @");";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Created table " + secondarySchema + "." + tableName + " in database.");
        }

        private List<string> GetPrimaryKeys(GTFSColumnSet gtfsColumnSet)
        {
            List<String> primaryKeys = new List<string>();
            foreach (GTFSColumn column in gtfsColumnSet)
            {
                if (column.primaryKey)
                {
                    primaryKeys.Add(column.name);
                }
            }
            return primaryKeys;
        }

        /*
         *  Given a set of GTFS column of a table,
         *  returns a list of column from the set with their
         *  specification for the database. 
         *  Specification include: column datatype and can have null or not.
         */
        private List<string> GetColumns(GTFSColumnSet gtfsColumnSet)
        {
            List<String> columnList = new List<string>();
            foreach (GTFSColumn column in gtfsColumnSet)
            {
                String columnString = column.name + " " + column.type + " " + (column.primaryKey | !column.allowNull ? " NOT NULL " : " NULL");
                columnList.Add(columnString);
            }
            return columnList;
        }

        /*
         *  Check if the table already exists in the database.
         *  If it exists drop it from the database.
         */
        private void ExecuteDropTableQuery(GTFSTable gtfsTable)
        {
            string tableName = gtfsTable.name;
            string schemaName = secondarySchema;
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            String sqlQuery = @"IF OBJECT_ID ('" + secondarySchema + "." + tableName + @"', 'U') IS NOT NULL DROP TABLE " + schemaName + "." + tableName + ";";
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Dropped table " + schemaName + "." + tableName + " from database.");
        }

        private List<string> GetListOfPrimaryKeys(GTFSTable gtfsTable)
        {
            List<String> listOfPrimaryKeys = new List<string>();
            foreach (GTFSColumn column in gtfsTable.columns)
            {
                if (column.primaryKey)
                {
                    listOfPrimaryKeys.Add(column.name);
                }
            }
            return listOfPrimaryKeys;
        }
    }
}
