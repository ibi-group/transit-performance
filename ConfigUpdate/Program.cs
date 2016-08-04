using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ConfigUpdate
{
    class Program
    {
        private static ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        static String sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ConnectionString;

        static int Main(string[] args)
        {
            try
            {


                XmlConfigurator.Configure();
                String savedDataFile = ConfigurationManager.AppSettings["LastModifiedDateFile"];

                String configFilePath = ConfigurationManager.AppSettings["ConfigStructureFile"];

                String configFilesDirectory = ConfigurationManager.AppSettings["ConfigFilesPath"];

                if (!File.Exists(savedDataFile))
                {
                    RunFirstTime(configFilePath, configFilesDirectory);
                    CreateLastModifiedDateFile(configFilePath, configFilesDirectory);
                    return 0;
                }

                Dictionary<String, String> previousWriteTimes = GetPreviousWriteTime(savedDataFile);

                ConfigTableCollection tableCollection = ConvertConfigFileToJsonObject(configFilePath);

                Dictionary<String, String> currentWriteTimes = GetCurrentWriteTime(tableCollection, configFilesDirectory);

                bool structureChange = CheckStructureInformation(previousWriteTimes, configFilePath);

                if (structureChange)
                {
                    // execute the extra steps
                    RecreateDatabaseTables(configFilePath);
                }

                foreach (String file in currentWriteTimes.Keys)
                {
                    String currentFileWriteTime = currentWriteTimes[file];
                    if (previousWriteTimes.ContainsKey(file))
                    {
                        if (!previousWriteTimes[file].Equals(currentFileWriteTime))
                        {
                            // repopulate
                            UpdateAnyNewColumn(file);
                            PopulateDatabase(file);
                        }
                    }
                    else
                    {
                        // new file is added.
                        UpdateAnyNewColumn(file);
                        PopulateDatabase(file);
                    }
                }

                UpdateLastWriteTime(savedDataFile, currentWriteTimes);
                return 0;
            }
            catch(Exception e)
            {
                return 1;
            }
        }

        private static void UpdateLastWriteTime(string savedDataFile, Dictionary<string, string> currentWriteTimes)
        {
            File.Delete(savedDataFile);
            String configFilePath = ConfigurationManager.AppSettings["ConfigStructureFile"];
            using (System.IO.StreamWriter file = new System.IO.StreamWriter("lastmodified.txt"))
            {
                foreach (String filea in currentWriteTimes.Keys.ToList<String>())
                {
                    file.Write(currentWriteTimes[filea] + "@" + File.GetLastWriteTime(filea + ".csv").ToString() + "$");
                }
                file.Write(Path.GetFileNameWithoutExtension(configFilePath) + "@" + File.GetLastWriteTime(configFilePath).ToString());
            }
        }

        private static void CreateLastModifiedDateFile(string configFilePath, string configFilesDirectory)
        {
            ConfigTableCollection tableCollection = ConvertConfigFileToJsonObject(configFilePath);
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"lastmodified.txt"))
            {
                foreach (ConfigTable table in tableCollection)
                {
                    file.Write(table.name + "@" + File.GetLastWriteTime(""+table.name + ".csv").ToString() + "$");
                }
                file.Write(Path.GetFileNameWithoutExtension(configFilePath) + "@" + File.GetLastWriteTime(configFilePath).ToString());
            }
          
        }

        private static void RunFirstTime(string configFilePath, string configFilesDirectory)
        {
            ConfigTableCollection tableCollection = RecreateDatabaseTables(configFilePath);
            RecreateDatabaseTables(configFilePath);
           
            foreach(ConfigTable table in tableCollection)
            {
                UpdateAnyNewColumn(table.name);
                PopulateDatabase(table.name);
            }

        }


        private static ConfigTableCollection RecreateDatabaseTables(string path)
        {
            ConfigTableCollection tableCollection = ConvertConfigFileToJsonObject(path);
            foreach(ConfigTable table in tableCollection)
            {
                    ExecuteDropTableQuery(table);
                    ExecuteCreateTableQuery(table);
            }
            return tableCollection;
        }

        private static Dictionary<string, string> GetCurrentWriteTime( ConfigTableCollection tableCollection ,string configFilesPath)
        {
            Dictionary<String, String> currentWriteTime = new Dictionary<string, string>();
            String[] files = Directory.GetFiles(configFilesPath,"*.csv");

            List<String> tableList = new List<string>();

            foreach(ConfigTable table in tableCollection)
            {
                tableList.Add(table.name);
            }

            foreach(string file  in files)
            {
                    String s1 = File.GetLastWriteTime(file).ToString(); 
                    String s2 = Path.GetFileNameWithoutExtension(file);
                    if(tableList.Contains(s2))
                    {
                        currentWriteTime[s2] = s1;
                    }    
            }
            return currentWriteTime;
        }

        /*
     * If a column in present in the data file but not in the corresponfding 
     * database table, add the column to the table in the database.
     */
        static private void UpdateAnyNewColumn(string file)
        {
                List<string> fileColumnList = GetColumnList("" + file + ".csv");
                List<String> sqlColumnList = GetColumnListFromDatabase(file);
                foreach (string column in fileColumnList)
                {
                    if (!sqlColumnList.Contains(column))
                    {
                        AddColumnToTable(column, file);
                    }
                }
        }

        /*
         *  Get a list of column for a particular table from the database
         *  
         */
        static private List<string> GetColumnListFromDatabase(string tableName)
        {
            string sqlQuery = @"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('" + "dbo" + "." + tableName + "')";
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
 *  Add a column to an existing table. 
 */
        static private void AddColumnToTable(string columnName, string file)
        {
            string sqlQuery = @"ALTER TABLE dbo"   + "." + file + " ADD " + columnName + " VARCHAR(MAX) ";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            SqlDataReader reader = cmd.ExecuteReader();
            sqlConnection.Close();
        }

        private static bool CheckStructureInformation(Dictionary<string, string> previousWriteTime, string path)
        {
            DateTime currentWriteTime = File.GetLastWriteTime(path);
            String fileName = Path.GetFileNameWithoutExtension(path);

            String previousTime = previousWriteTime[fileName];
            if(currentWriteTime.Equals(previousWriteTime))
            {
                return false;
            }
            return true;
        }

        private static Dictionary<string, string> GetPreviousWriteTime(string savedDataFile)
        {
            Dictionary<String, String> previousWriteTime = new Dictionary<string, string>();

            using (StreamReader sr = new StreamReader(savedDataFile))
            {
                String line = sr.ReadToEnd();
                string[] fileTimestamps = line.Split('$');
                foreach(String file in fileTimestamps)
                {
                    if (!String.IsNullOrEmpty(file))
                    {
                        string[] fileInformation = file.Split('@');
                        String fileName = fileInformation[0];
                        String fileTime = fileInformation[1]; ;
                        //for (int i = 1; i < fileInformation.Length; i++ )
                        //{
                        //    fileTime += fileInformation[i];
                        //}
                        previousWriteTime.Add(fileName, fileTime);
                    }
                }
            }
            return previousWriteTime;
        }

      
        /*
            *  This method returns the list of column from the file
            *  Column name are present in the first line of GTFS data file
            */
       static  private List<String> GetColumnList(string file)
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
       static private List<String> ParseLine(string line)
       {
           String[] feedValues = string.IsNullOrEmpty(line) ? null : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))").Matches(line).Cast<Match>().Select(match => match.Groups[4].Success ? match.Groups[4].Value : ((match.Groups[2].Success ? match.Groups[2].Value : "") + (match.Groups[3].Success ? match.Groups[3].Value : ""))).ToArray();
           return feedValues.ToList<String>();
       }

        static private void CopyDataIntoTable(DataTable datatable, string fileName)
        {
            List<String> columnList = GetColumnList(""+fileName + ".csv");
            List<String> dateColumns = new List<string>();
            Log.Info(datatable.TableName);
            using (StreamReader sr = new StreamReader("" + fileName + ".csv"))
            {
                sr.ReadLine();
                while (sr.Peek() > -1)
                {
                    string line = sr.ReadLine();
                    List<string> dataRowValues = ParseLine(line);
                    AddDataRow(columnList, datatable, dataRowValues);

                }
            }

            BulkInsertIntoDatabase(datatable);
        }

        /*
      * Copy the datatable to the database. 
      */
        static private void BulkInsertIntoDatabase(DataTable datatable)
        {
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            using (SqlBulkCopy s = new SqlBulkCopy(sqlConnection))
            {
                s.DestinationTableName = datatable.TableName;
                foreach (var column in datatable.Columns)
                    s.ColumnMappings.Add(column.ToString(), column.ToString());
                s.WriteToServer(datatable);
            }
            sqlConnection.Close();
        }

        /*
       *  Add a new data row.
       */
        static private void AddDataRow(List<String> columnList, DataTable datatable, List<string> dataRowValues)
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
                    newDataRow[columnName] = String.IsNullOrEmpty(data) ? null : data;
                }
                i++;
            }

            datatable.Rows.Add(newDataRow);
        }

        private static void PopulateDatabase(String requiredFile)
        {
            DataTable datatable = GetCorrespondingTable(requiredFile);
            CopyDataIntoTable(datatable, requiredFile);  
        }

        private static DataTable GetCorrespondingTable(String requiredFile)
        {
            DataTable datatable = new DataTable( "dbo." + requiredFile);
            String sqlQuery = @"select * from "   + "dbo." + requiredFile;
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            datatable.Load(cmd.ExecuteReader());
            sqlConnection.Close();
            return datatable;
        }

        /*
         *  Check if the table already exists in the database.
         *  If it exists drop it from the database.
         */
        static private void ExecuteDropTableQuery(ConfigTable ConfigTable)
        {
            string tableName = ConfigTable.name;
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            String sqlQuery = @"IF OBJECT_ID ('dbo." + tableName + @"', 'U') IS NOT NULL DROP TABLE dbo." + tableName + ";";
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Dropped table dbo"  + "." + tableName + " from database.");
        }

        /*
            *  Create the table in the database 
         */
        static private void ExecuteCreateTableQuery(ConfigTable ConfigTable)
        {
            String tableName = ConfigTable.name;
            List<String> columnsList = GetColumns(ConfigTable.columns);
            List<String> primaryKeys = GetPrimaryKeys(ConfigTable.columns);
            String sqlQuery = @"CREATE TABLE dbo"  + "." +
                tableName +
                " ( " +
                String.Join(" , ", columnsList) +
                (primaryKeys.Count > 0 ? @" PRIMARY KEY (" + string.Join(", ", primaryKeys) + " )" : "") + @");";
            SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
            sqlConnection.Open();
            SqlCommand cmd = new SqlCommand(sqlQuery, sqlConnection);
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            Log.Info("Created table dbo"  + "." + tableName + " in database.");
        }

        static private List<string> GetPrimaryKeys(ConfigColumnSet ConfigColumnSet)
        {
            List<String> primaryKeys = new List<string>();
            foreach (ConfigColumn column in ConfigColumnSet)
            {
                if (column.primaryKey)
                {
                    primaryKeys.Add(column.name);
                }
            }
            return primaryKeys;
        }


        static  private List<string> GetColumns(ConfigColumnSet ConfigColumnSet)
        {
            List<String> columnList = new List<string>();
            foreach (ConfigColumn column in ConfigColumnSet)
            {
                String columnString = column.name + " " + column.type + " " + (column.primaryKey | !column.allowNull ? " NOT NULL " : " NULL");
                columnList.Add(columnString);
            }
            return columnList;
        }

        static private ConfigTableCollection ConvertConfigFileToJsonObject(String path)
        {
            Log.Info("Parse the Config_file_structure and identify the schema tables.");
            string Config_file_structure = path;
            String jsonString = null;
            using (StreamReader sr = new StreamReader(Config_file_structure))
            {
                jsonString = sr.ReadToEnd();
            }
            ConfigTableCollection tableCollection = SchemaContainer.GetTables(jsonString).tables;
            Log.Info("here");
            return tableCollection;
        }
    }
}
