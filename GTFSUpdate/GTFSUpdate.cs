using log4net;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GTFS
{
    class GTFSUpdate
    {
        ILog Log;

        internal bool InitialiseGTFSUpdate(ILog log)
        {
            bool initialisationSuccessful = false;

            Log = log;
            initialisationSuccessful = true;

            return initialisationSuccessful;
        }

        internal int RunGTFSUpdate()
        {
            int updateSuccessful = 1;
            string useFeedInfo = ConfigurationManager.AppSettings["UseFeedInfo"].ToUpper();
            string feedInfoFileUrl = ConfigurationManager.AppSettings["FeedInfoFileUrl"];

            try
            {
                bool feedInfoUpdated = false;

                if ("TRUE".Equals(useFeedInfo))
                {
                    DownloadFile("feed_info_temp.txt", feedInfoFileUrl);
                    feedInfoUpdated = CompareFeedInfoFile();
                }

                if (feedInfoUpdated || "FALSE".Equals(useFeedInfo))
                {
                    Log.Info("Run GTFS schedule database update.");

                    Boolean databaseUpdated = UpdateGTFSDatabase();
                    if(databaseUpdated)
                    { updateSuccessful = 0; }

                }
                else
                {
                    Log.Info("GTFS feed schedule dataset has remain unchanged.");
                    updateSuccessful = 2;
                    return updateSuccessful;
                }
            }
            catch (Exception ex)
            {
                Log.Error("Error in GTFS run.", ex);
                updateSuccessful = 1;
            }

            return updateSuccessful;
        }

        private bool UpdateGTFSDatabase()
        {
            /*
             * Download the .zip file
             * Extract files from zip archive
             * Process data from the files 
             * Update database with new dataset
             */
            Boolean updateGtfsSuccessful = false;
            
            string downloadGTFS = ConfigurationManager.AppSettings["DownloadGTFS"].ToUpper();
            if ("TRUE".Equals(downloadGTFS))
            {
                string GTFSDataSetUrl = ConfigurationManager.AppSettings["GTFSDataSetUrl"];
                string GTFSZipPath = ConfigurationManager.AppSettings["GTFSZipPath"];
                DownloadFile(GTFSZipPath, GTFSDataSetUrl);
                ExtractZipArchive(GTFSZipPath);
            }

            GTFSUpdateProcess gtfsUpdateProcess = new GTFSUpdateProcess();
            updateGtfsSuccessful = gtfsUpdateProcess.BeginGTFSUpdateProcess(Log);
            return updateGtfsSuccessful;
        }

        private void ExtractZipArchive(string path)
        {
            string GTFSPath = ConfigurationManager.AppSettings["GTFSPath"];

            var dir = new DirectoryInfo(GTFSPath);
            if (dir.Exists)
            {
                dir.Delete(true);
                Log.Info("Delete GTFS folder");
            }
            ZipFile.ExtractToDirectory(path, GTFSPath);
            Log.Info("GTFS extraction successful.");
        }

        private bool CompareFeedInfoFile()
        {
            bool feedInfoUpdated = false;
            if (!File.Exists("feed_info.txt"))
            {
                string feedInfoFileUrl = ConfigurationManager.AppSettings["FeedInfoFileUrl"];
                DownloadFile("feed_info.txt", feedInfoFileUrl);
                return true;
            }
            feedInfoUpdated = CompareFields();
            return feedInfoUpdated;
        }

        private bool CompareFields()
        {
            bool feedInfoUpdated = false;

            String local_line_1 = null;
            String local_line_2 = null;

            using (StreamReader sr = new StreamReader("feed_info.txt"))
            {
                local_line_1 = sr.ReadLine();
                local_line_2 = sr.ReadLine();
            }

            String[] local_feedKeys = GetFeedKeys(local_line_1);
            Object[] local_feedValues = GetFeedValues(local_line_2);

            Dictionary<String, Object> local_feed_info_fields = new Dictionary<String, Object>();

            if (local_feedKeys.Length == local_feedValues.Length)
            {
                for (int i = 0; i < local_feedKeys.Length; i++)
                {
                    local_feed_info_fields[local_feedKeys[i]] = local_feedValues[i];
                }
            }

            String downloaded_line_1 = null;
            String downloaded_line_2 = null;

            using (StreamReader sr = new StreamReader("feed_info_temp.txt"))
            {
                downloaded_line_1 = sr.ReadLine();
                downloaded_line_2 = sr.ReadLine();
            }

            String[] downloaded_feedKeys = GetFeedKeys(downloaded_line_1);
            Object[] downloaded_feedValues = GetFeedValues(downloaded_line_2);

            Dictionary<String, Object> downloaded__feed_info_fields = new Dictionary<String, Object>();

            if (downloaded_feedKeys.Length == downloaded_feedValues.Length)
            {
                for (int i = 0; i < downloaded_feedKeys.Length; i++)
                {
                    downloaded__feed_info_fields[downloaded_feedKeys[i]] = downloaded_feedValues[i];
                }
            }

            foreach (KeyValuePair<String, Object> entry in downloaded__feed_info_fields)
            {
                string key = entry.Key;
                Object local_value = null;
                Object downloaded_value = null;

                local_value = local_feed_info_fields[key];
                downloaded_value = downloaded__feed_info_fields[key];

                if (!downloaded_value.Equals(local_value))
                {
                    Log.Info("Field: " + key + " has changed. Value " + local_value + " is changed to " + downloaded_value);
                    feedInfoUpdated = true;
                }
            }
            return feedInfoUpdated;

        }

        private Object[] GetFeedValues(string line_2)
        {
            Object[] feedValues = string.IsNullOrEmpty(line_2) ? null : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))").Matches(line_2).Cast<Match>().Select(match => match.Groups[4].Success ? match.Groups[4].Value : ((match.Groups[2].Success ? match.Groups[2].Value : "") + (match.Groups[3].Success ? match.Groups[3].Value : ""))).ToArray();
            return feedValues;
        }

        private String[] GetFeedKeys(string line_2)
        {
            String[] feedValues = string.IsNullOrEmpty(line_2) ? null : new Regex(@"(,|\n|^)(?:(?:""((?:.|(?:\r?\n))*?)""(?:(""(?:.|(?:\r?\n))*?)"")?)|([^,\r\n]*))").Matches(line_2).Cast<Match>().Select(match => match.Groups[4].Success ? match.Groups[4].Value : ((match.Groups[2].Success ? match.Groups[2].Value : "") + (match.Groups[3].Success ? match.Groups[3].Value : ""))).ToArray();
            return feedValues;
        }


        private void DownloadFile(string outputFileName, string Url)
        {
            using (WebClient Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }
            Log.Info("Download of file " + outputFileName + " successful.");
        }
    }
}
