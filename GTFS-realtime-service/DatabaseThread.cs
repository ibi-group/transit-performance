using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    class DatabaseThread
    {
        private EventQueue eventQueue;
        ILog Log;
        String sqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
        

        internal void ThreadRun()
        {
            //ClearDatabase();
            while(true)
            {
                List<Event> eventList = eventList = new List<Event>();
                while(eventQueue.GetCount() > 0)
                {
                   // eventList = new List<Event>();
                    Event _event = eventQueue.Dequeue();
                    eventList.Add(_event);
                }
                if(eventList.Count > 0)
                {
                    AddEventsToDatabase(eventList);
                }
                eventList.Clear();
                Thread.Sleep(100);
            }
        }

        private void ClearDatabase()
        {
            String sqlQuery = "delete from dbo.rt_eventtt";
            SqlConnection conn = new SqlConnection(sqlConnectionString);
            SqlCommand cmd = new SqlCommand(sqlQuery,conn);
            conn.Open();
            cmd.ExecuteNonQuery();
            conn.Close();
        }

        private void AddEventsToDatabase(List<Event> eventList)
        {
            try
            {
                Log.Info("Start AddEventsToDatabase");

                DataTable dataTable = GetEventTable();
                AddRowsToTable(dataTable, eventList);

                Log.Info("End AddRowsToTable");

                InsertInDatabase(dataTable);

                Log.Info("End InsertInDatabase");
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
            }
        }

        private void InsertInDatabase(DataTable dataTable)
        {
            try
            {
                SqlConnection sqlConnection = new SqlConnection(sqlConnectionString);
                sqlConnection.Open();
                using (SqlBulkCopy s = new SqlBulkCopy(sqlConnectionString, SqlBulkCopyOptions.FireTriggers))
                {
                    s.DestinationTableName = dataTable.TableName;
                    foreach (var column in dataTable.Columns)
                        s.ColumnMappings.Add(column.ToString(), column.ToString());
                    s.WriteToServer(dataTable);
                }
                sqlConnection.Close();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
                Log.Error(e.StackTrace);
            }
        }
        
        private void AddRowsToTable(DataTable dataTable, List<Event> eventList)
        {
           List<string> ignoreRoutes= GetIgnoreRoute();
           foreach(Event _event in eventList)
           {
               if (!ignoreRoutes.Contains(_event.routeId))
               {
                   AddEventToRow(dataTable, _event);   
               }
               else
               {
                   Log.Info("Ignored route: "+_event.routeId);
               }
           }
        }
        
        /*
        private void AddRowsToTable(DataTable dataTable, List<Event> eventList)
        {
            List<string> acceptRoutes = GetAcceptRoute();
            foreach (Event _event in eventList)
            {
                if (acceptRoutes.Contains(_event.routeId))
                {
                    AddEventToRow(dataTable, _event);
                }
                else
                {
                    //Log.Info("Ignored route: " + _event.routeId);
                }
            }
        }
        */

        private List<string> GetIgnoreRoute()
        {
            string ignoreRoutes = ConfigurationManager.AppSettings["IgnoreRoutes"];
            List<string> listOfIgnoreRoutes = new List<string>(ignoreRoutes.Split(','));
            return listOfIgnoreRoutes;
        }

        private List<string> GetAcceptRoute()
        {
            string ignoreRoutes = ConfigurationManager.AppSettings["AcceptRoutes"];
            List<string> listOfIgnoreRoutes = new List<string>(ignoreRoutes.Split(','));
            return listOfIgnoreRoutes;
        }

        private void AddEventToRow(DataTable dataTable, Event _event)
        {
            DataRow eventRow = dataTable.NewRow();
            eventRow["service_date"] = _event.serviceDate;
            eventRow["route_id"] = _event.routeId;
            eventRow["trip_id"] = _event.tripId;
            eventRow["direction_id"] = null == _event.directionId ? (object)DBNull.Value : _event.directionId;
            eventRow["stop_id"] = _event.stopId;
            eventRow["vehicle_id"] = _event.vehicleId;
            eventRow["vehicle_label"] = _event.vehicleLabel;
            eventRow["event_type"] = _event.eventType;
            eventRow["event_time"] = _event.eventTime;
            eventRow["stop_sequence"] = _event.stopSequence;
            eventRow["file_time"] = _event.fileTimestamp;
          
            dataTable.Rows.Add(eventRow);

        }

        DataTable GetEventTable()
        {
            DataTable eventTable = new DataTable("dbo.rt_event");
            
            DataColumn service_date = new DataColumn("service_date",typeof(DateTime));
            DataColumn route_id = new DataColumn("route_id", typeof(String));
            DataColumn trip_id = new DataColumn("trip_id",typeof(String));
            DataColumn direction_id = new DataColumn("direction_id",typeof(int));
            direction_id.AllowDBNull = true;
            DataColumn stop_id = new DataColumn("stop_id",typeof(string));
            DataColumn vehicle_id = new DataColumn("vehicle_id",typeof(string));
            DataColumn vehicle_label = new DataColumn("vehicle_label", typeof(string));
            DataColumn event_type = new DataColumn("event_type", typeof(string));
            DataColumn event_time = new DataColumn("event_time",typeof(int));
            DataColumn stop_sequence = new DataColumn("stop_sequence", typeof(int));
            DataColumn file_time = new DataColumn("file_time", typeof(int));

            eventTable.Columns.Add(service_date);
            eventTable.Columns.Add(route_id);
            eventTable.Columns.Add(trip_id);
            eventTable.Columns.Add(direction_id);
            eventTable.Columns.Add(stop_id);
            eventTable.Columns.Add(vehicle_id);
            eventTable.Columns.Add(vehicle_label);
            eventTable.Columns.Add(event_type);
            eventTable.Columns.Add(event_time);
            eventTable.Columns.Add(stop_sequence);
            eventTable.Columns.Add(file_time);
            return eventTable;

        }

        public DatabaseThread(ILog Log, EventQueue eventQueue)
        {
            // TODO: Complete member initialization
            this.Log = Log;
            this.eventQueue = eventQueue;
        }
    }
}
