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

namespace gtfs_realtime_trip_service
{
    class DatabaseThread
    {
        private string SqlConnectionString = ConfigurationManager.ConnectionStrings["DatabaseConnectionString"].ToString();
        private BlockingQueue<Event> InsertQueue;
        private BlockingQueue<Event> UpdateQueue;
        ILog Log;
       

        public DatabaseThread(ILog Log, BlockingQueue<Event> InsertEventQueue, BlockingQueue<Event> UpdateEventQueue)
        {
            // TODO: Complete member initialization
            this.Log = Log;
            this.InsertQueue = InsertEventQueue;
            this.UpdateQueue = UpdateEventQueue;
        }
        private DataTable EventTable { get; set; }

        internal void ThreadRun()
        {
            List<Event> updateEventList = null;
            List<Event> insertEventList = null;

            while(true)
            {
                updateEventList = new List<Event>();
                insertEventList = new List<Event>();
                try
                {
                    while (UpdateQueue.GetCount() > 0)
                    {
                        Event _event = UpdateQueue.Dequeue();
                        updateEventList.Add(_event);
                        insertEventList.Add(_event);
                    }
                    while (InsertQueue.GetCount() > 0)
                    {
                        Event _event = InsertQueue.Dequeue();
                        insertEventList.Add(_event);
                    }
                    UpdateDatabaseTable(updateEventList, insertEventList);
                }
                catch(Exception ex)
                {
                    Log.Error(ex.Message);
                    Log.Error(ex.StackTrace);
                }
                Thread.Sleep(100);
            }
        }

        private void UpdateDatabaseTable(List<Event> updateEventList, List<Event> insertEventList)
        {
            if(updateEventList.Count > 0)
                DeleteRows(updateEventList);
            if(insertEventList.Count > 0)
                InsertRows(insertEventList);
        }

        private void InsertRows(List<Event> insertEventList)
        {
              if(EventTable == null)
              {
                  CreateEventTable();
              }
              EventTable.Clear();
              AddRows(insertEventList);
              Log.Debug("Trying to insert "+insertEventList.Count + " rows in database.");
            using(SqlConnection connection = new SqlConnection(SqlConnectionString))
            {
                using(SqlBulkCopy sbc = new SqlBulkCopy(connection))
                {
                    connection.Open();
                    sbc.DestinationTableName = EventTable.TableName;
                    foreach(var column in EventTable.Columns)
                    {
                        sbc.ColumnMappings.Add(column.ToString(),column.ToString());
                    }
                    sbc.WriteToServer(EventTable);
                    connection.Close();
                    Log.Debug("Inserted  "+EventTable.Rows.Count + " rows in database.");
                }
            }
        }

        private void AddRows(List<Event> insertEventList)
        {
            foreach(var _event in insertEventList)
            {
                DataRow eventRow = EventTable.NewRow();
                eventRow["service_date"] = _event.ServiceDate;
                eventRow["route_id"] = _event.RouteId ;
                eventRow["trip_id"] = _event.TripId;
                eventRow["direction_id"] = _event.DirectionId == null ? (object)DBNull.Value : _event.DirectionId;
                eventRow["stop_id"] = _event.StopId;
                eventRow["vehicle_id"] = _event.VehicleId;
                eventRow["event_type"] = _event._EventType == EventType.PRA ? "PRA" : "PRD";
                eventRow["event_time"] = _event.EventTime;
                eventRow["file_time"] = _event.FileTimestamp;
                eventRow["event_identifier"] = _event.GetEventIdentifier();
                eventRow["stop_sequence"] = _event.StopSequence;
                EventTable.Rows.Add(eventRow);
             }
        }

        private void CreateEventTable()
        {
            EventTable = new DataTable();
            EventTable.TableName = "dbo.event_rt_trip";
            DataColumn route_id = new DataColumn("route_id", typeof(string));
            DataColumn trip_id = new DataColumn("trip_id", typeof(string));
            DataColumn direction_id = new DataColumn("direction_id", typeof(int));
            DataColumn stop_id = new DataColumn("stop_id", typeof(string));
            DataColumn event_type = new DataColumn("event_type", typeof(string));
            DataColumn event_time = new DataColumn("event_time", typeof(int));
            DataColumn file_time = new DataColumn("file_time", typeof(int));
            DataColumn event_identifier = new DataColumn("event_identifier", typeof(string));
            DataColumn service_date = new DataColumn("service_date", typeof(DateTime));
            DataColumn vehicle_id = new DataColumn("vehicle_id", typeof(string));
            DataColumn stop_sequence = new DataColumn("stop_sequence", typeof(int));

            EventTable.Columns.Add(route_id);
            EventTable.Columns.Add(trip_id);
            EventTable.Columns.Add(direction_id);
            EventTable.Columns.Add(stop_id);
            EventTable.Columns.Add(event_type);
            EventTable.Columns.Add(event_time);
            EventTable.Columns.Add(file_time);
            EventTable.Columns.Add(event_identifier);
            EventTable.Columns.Add(service_date);
            EventTable.Columns.Add(vehicle_id);
            EventTable.Columns.Add(stop_sequence);
        }

        private void DeleteRows(List<Event> updateEventList)
        {
            string deleteList = GetDeleteList(updateEventList);
            using(SqlConnection connection = new  SqlConnection(SqlConnectionString))
            {
                connection.Open();
                string query = "DELETE FROM dbo.event_rt_trip WHERE event_identifier in " + deleteList;
                SqlCommand cmd = new SqlCommand();
                cmd.CommandText = query;
                cmd.CommandTimeout = 20;
                cmd.Connection = connection;
                Log.Debug("Begin delete operation.");
                int rowsDeleted = cmd.ExecuteNonQuery();
                Log.Debug("Number of rows deleted from event table "+rowsDeleted + ".");
            }
        }

        private string GetDeleteList(List<Event> updateEventList)
        {
            //Log.Debug("Building delete list");
            StringBuilder sbr = new StringBuilder();
            sbr.Append("(");
            foreach(var _event in updateEventList)
            {
                string temp = _event.GetEventIdentifier();
                sbr.Append("'");
                sbr.Append(temp);
                sbr.Append("'");
                sbr.Append(",");
            }
            sbr.Append("''");
            sbr.Append(")");
            //Log.Debug("Builded delete list with "+ updateEventList.Count +" items");
            return sbr.ToString();
        }

        
    }
}
