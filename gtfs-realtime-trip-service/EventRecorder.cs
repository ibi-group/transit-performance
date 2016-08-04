using GtfsRealtimeLib;
using log4net;
using Newtonsoft.Json;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace gtfs_realtime_trip_service
{
    /*
     * This class operates on entities and produces events.
     * */
    class EventRecorder
    {
        Dictionary<EntityIdentifier,Entity> CurrentEntities;
        ILog Log;

        BlockingQueue<Event> UpdateQueue;
        BlockingQueue<Event> InsertQueue;

        ulong FileTimestamp;

        List<string> AcceptList = new List<string>();

        public EventRecorder(BlockingQueue<Event> InsertEventQueue, BlockingQueue<Event> UpdateEventQueue, ILog Log)
        {
            // TODO: Complete member initialization
            this.InsertQueue = InsertEventQueue;
            this.UpdateQueue = UpdateEventQueue;
            this.Log = Log;
        }

        internal void RecordEvents()
        {
            
            string outputFileName = GetOutputFileName();
            string url = GetUrl();
            int cycleTime = GetCycleTime();
            CreateAcceptList();
            // Above part is executed only once when thread is started.

            while(true)
            {
                try
                {
                    DownloadFile(outputFileName, url);

                    bool doReset = IsResetTime();
                    if (doReset)
                        DoReset();
                    else
                    {         
                        RecordEvents(outputFileName);
                    }
                }
 
                catch(Exception e)
                {
                    Log.Debug("Something went wrong in Event recorder. Look below for message and stack trace.");
                    while (e != null)
                    {
                        Log.Error(e.Message);
                        e = e.InnerException;
                    }
                }

                // Pause this thread for cycleTime
                Thread.Sleep(cycleTime);
            }

        }

        private void CreateAcceptList()
        {
            string tem = ConfigurationManager.AppSettings["ACCEPTROUTE"];
            tem = tem.Trim();
            string[] arr = tem.Split(',');
            foreach(var x in arr)
            {
                AcceptList.Add(x);
            }
            
        }

        private void RecordEvents(string outputFileName)
        {
            Log.Debug("Begin RecordEvents");
            FeedMessage feedMessage = GetFeedMesssages(outputFileName);
            if (feedMessage == null)
                return;
            WriteFeedMessageToFile(feedMessage);

            ulong fileTimestamp = feedMessage.header.timestamp;
            if(fileTimestamp == FileTimestamp)
            {
                Log.Info("Current file has same timestamp as previous one.");
                return;
            }
            else
            {
                ProcessFeedMessages(feedMessage);
                FileTimestamp = fileTimestamp;
            }
            Log.Debug("End RecordEvents");
        }

        private void ProcessFeedMessages(FeedMessage feedMessage)
        {
            if(CurrentEntities == null)
            {
                CurrentEntities = GetEntites(feedMessage);
                GenerateEvents(CurrentEntities);
            }
            else
            {
                Dictionary<EntityIdentifier, Entity> newEntities = GetEntites(feedMessage);
                ProcessNewEvents(newEntities);
            }
        }

        /*
         * Here we do two things. First we find, which events will be updated.
         * Second update the events in Current Entities.
         * */
        private void ProcessNewEvents( Dictionary<EntityIdentifier,Entity> newEntities)
        {
           foreach(var entity in newEntities)
           {
               if(CurrentEntities.ContainsKey(entity.Key))
               {
                   // see if stop time has changed. if stop has change it means there is change in 
                   // prediction. we need to update it in database. so put that event in update 
                   // queue.
                   if(CurrentEntities[entity.Key].EventTime != entity.Value.EventTime)
                   {
                       UpdateEvent(entity.Value);
                   }
                   CurrentEntities[entity.Key] = entity.Value; // This is where magic happens
               }
               else
               {
                   // if current entities do not have new one, add that entity to current list
                   CurrentEntities.Add(entity.Key, entity.Value);
                   // also generate an insert event for new one.
                   InsertEvent(entity.Value);
               }
           }
        }

        private void InsertEvent(Entity entity)
        {
            EventType eventType = entity.GetEventType();
            if (eventType.Equals(EventType.PRA))
                InsertArrivalEvent(entity);
            else
                InsertDepartureEvent(entity);
        }

        private void UpdateEvent(Entity entity)
        {
            EventType eventType = entity.GetEventType();
            if (eventType.Equals(EventType.PRA))
                UpdateArrivalEvent(entity);
            else
                UpdateDepartureEvent(entity);
        }

        private void UpdateArrivalEvent(Entity entity)
        {
            Event _event = CreateEvent(entity);
            UpdateQueue.Enqueue(_event);
        }

        private void UpdateDepartureEvent(Entity entity)
        {
                Event _event = CreateEvent(entity);
                UpdateQueue.Enqueue(_event);
        }

        private Dictionary<EntityIdentifier, Entity> GetEntites(FeedMessage feedMessage)
        {
            Dictionary<EntityIdentifier, Entity> entities = new Dictionary<EntityIdentifier, Entity>();
            List<FeedEntity> feedEntities = feedMessage.entity;

                foreach (var feedEntity in feedEntities)
                {               
                    if(AcceptList.Contains(feedEntity.trip_update.trip.route_id))
                    {
                        string tripId = feedEntity.trip_update.trip.trip_id;
                        string vehicleId = feedEntity.trip_update.vehicle == null ? null : feedEntity.trip_update.vehicle.id;
                        string routeId = feedEntity.trip_update.trip.route_id;
                        string _serviceDate = feedEntity.trip_update.trip.start_date;
                        DateTime serviceDate = DateTime.ParseExact(_serviceDate, "yyyyMMdd", CultureInfo.InvariantCulture);
                        ulong fileTimestamp = feedMessage.header.timestamp;
                        uint? directionId = feedEntity.trip_update.trip.direction_id;
                        foreach (var stop in feedEntity.trip_update.stop_time_update)
                        {
                            string stopId = stop.stop_id;
                            uint stopSequence = stop.stop_sequence;

                            //add arrival entity
                            if (stop.arrival != null)
                            {
                                long eventTimeArrival = stop.arrival.time;
                                EventType eventType = EventType.PRA;
                                Entity entity = new Entity(serviceDate, routeId, tripId, stopId, stopSequence, vehicleId, eventType, eventTimeArrival, fileTimestamp, directionId);
                                EntityIdentifier eId = new EntityIdentifier(tripId, stopSequence, serviceDate, eventType);
                                try
                                {

                                    if (entities.ContainsKey(eId))
                                        entities.Remove(eId);
                                    entities.Add(eId, entity);
                                }
                                catch (Exception e)
                                {
                                    Log.Debug(entities.ContainsKey(eId));
                                    Entity et = entities[eId];
                                    Log.Debug(et.TripId + "--" + et.StopSequence);
                                    Log.Debug(eId.ToString());
                                    Log.Error(e.Message);
                                }
                            }

                            //add departure entity
                            if (stop.departure != null)
                            {
                                long eventTimeDeparture = stop.departure.time;
                                EventType eventType = EventType.PRD;
                                Entity entity = new Entity(serviceDate, routeId, tripId, stopId, stopSequence, vehicleId, eventType, eventTimeDeparture, fileTimestamp, directionId);
                                EntityIdentifier eId = new EntityIdentifier(tripId, stopSequence, serviceDate, eventType);
                                try
                                {

                                    if (entities.ContainsKey(eId))
                                        entities.Remove(eId);
                                    entities.Add(eId, entity);
                                }
                                catch (Exception e)
                                {
                                    Log.Debug(entities.ContainsKey(eId));
                                    Entity et = entities[eId];
                                    Log.Debug(et.TripId + "--" + et.StopSequence);
                                    Log.Debug(eId.ToString());
                                    Log.Error(e.Message);
                                }
                            }
                        }
                    }
                }
            return entities;
        }



        private void GenerateEvents(Dictionary<EntityIdentifier, Entity> Entities)
        {
            foreach(var entity in Entities)
            {
                EventType eventType = entity.Value.GetEventType();
                if (eventType.Equals(EventType.PRA))
                    InsertArrivalEvent(entity.Value);
                else
                    InsertDepartureEvent(entity.Value);
            }
        }

        private void InsertDepartureEvent(Entity entity)
        {
            Event _event = CreateEvent(entity);
            InsertQueue.Enqueue(_event);   
        }

        private void InsertArrivalEvent(Entity entity)
        {
            Event _event = CreateEvent(entity);
            InsertQueue.Enqueue(_event);
        }

        private Event CreateEvent(Entity entity)
        {
            DateTime serviceDate = entity.ServiceDate;
            string routeId = entity.RouteId;
            string tripId = entity.TripId;
            string stopId = entity.StopId;
            uint stopSequence = entity.StopSequence;
            string vehicleId = entity.VehicleId;
            EventType eventType = entity._EventType;
            long eventTime = entity.EventTime;
            ulong fileTimestamp = entity.FileTimestamp;
            uint? directionId = entity.DirectionId;
            Event _event = new Event(serviceDate,routeId,tripId,stopId,stopSequence,vehicleId,eventType,eventTime,fileTimestamp,directionId);
            return _event;
        }

       

        /*
         * It serializes feed message object to json like structure
         * and writes it to file named TRIPUPDATES.JSON.
         * */
        private void WriteFeedMessageToFile(FeedMessage feedMessage)
        {
            string jsonFileName = ConfigurationManager.AppSettings["JSONPATH"];
            string text = JsonConvert.SerializeObject(feedMessage,Formatting.Indented);

            File.WriteAllText(jsonFileName, text);
        }

        /**
         * This method deserialise content of .pb file into FeedMessage 
         */
        private FeedMessage GetFeedMesssages(string outputFileName)
        {
            FeedMessage feedMessage = null;
            using(var file = File.OpenRead(outputFileName))
            {
                feedMessage = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessage;
        }

        /*
         * Return the pause time for while loop. If nothing specified
         * set it to 30 seconds.
         * */
        private int GetCycleTime()
        {
            string temp = ConfigurationManager.AppSettings["FREQUENCY"];
            if (string.IsNullOrEmpty(temp))
                return 30000;
            return (int.Parse(temp)) * 1000;
        }

        private void DownloadFile(string outputFileName, string url)
        {
            Log.Debug("File downloaded");
                using (WebClient client = new WebClient())
                {              
                    client.DownloadFile(url, outputFileName);
                }           
        }

        private void DoReset()
        {
            Log.Debug("Doing reset.");
            ArchiveManager arm = new ArchiveManager() ;
           bool archiveSuccesful = arm.ArchiveData(Log);
            if(archiveSuccesful)
            {
                Thread.Sleep(2000);
                Environment.Exit(1); // Not the best approach.
            }
        }

        private bool IsResetTime()
        {
            string nowTime = DateTime.Now.ToShortTimeString();
            string resetTime = ConfigurationManager.AppSettings["RESETTIME"];
            resetTime = (String.IsNullOrEmpty(resetTime)) == true ? "3:00 AM" : resetTime;
            return nowTime.Equals(resetTime);
        }

        private string GetUrl()
        {
            string url = ConfigurationManager.AppSettings["URL"];
            return url;
        }

        /*
         * Return the file name of the output file.
         * This file name gives the location of the file 
         * by returning full path.
         * */
        private string GetOutputFileName()
        {
            string outputFileName = ConfigurationManager.AppSettings["FILEPATH"];
            return outputFileName;
        }
    }
}
