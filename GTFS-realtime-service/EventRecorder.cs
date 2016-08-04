using GtfsRealtimeLib;
using log4net;
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

namespace GTFS_realtime_service
{
    class EventRecorder
    {
         ILog Log;
         ulong previousFileTimestamp, staleFileTimestampThreshold;
         Dictionary<VehicleEntity, Entity> EternalEntitySet;
         EventQueue eventQueue;

        /// <summary>
        /// Begin recording the events
        /// </summary>
        internal void RecordEvents( )
        {
            String outputFileName =  ConfigurationManager.AppSettings["FilePath"];
            String url = ConfigurationManager.AppSettings["URL"];

            string staleFileTimestampThresholdStr = ConfigurationManager.AppSettings["StaleFileTimestampThreshold"];
            staleFileTimestampThreshold = String.IsNullOrEmpty(staleFileTimestampThresholdStr) == true ? 300 : ulong.Parse(staleFileTimestampThresholdStr);

            string updateCycleStr = ConfigurationManager.AppSettings["Frequency"];
            int updateCycle = String.IsNullOrEmpty(updateCycleStr) == true ? 15 : int.Parse(updateCycleStr);
            updateCycle = updateCycle * 1000; // convert seconds to miliseconds

            while(true)
            {
                Log.Info("Start RecordEvents iteration");
                try
                {
                    DownloadFile(outputFileName, url);
                    RecordAnyNewEvents(outputFileName);
                }
                catch(Exception e)
                {
                    Log.Error(e.Message);
                    Log.Error(e.StackTrace);
                }
                Log.Info("End RecordEvents iteration");
                Thread.Sleep(updateCycle);
            }
        }

        private void RecordAnyNewEvents(string outputFileName)
        {
            FeedMessage feedMessages = GetFeedMessages(outputFileName);
            /*
             * Check the file time stamp, if it is same as previous one,
             * do not process any further as there is no update
             */
            ulong currentFileTimestamp = feedMessages.header.timestamp;

            Log.Info("currentFileTimestamp: " + currentFileTimestamp + " previousFileTimestamp: " + previousFileTimestamp);

            if(currentFileTimestamp != previousFileTimestamp)
            {
                ProcessFeedMessages(feedMessages);
                previousFileTimestamp = currentFileTimestamp;
            }
            //else timestamps are equal...if currenteFileTimestamp is more than x seconds old, then exit and
            //service will be automatically restarted...restart may fix problem with old data
            else if (GetEpochTime() - currentFileTimestamp > staleFileTimestampThreshold)
            {
                Log.Error("currentFileTimestamp is stale. currentFileTimestamp: " + currentFileTimestamp + " currentTime: " + GetEpochTime() + " Exiting now...");
                Environment.Exit(2);
            }
        }


        private void ProcessFeedMessages(FeedMessage feedMessages)
        {
            Dictionary<VehicleEntity, Entity> EphemeralEntitySet = GetEntitySet(feedMessages);
            if(EternalEntitySet == null)
            {
                InitializeEternalEntitySet(EphemeralEntitySet);
            }
            else
            {
                UpdateEternalEntitySet(EphemeralEntitySet);
            }
        }
        /// <summary>
        /// Update the Eternal Entity Set. Identify any arrival or departure events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet)
        {
            foreach (KeyValuePair<VehicleEntity, Entity> entry in EphemeralEntitySet)
            {
                if (EternalEntitySet.ContainsKey(entry.Key))
                {
                    UpdateEternalEntitySet(EphemeralEntitySet, entry);
                }
                else
                {
                    AddNewEntityToEternalEntitySet(entry);
                }
            }
        }

        private void AddNewEntityToEternalEntitySet(KeyValuePair<VehicleEntity, Entity> entry)
        {
            if (VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(entry.Value.currentStopStatus))
            {
                EternalEntitySet.Add(entry.Key, entry.Value);
                GenerateArrivalEvent(entry.Key);
            }
        }
        /// <summary>
        /// Idnetify if any entity is updated and generate the appropriate events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        /// <param name="entry"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, KeyValuePair<VehicleEntity, Entity> entry)
        {
            int parameterCode = IdentifyIfEntityChanged(EternalEntitySet[entry.Key], entry.Value);
            switch (parameterCode)
            {
                case 0:
                    ProcessStopSequenceChange(EphemeralEntitySet, entry.Key);
                    break;
                case 1:
                    ProcessStopStatusChange(EphemeralEntitySet, entry.Key);
                    break;
                case 2:
                    ProcessTripChange(EphemeralEntitySet, entry.Key);
                    break;
                default:
                    break;
            }
        }

        private void ProcessTripChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 0);
        }

        private void ProcessStopStatusChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            if (EternalEntitySet[vehicleEntity].departure != 1)
            {
                GenerateDepartureEvent(EphemeralEntitySet, vehicleEntity);
                if (EternalEntitySet[vehicleEntity].departure == 1 && EternalEntitySet[vehicleEntity].arrival == 1)
                {
                    UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 1);
                }
            }
        }

        private void ProcessStopSequenceChange(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            String currentStatus = EphemeralEntitySet[vehicleEntity].currentStopStatus;
            GenerateDepartureEvent(EphemeralEntitySet, vehicleEntity);
            if (EternalEntitySet[vehicleEntity].departure == 1 && EternalEntitySet[vehicleEntity].arrival == 1)
            {
                UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 1);
            }
            if (VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(currentStatus))
            {
                UpdateEternalEntitySet(EphemeralEntitySet, vehicleEntity, 0);
                GenerateArrivalEvent(vehicleEntity);
            }
        }

        /// <summary>
        /// It updates the entities in the eternal eneity set with the new values.
        /// Update will depend on the update type that is supplied.
        /// If update type is 1, we completely remove that entity from the set.
        /// If update type is 0, we update that entity with new value.
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        /// <param name="vehicleEntity"></param>
        /// <param name="updateType"></param>
        private void UpdateEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity, int updateType)
        {
            if (updateType == 1)
            {
                EternalEntitySet.Remove(vehicleEntity);
                return;
            }
            if (updateType == 0)
            {
                Entity e = new Entity(EphemeralEntitySet[vehicleEntity]);
                EternalEntitySet[vehicleEntity] = e;
            }
        }

        private void GenerateDepartureEvent(Dictionary<VehicleEntity, Entity> EphemeralEntitySet, VehicleEntity vehicleEntity)
        {
            String startDate = EternalEntitySet[vehicleEntity].startDate;
            DateTime serviceDate = DateTime.ParseExact(startDate, "yyyyMMdd", CultureInfo.InvariantCulture);
            String routeId = EphemeralEntitySet[vehicleEntity].routeId;
            String tripId = EphemeralEntitySet[vehicleEntity].tripId;
            String stopId = EternalEntitySet[vehicleEntity].stopId;
            uint stopSequence = EternalEntitySet[vehicleEntity].stopSequence;
            String vehicleId = vehicleEntity.VehicleId;
            String vehicleLabel = vehicleEntity.VehicleLabel;
            EventType eventType = EventType.DEP;
            ulong actualTime = EphemeralEntitySet[vehicleEntity].vehicletimeStamp;
            ulong fileTimestamp = EphemeralEntitySet[vehicleEntity].fileTimestamp;
            uint? directionId = EphemeralEntitySet[vehicleEntity].directionId;
            Event newEvent = new Event(serviceDate, routeId, tripId, stopId, stopSequence, vehicleId, vehicleLabel, eventType, actualTime, fileTimestamp, directionId);
            String eventString = newEvent.ToString();
            EternalEntitySet[vehicleEntity].departure = 1;
            Log.Info(eventString);
            eventQueue.Enqueue(newEvent);
        }

        private int IdentifyIfEntityChanged(Entity entity1, Entity entity2)
        {
            if (entity1.stopSequence != (entity2.stopSequence))
            {
                return 0;
            }
            if (!entity1.currentStopStatus.Equals(entity2.currentStopStatus))
            {
                return 1;
            }
            if (!entity1.tripId.Equals(entity2.tripId))
            {
                return 2;
            }
            return -1;
        }

        /// <summary>
        /// Initialize the Eteranal Entity set for the first run.
        /// Also generate the arrival events
        /// </summary>
        /// <param name="EphemeralEntitySet"></param>
        private void InitializeEternalEntitySet(Dictionary<VehicleEntity, Entity> EphemeralEntitySet)
        {
            EternalEntitySet = new Dictionary<VehicleEntity, Entity>();
            foreach (KeyValuePair<VehicleEntity, Entity> entry in EphemeralEntitySet)
            {
                if (VehicleCurrentStopStatus.STOPPED_AT.ToString().Equals(entry.Value.currentStopStatus))
                {
                    EternalEntitySet.Add(entry.Key, entry.Value);
                    GenerateArrivalEvent(entry.Key);
                    EternalEntitySet[entry.Key].arrival = 1;
                }
            }
        }

        /// <summary>
        /// Returns a customized Entity set from the feed messages
        /// </summary>
        /// <param name="feedMessages"></param>
        /// <returns></returns>
        private Dictionary<VehicleEntity, Entity> GetEntitySet(FeedMessage feedMessages)
        {
            EntityFactory entityFactory = new EntityFactory();
            Dictionary<VehicleEntity, Entity> entitySet = entityFactory.ProduceEntites(feedMessages);
            return entitySet;
        }

        /// <summary>
        /// This method deserialize the .pb file to generate FeedMessage object
        /// </summary>
        /// <param name="outputFileName"></param>
        /// <returns></returns>
        private FeedMessage GetFeedMessages(string outputFileName)
        {
            FeedMessage feedMessages = null;
            using (var file = File.OpenRead(outputFileName))
            {
                feedMessages = Serializer.Deserialize<FeedMessage>(file);
            }
            return feedMessages;
        }

        /// <summary>
        /// Download the file from the url specified url to the output file.
        /// </summary>
        /// <param name="outputFileName"></param>
        /// <param name="Url"></param>
        private  void DownloadFile(string outputFileName, string Url)
        {
            using (WebClient Client = new WebClient())
            {
                Client.DownloadFile(Url, outputFileName);
            }      
        }

        private  void GenerateArrivalEvent(VehicleEntity vehicleEntity)
        {

            //DateTime serviceDate = DateTime.Now;
            String startDate = EternalEntitySet[vehicleEntity].startDate;
            DateTime serviceDate = DateTime.ParseExact(startDate,  "yyyyMMdd", CultureInfo.InvariantCulture);
            String routeId = EternalEntitySet[vehicleEntity].routeId;
            String tripId = EternalEntitySet[vehicleEntity].tripId;
            String stopId = EternalEntitySet[vehicleEntity].stopId;
            uint stopSequence = EternalEntitySet[vehicleEntity].stopSequence;
            String vehicleId = vehicleEntity.VehicleId;
            String vehicleLabel = vehicleEntity.VehicleLabel;
            EventType eventType = EventType.ARR;
            ulong actualTime = EternalEntitySet[vehicleEntity].vehicletimeStamp;
            ulong fileTimestamp = EternalEntitySet[vehicleEntity].fileTimestamp;
            uint? directionId = EternalEntitySet[vehicleEntity].directionId;
             Event newEvent = new Event(serviceDate, routeId, tripId, stopId, stopSequence, vehicleId, vehicleLabel, eventType, actualTime, fileTimestamp, directionId);
            String eventString = newEvent.ToString();
            EternalEntitySet[vehicleEntity].arrival = 1;
            Log.Info(eventString);
            eventQueue.Enqueue(newEvent);
        }

        private static ulong GetEpochTime()
        {
            DateTime epochPoint = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            ulong epoch = (ulong)DateTime.Now.ToUniversalTime().Subtract(epochPoint).TotalSeconds;
            return epoch;
        }

        public EventRecorder(EventQueue eventQueue, ILog Log)
        {
            // TODO: Complete member initialization
            this.eventQueue = eventQueue;
            this.Log = Log;
        }
    }
}
