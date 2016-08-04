using GtfsRealtimeLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace GTFS_realtime_service
{
    class EntityFactory
    {
        internal Dictionary<VehicleEntity, Entity> ProduceEntites(FeedMessage feedMessages)
        {
            List<FeedEntity> feedEntityList = feedMessages.entity;
            Dictionary<VehicleEntity, Entity> vehicleEntitySet = new Dictionary<VehicleEntity, Entity>();
            
            string includeEntitiesWithoutTrip = ConfigurationManager.AppSettings["IncludeEntitiesWithoutTrip"].ToUpper();
            
            foreach (FeedEntity feedEntity in feedEntityList)
            {
                //if a trip id exists for this entity or if config parameter says to include entities without a trip id 
                //then do the following...else skip (discard) this entity
                if (feedEntity.vehicle.trip != null || "TRUE".Equals(includeEntitiesWithoutTrip))
                {
                    String currentStopStatus = feedEntity.vehicle.current_status.ToString();
                    String tripId = feedEntity.vehicle.trip.trip_id;
                    String routeId = feedEntity.vehicle.trip.route_id;
                    String stopId = feedEntity.vehicle.stop_id;
                    uint stopSequence = feedEntity.vehicle.current_stop_sequence;
                    feedEntity.vehicle.current_status.ToString();
                    ulong vehicletimeStamp = feedEntity.vehicle.timestamp;
                    String VehicleId = feedEntity.vehicle.vehicle.id;
                    String VehicleLabel = feedEntity.vehicle.vehicle.label;
                    ulong fileStamp = feedMessages.header.timestamp;
                    string startDate = feedEntity.vehicle.trip.start_date;
                    uint? directionId = feedEntity.vehicle.trip.direction_id;
                    Entity entity = new Entity(tripId, routeId, stopId, stopSequence, currentStopStatus, vehicletimeStamp, fileStamp, startDate, directionId);
                    VehicleEntity vehicleEntity = new VehicleEntity();
                    vehicleEntity.VehicleId = VehicleId;
                    vehicleEntity.VehicleLabel = VehicleLabel;
                    vehicleEntity.tripId = feedEntity.vehicle.trip.trip_id;
                    if (vehicleEntitySet.ContainsKey(vehicleEntity))
                    {
                        // HANDLE THIS CASE DIFFERENTLY, FOR NOW ITS OK
                        vehicleEntitySet.Remove(vehicleEntity);
                    }
                    vehicleEntitySet.Add(vehicleEntity, entity);
                }
        }
        return vehicleEntitySet;
        }
    }
}
