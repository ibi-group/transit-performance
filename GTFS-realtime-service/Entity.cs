using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    class Entity
    {
        public ulong fileTimestamp;
        public  String tripId;
        public String routeId;
        public String stopId;
        public uint stopSequence;
        public String currentStopStatus;
        public ulong vehicletimeStamp;
        public int arrival=0;
        public int departure=0;
        public string startDate;
        public uint? directionId;

        public Entity(string tripId, string routeId, string stopId, uint stopSequence, String currentStopStatus, ulong vehicletimeStamp,ulong fileStamp,string startDate, uint? directionId)
        {
            this.tripId = tripId;
            this.routeId = routeId;
            this.stopId = stopId;
            this.stopSequence = stopSequence;
            this.currentStopStatus = currentStopStatus;
            this.vehicletimeStamp = vehicletimeStamp;
            arrival = 0;
            departure = 0;
            this.fileTimestamp = fileStamp;
            this.startDate = startDate;
            this.directionId = directionId;
        }

        public Entity()
        {
            // TODO: Complete member initialization
        }

        public Entity(Entity entity)
        {
            // TODO: Complete member initialization
            this.tripId = entity.tripId;
            this.routeId = entity.routeId;
            this.stopId = entity.stopId;
            this.stopSequence = entity.stopSequence;
            this.currentStopStatus = entity.currentStopStatus;
            this.vehicletimeStamp = entity.vehicletimeStamp;
            arrival = entity.arrival;
            departure = entity.departure;
            this.fileTimestamp = entity.fileTimestamp;
            this.startDate = entity.startDate;
            this.directionId = entity.directionId;
        }

        public override bool Equals(object obj)
        {
            var item = obj as Entity;
            if (item == null)
            {
                return false;
            }
            bool tripIdFlag = item.tripId.Equals(this.tripId);
            bool stopSequenceFlag = item.stopSequence == this.stopSequence;
            bool currentStopStatusFlag = item.currentStopStatus.Equals(this.currentStopStatus);
            return (tripIdFlag && stopSequenceFlag && currentStopStatusFlag);
        }

        public override int GetHashCode()
        {
            return (tripId + stopSequence + currentStopStatus).GetHashCode();
        }
    }


}
