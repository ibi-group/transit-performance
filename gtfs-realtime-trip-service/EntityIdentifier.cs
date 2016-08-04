using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gtfs_realtime_trip_service
{
    class EntityIdentifier
    {

        string TripId;
        uint StopSequence;

        private DateTime ServiceDate;
        EventType EventType;

        

        public EntityIdentifier(string tripId, uint stopSequence, DateTime serviceDate, EventType eventType)
        {
            // TODO: Complete member initialization
            this.TripId = tripId;
            this.StopSequence = stopSequence;
            this.ServiceDate = serviceDate;
            this.EventType = eventType;
        }

        /*
         * Two entities are said to be equal if their trip id, 
         * stop sequence, service date, and event type are the same
         * */
        public override bool Equals(object obj)
        {
            var item = obj as EntityIdentifier;
            if (item == null)
                return false;

            // Null check for trip id.
            // Ideally this condition never happens. In case it happens return false
            if (String.IsNullOrEmpty(this.TripId) || String.IsNullOrEmpty(item.TripId))
                return false;

            return this.TripId.Equals(item.TripId) 
                && this.StopSequence == item.StopSequence
                && item.ServiceDate.Equals(this.ServiceDate)
                && this.EventType.Equals(item.EventType);
        }

        /*
         * This method return hashcode for a given entity.
         * An entity key is composed of trip id, stop sequence, service date, and event type
         * */
        public override int GetHashCode()
        {
            string EventTypeString = this.EventType == EventType.PRA ? "PRA" : "PRD";
            string entityKey = this.TripId + this.StopSequence + this.ServiceDate.ToShortDateString() + EventTypeString;
            return entityKey.GetHashCode();
        }

        public override string ToString()
        {
            string EventTypeString = this.EventType == EventType.PRA ? "PRA" : "PRD";
            string entityString = this.TripId + "-"+this.StopSequence +"-" +this.ServiceDate.ToShortDateString() + "-"+EventTypeString;
            return entityString;
        }
    }
}
