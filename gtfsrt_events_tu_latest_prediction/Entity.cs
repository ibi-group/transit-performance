using System;

namespace gtfsrt_events_tu_latest_prediction
{
    /*
     * Entity objects are used to compare, when an 
     * Event will be generated.
     * 
     * So this class will look very similar to Event class.
     */

    internal class Entity
    {
        internal DateTime ServiceDate { get; set; }
        internal string RouteId;
        internal string TripId;
        internal string StopId;
        internal uint StopSequence;
        internal string VehicleId;
        internal EventType _EventType;
        internal long EventTime;
        internal ulong FileTimestamp;
        internal uint? DirectionId;


        internal Entity()
        {
            // Default Constructor
        }

        /*
         * Parameterized constructor
         * */

        internal Entity
            (
            DateTime serviceDate,
            string routeId,
            string tripId,
            string stopId,
            uint stopSequence,
            string vehicleId,
            EventType eventType,
            long eventTime,
            ulong fileTimestamp,
            uint? directionId
            )
        {
            ServiceDate = serviceDate;
            RouteId = routeId;
            TripId = tripId;
            StopId = stopId;
            StopSequence = stopSequence;
            VehicleId = vehicleId;
            _EventType = eventType;
            EventTime = eventTime;
            FileTimestamp = fileTimestamp;
            DirectionId = directionId;

        }

        /*
         * Two entities are said to equal if their trip ids and 
         * stop sequence are same.
         * */

        public override bool Equals(object obj)
        {
            var item = obj as Entity;
            if (item == null)
                return false;

            // Null check for trip id.
            // Ideally this condition never happens. In case it happens return false
            if (string.IsNullOrEmpty(TripId) || string.IsNullOrEmpty(item.TripId))
                return false;

            return TripId.Equals(item.TripId) && StopSequence == item.StopSequence;
        }

        /**
         * Return event type
         */

        internal EventType GetEventType()
        {
            return _EventType;
        }

        /*
         * This method return hashcode for a given entity.
         * An entity key is composed of trip id and stop sequence.
         * */

        public override int GetHashCode()
        {
            var entityKey = TripId + StopSequence;
            return entityKey.GetHashCode();
        }
    }
}
