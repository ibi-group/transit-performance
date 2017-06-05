using System;
using System.Text;

namespace gtfsrt_events_vp_current_status
{
    internal class Event
    {
        public DateTime serviceDate;
        public string routeId;
        public string tripId;
        public string stopId;
        public uint stopSequence;
        public string vehicleId;
        public string vehicleLabel;
        public EventType eventType;
        public ulong eventTime;
        public ulong fileTimestamp;
        public uint? directionId;

        public override string ToString()
        {
            var sbr = new StringBuilder();
            sbr.Append("");
            sbr.Append(serviceDate.ToShortDateString());
            sbr.Append(" , ");
            sbr.Append(routeId);
            sbr.Append(" , ");
            sbr.Append(tripId);
            sbr.Append(" , ");
            sbr.Append(stopId);
            sbr.Append(" , ");
            sbr.Append(stopSequence);
            sbr.Append(" , ");
            sbr.Append(vehicleId);
            sbr.Append(" , ");
            sbr.Append(eventType);
            sbr.Append(" , ");
            sbr.Append(eventTime);

            sbr.Append(" , ");
            sbr.Append(fileTimestamp);

            sbr.Append(" , ");
            sbr.Append(directionId);

            return sbr.ToString();
        }

        public string ToSwtring()
        {
            var sbr = new StringBuilder();
            sbr.Append("Service Date: ");
            sbr.Append(serviceDate.ToShortDateString());
            sbr.Append(" Route ID: ");
            sbr.Append(routeId);
            sbr.Append(" Trip ID: ");
            sbr.Append(tripId);
            sbr.Append(" Stop ID: ");
            sbr.Append(stopId);
            sbr.Append(" Stop Sequence: ");
            sbr.Append(stopSequence);
            sbr.Append(" Vehicle ID: ");
            sbr.Append(vehicleId);
            sbr.Append(" Event Type: ");
            sbr.Append(eventType);
            sbr.Append(" Time: ");
            sbr.Append(eventTime);
            return sbr.ToString();
        }

        public Event(DateTime serviceDate,
                     string routeId,
                     string tripId,
                     string stopId,
                     uint stopSequence,
                     string vehicleId,
                     string vehicleLabel,
                     EventType eventType,
                     ulong actualTime,
                     ulong fileTimestamp1,
                     uint? directionId)
        {
            // TODO: Complete member initialization
            this.serviceDate = serviceDate;
            this.routeId = routeId;
            this.tripId = tripId;
            this.stopId = stopId;
            this.stopSequence = stopSequence;
            this.vehicleId = vehicleId;
            this.vehicleLabel = vehicleLabel;
            this.eventType = eventType;
            eventTime = actualTime;
            fileTimestamp = fileTimestamp1;
            this.directionId = directionId;
        }
    }

    public enum EventType
    {
        ARR = 0,
        DEP = 1,
    }
}