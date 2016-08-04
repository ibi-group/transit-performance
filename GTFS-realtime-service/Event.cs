using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    class Event
    {
        
         public DateTime serviceDate;
         public String routeId;
         public String tripId;
         public String stopId;
         public uint stopSequence;
         public String vehicleId;
         public String vehicleLabel;
         public EventType eventType;
         public ulong eventTime;
         public ulong fileTimestamp;
         public uint? directionId;

      override public String ToString()
      {
          StringBuilder sbr = new StringBuilder();
          sbr.Append("");
          sbr.Append(this.serviceDate.ToShortDateString());
          sbr.Append(" , ");
          sbr.Append(this.routeId);
          sbr.Append(" , ");
          sbr.Append(this.tripId);
          sbr.Append(" , ");
          sbr.Append(this.stopId);
          sbr.Append(" , ");
          sbr.Append(this.stopSequence);
          sbr.Append(" , ");
          sbr.Append(this.vehicleId);
          sbr.Append(" , ");
          sbr.Append(this.eventType.ToString());
          sbr.Append(" , ");
          sbr.Append(eventTime);

          sbr.Append(" , ");
          sbr.Append(fileTimestamp);

          sbr.Append(" , ");
          sbr.Append(this.directionId);

          return sbr.ToString();
      }


        public String    ToSwtring()
       {
           StringBuilder sbr = new StringBuilder();
           sbr.Append("Service Date: ");
           sbr.Append(this.serviceDate.ToShortDateString());
           sbr.Append(" Route ID: ");
           sbr.Append(this.routeId);
           sbr.Append(" Trip ID: ");
           sbr.Append(this.tripId);
           sbr.Append(" Stop ID: ");
           sbr.Append(this.stopId);
           sbr.Append(" Stop Sequence: ");
           sbr.Append(this.stopSequence);
           sbr.Append(" Vehicle ID: ");
           sbr.Append(this.vehicleId);
           sbr.Append(" Event Type: ");
           sbr.Append(this.eventType.ToString());
           sbr.Append(" Time: ");
           sbr.Append(eventTime);
 	        return sbr.ToString();
       }

        public Event(DateTime serviceDate, string routeId, string tripId, string stopId, uint stopSequence, string vehicleId, string vehicleLabel, EventType eventType, ulong actualTime, ulong fileTimestamp1, uint? directionId)
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
           this.eventTime = actualTime;
           this.fileTimestamp = fileTimestamp1;
           this.directionId = directionId;
       }


    }

        public enum EventType
        {
          ARR = 0,
          DEP = 1,
        }
}
