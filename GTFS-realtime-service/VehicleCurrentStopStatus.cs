using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    public enum VehicleCurrentStopStatus
    {
        INCOMING_AT = 0,
        STOPPED_AT = 1,
        IN_TRANSIT_TO = 2,
    }
}
