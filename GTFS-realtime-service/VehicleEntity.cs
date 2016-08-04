using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS_realtime_service
{
    class VehicleEntity
    {
        public String VehicleId;
        public String VehicleLabel;
        public String tripId;        

        public override bool Equals(object obj)
        {
            var item = obj as VehicleEntity;
            if (item == null)
            {
                return false;
            }
        
            return (this.VehicleId.Equals(item.VehicleId) && this.tripId.Equals(item.tripId));
        }

        public override int GetHashCode()
        {
            return (VehicleId+tripId).GetHashCode();
        }
    }
}
