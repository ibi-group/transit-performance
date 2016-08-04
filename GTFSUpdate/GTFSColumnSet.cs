using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GTFS
{
    [JsonConverter(typeof(ColumnSetConverter))]
    class GTFSColumnSet : List<GTFSColumn>
    {

    }
}

