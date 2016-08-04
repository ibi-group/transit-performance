﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConfigUpdate
{
    [JsonConverter(typeof(TableCollectionConverter))]
    class ConfigTableCollection : List<ConfigTable>
    {
    }
}
