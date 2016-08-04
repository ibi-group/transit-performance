using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConfigUpdate
{
    class SchemaContainer
    {
        internal static SchemaContainer GetTables(string jsonString)
        {
            return JsonConvert.DeserializeObject<SchemaContainer>(jsonString);
        }

        [JsonProperty("tables")]
        public ConfigTableCollection tables
        {
            get;
            set;
        }
    }
}
