using Newtonsoft.Json;

namespace ConfigUpdate
{
    internal class ConfigTable
    {
        public ConfigTable()
        {
            required = true;
        }

        [JsonProperty("columns", NullValueHandling = NullValueHandling.Ignore)]
        public ConfigColumnSet columns { get; set; }

        [JsonProperty("required", NullValueHandling = NullValueHandling.Ignore)]
        public bool required { get; set; }

        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        public string name { get; set; }
    }
}
