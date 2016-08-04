using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConfigUpdate
{
    public class TableCollectionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ConfigTableCollection);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            ConfigTableCollection tableCollection = new ConfigTableCollection();
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.EndObject:
                        return tableCollection;
                    case JsonToken.PropertyName:
                        string tableName = (string)reader.Value;
                        reader.Read();
                        ConfigTable table = serializer.Deserialize<ConfigTable>(reader);
                        table.name = tableName;
                        tableCollection.Add(table);
                        break;

                }
            }
            return tableCollection;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var tableCollection = value as ConfigTableCollection;
            writer.WriteStartObject();

            foreach (ConfigTable table in tableCollection)
            {
                writer.WritePropertyName(table.name);
                writer.WriteStartObject();
                writer.WritePropertyName("columns");
                serializer.Serialize(writer, table.columns);
                if (!table.required)
                {
                    writer.WritePropertyName("required");
                    writer.WriteValue(table.required);
                }
                writer.WriteEndObject();
            }
            writer.WriteEndObject();
        }
    }
}
