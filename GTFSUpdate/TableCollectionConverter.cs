using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GTFS
{
    public class TableCollectionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(GTFSTableCollection);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            GTFSTableCollection tableCollection = new GTFSTableCollection();
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.EndObject:
                        return tableCollection;
                    case JsonToken.PropertyName:
                        string tableName = (string)reader.Value;
                        reader.Read();
                        GTFSTable table = serializer.Deserialize<GTFSTable>(reader);
                        table.name = tableName;
                        tableCollection.Add(table);
                        break;

                }
            }
            return tableCollection;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var tableCollection = value as GTFSTableCollection;
            writer.WriteStartObject();

            foreach (GTFSTable table in tableCollection)
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
