using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConfigUpdate
{
    class ColumnSetConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ConfigColumnSet);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            ConfigColumnSet columnSet = new ConfigColumnSet();
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.EndObject:
                        return columnSet;
                    case JsonToken.PropertyName:
                        string columnName = (string)reader.Value;
                        reader.Read();
                        ConfigColumn column = serializer.Deserialize<ConfigColumn>(reader);
                        column.name = columnName;
                        columnSet.Add(column);
                        break;
                }
            }
            return columnSet;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var columnSet = value as ConfigColumnSet;
            writer.WriteStartObject();

            foreach (ConfigColumn column in columnSet)
            {
                writer.WritePropertyName(column.name);
                writer.WriteStartObject();

                if (!column.required)
                {
                    writer.WritePropertyName("required");
                    writer.WriteValue(column.required);
                }
                if (!column.allowNull)
                {
                    writer.WritePropertyName("null");
                    writer.WriteValue(column.allowNull);
                }
                if (!column.create)
                {
                    writer.WritePropertyName("create");
                    writer.WriteValue(column.create);
                }
                if (column.index)
                {
                    writer.WritePropertyName("index");
                    writer.WriteValue(column.index);
                }
                if (column.primaryKey)
                {
                    writer.WritePropertyName("primaryKey");
                    writer.WriteValue(column.primaryKey);
                }

                writer.WritePropertyName("type");
                writer.WriteValue(column.type);
                writer.WriteEndObject();
            }
            writer.WriteEndObject();
        }
    }
}
