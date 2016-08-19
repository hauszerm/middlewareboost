using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects
{
    public class Measurand
    {
        public const string TIMESTAMP_NAME = "cts";
        public const string VALUE_NAME = "v";
        public const string PLANT_NAME = "p";
        public const string EQUIPMENT_NAME = "e";

        [JsonConverter(typeof(IsoDateTimeConverter))]
        [JsonProperty(PropertyName = TIMESTAMP_NAME)]
        public DateTime Timestamp { get; set; }
        
        
        public string Name { get; set; }

        [JsonProperty(PropertyName = VALUE_NAME)]
        public double Value { get; set; }

        [JsonProperty(PropertyName = PLANT_NAME)]
        public string Plant { get; set; }

        [JsonProperty(PropertyName = EQUIPMENT_NAME)]
        public string Equipment { get; set; }

        public static void UpdateMeasurand(Measurand source, Measurand target)
        {
            target.Name = source.Name;
            target.Value = source.Value;

            target.Plant = source.Plant;
            target.Equipment = source.Equipment;
        }

        public void NewTimestamp()
        {
            Timestamp = DateTime.Now;
        }

        public void RandomizeValue(Random rnd)
        {
            double val;

            val = this.Value;
            this.Value = (val + rnd.Next(1, 10));

            //if (Double.TryParse(this.Value, out val)) {
            //    this.Value = (val + rnd.Next(1, 10)).ToString();
            //}
        }
    }
}
