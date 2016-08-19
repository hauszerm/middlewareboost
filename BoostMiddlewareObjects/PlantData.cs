using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareObjects
{
    public class PlantData
    {
        public static PlantData CreatePlantData() {
            PlantData result = new PlantData();
            result.PlantAddress = "127.0.0.1:4711";
            result.TargetTable = 1;

            return result;
        }


        public DateTime TimestampClient { get; set; }
        public float Temp1 { get; set; }
        public float Temp2 { get; set; }

        public string PlantAddress { get; set; }
        public int MeasurementId { get; set; }
        public int Thread { get; set; }

        public string FillData { get; set; }

        public long TargetTable { get; set; }

        public List<PlantData> MeasurmentList { get; set; }

    }
}
