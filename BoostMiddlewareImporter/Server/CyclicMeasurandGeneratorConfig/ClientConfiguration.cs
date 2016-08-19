using BoostMiddlewareObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server.CyclicMeasurandGeneratorConfig
{
    public class ClientConfiguration
    {
        public ClientConfiguration()
        {
            NextCycleStartUTC = DateTime.UtcNow;
        }

        public int cycle { get; set; }
        public int count { get; set; }
        public List<Measurand> measurands { get; set; }

        public DateTime NextCycleStartUTC { get; set; }
    }
}