using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Server.CyclicMeasurandGeneratorConfig
{
    public class PlantConfiguration
    {
        public PlantConfiguration()
        {
            SmallestInterval = Int32.MaxValue;
        }

        public List<ClientConfiguration> ClientConfig { get; set; }

        /// <summary>
        /// Seconds
        /// </summary>
        public int SmallestInterval { get; set; }

        public int Id { get; set; }
    }
}
