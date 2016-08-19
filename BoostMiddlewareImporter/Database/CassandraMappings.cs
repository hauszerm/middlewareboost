using Cassandra.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Database
{
    public class CassandraMappings : Mappings
    {
        public CassandraMappings()
        {
            // Define mappings in the constructor of your class
            // that inherits from Mappings
            For<PropertyData>()
                .TableName("propertyData")
                .PartitionKey(u => u.PropertyID, u => u.MeasurementId, u => u.ClientTimestampUTC)
                .Column(u => u.ClientTimestampUTC)
                .Column(u => u.Id)
                .Column(u => u.JSON)
                .Column(u => u.PropertyID)
                .Column(u => u.Thread)
                .Column(u => u.Value)
                .Column(u => u.ValueTimestampLocal)
                .Column(u => u.ValueTimestampUTC)
                ;
        }
    }
}
