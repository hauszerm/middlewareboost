using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoostMiddlewareImporter.Database
{
#if MongoDB
    [BsonIgnoreExtraElements]
    public class PropertyData : MongoEntity
    {
#else
    public class PropertyData
    {
        public string Id { get; set; }
#endif
    
        [BsonElement]
        public int PropertyID { get; set; }
        [BsonElement]
        public int MeasurementId { get; set; }
        [BsonElement]
        public DateTime? ClientTimestampUTC { get; set; }
        [BsonElement]
        public DateTime? ValueTimestampLocal { get; set; }
        [BsonElement]
        public DateTime? ValueTimestampUTC { get; set; }
        
        [BsonElement]
        public double? Value { get; set; }
        [BsonElement]
        public string JSON { get; set; }
        [BsonElement]
        public int? Thread { get; set; }
    }
}
