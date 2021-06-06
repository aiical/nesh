using MongoDB.Bson.Serialization.Attributes;

namespace Nesh.Abstractions.Storage.Models
{
    public class Realm
    {
        [BsonId]
        public int id { get; set; }

        public string name { get; set; }

        //public DateTime create_time { get; set; }
    }
}
