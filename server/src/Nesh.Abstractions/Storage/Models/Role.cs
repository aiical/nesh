using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;
using System;

namespace Nesh.Abstractions.Storage.Models
{
    public class Role
    {
        [BsonId]
        public long origin_id { get; set; }

        [JsonIgnore]
        public Guid user_id { get; set; }

        public int realm_id { get; set; }

        public string resume_json { get; set; }

        [JsonIgnore]
        public DateTime create_time { get; set; }
    }
}
