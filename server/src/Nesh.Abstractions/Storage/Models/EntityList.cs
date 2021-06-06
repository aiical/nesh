using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Nesh.Abstractions.Storage.Models
{
    public class EntityList
    {
        [BsonId]
        public long origin { get; set; }

        public NodeType node { get; set; }

        public List<EntityChild> entities { get; set; }
    }

    public class EntityChild
    {
        [BsonId]
        public long unique { get; set; }

        public string type { get; set; }

        public BsonDocument entity { get; set; }
    }
}
