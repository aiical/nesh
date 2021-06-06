using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Nesh.Abstractions.Storage.Models
{
    public enum Platform
    {
        Sim = 1,
        WeChat = 2,
    }

    public class Account
    {
        [BsonId]
        public Guid user_id { get; set; }

        public string hash_slat { get; set; }

        public Platform platform { get; set; }

        public string platform_id { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime create_time { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime expired_time { get; set; }

        public string access_token { get; set; }
    }
}
