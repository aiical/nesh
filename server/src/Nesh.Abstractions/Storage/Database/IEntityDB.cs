using Nesh.Abstractions.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Storage.Database
{
    public interface IEntityDB
    {
        Task<bool> IsPersist(long origin);

        Task<NodeType> GetNodeType(long origin);

        Task SetNodeType(long origin, NodeType node_type);

        Task<IReadOnlyList<Entity>> GetEntities(long origin);
    }
}
