using System;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Agent
{
    public interface IRoleAgent : IAgent
    {
        Task BindSession(Guid user_id, string stream);

        Task SendEntities();
    }
}
