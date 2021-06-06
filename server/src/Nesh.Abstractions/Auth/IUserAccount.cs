using Nesh.Abstractions.Storage.Models;
using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Auth
{
    public interface IUserAccount : IGrainWithGuidKey
    {
        Task<IReadOnlyList<Role>> GetRoles();

        Task<bool> HasRole(long role_id); 

        Task<Role> CreateRole(int realm, string resume_json);
    }
}
