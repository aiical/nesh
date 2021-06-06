using Orleans;
using System;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Auth
{
    public interface IAccessToken : IGrainWithStringKey
    {
        Task<Guid> GetUserId();

        Task<bool> IsExpired();
    }
}
