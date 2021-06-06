using Nesh.Abstractions.Storage.Models;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Storage.Database
{
    public interface IRealmDB
    {
        Task<Realm> GetRealm(int realm);
    }
}
