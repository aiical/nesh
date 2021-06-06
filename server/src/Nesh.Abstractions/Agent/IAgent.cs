using Nesh.Abstractions.Data;
using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Agent
{
    public interface IAgent : IGrainWithIntegerKey
    {
        Task OnResponse(int message_id, NList message);

        Task SendMessage(NList message);

        Task Online();

        Task Offline();
    }

    public interface IAgentObserver : IAsyncObserver<NList>
    {
    }
}
