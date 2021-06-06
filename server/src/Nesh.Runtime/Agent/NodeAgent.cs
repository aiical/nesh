using Microsoft.Extensions.DependencyInjection;
using Nesh.Abstractions.Agent;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Storage.Database;
using Nesh.Runtime.Service;
using Orleans;
using System.Threading.Tasks;

namespace Nesh.Runtime.Agent
{
    public abstract class NodeAgent : Grain, IAgent
    {
        protected IEntityDB EntityDB { get; set; }

        protected IIdGeneratorService IdGenerator { get; set; }

        public override async Task OnActivateAsync()
        {
            EntityDB = ServiceProvider.GetService<IEntityDB>();

            IdGenerator = ServiceProvider.GetService<IIdGeneratorService>();

            await base.OnActivateAsync();
        }

        public abstract Task OnResponse(int message_id, NList message);

        public abstract Task SendMessage(NList message);

        public abstract Task Online();

        public abstract Task Offline();
    }
}
