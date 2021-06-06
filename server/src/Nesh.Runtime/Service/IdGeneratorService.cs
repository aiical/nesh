using IdGen;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;

namespace Nesh.Runtime.Service
{
    public static class IdGeneratorExtension
    {
        /// <summary>
        /// 配置默认配置
        /// </summary>
        public static IServiceCollection AddIdGenerator(this IServiceCollection services, Action<IdGeneratorOption> configure)
        {
            services.Configure(configure);
            services.TryAddSingleton<IIdGeneratorService, IdGeneratorService>();
            return services;
        }
    }

    public class IdGeneratorOption
    {
        public int AppId { get; set; }

        public IdGeneratorOptions GeneratorOptions { get; set; }
    }

    public interface IIdGeneratorService
    {
        long NewIdentity();
    }

    public class IdGeneratorService : IIdGeneratorService
    {
        private readonly IdGenerator _generator;
        private readonly IdGeneratorOption _option;
        private readonly ILogger<IdGeneratorService> _logger;

        public IdGeneratorService(IOptions<IdGeneratorOption> option, ILogger<IdGeneratorService> logger)
        {
            _option = option.Value ?? throw new ArgumentNullException(nameof(IdGeneratorOption));
            _logger = logger ?? throw new ArgumentNullException(nameof(ILogger<IdGeneratorService>));

            // Create an IdGenerator with it's generator-id set to 0, our custom epoch 
            // and id-structure
            _generator = new IdGenerator(_option.AppId, _option.GeneratorOptions);
        }

        public long NewIdentity()
        {
            if (_generator == null)
            {
                throw new Exception("_generator is not init.");
            }

            long id = _generator.CreateId();
            return id;
        }
    }
}
