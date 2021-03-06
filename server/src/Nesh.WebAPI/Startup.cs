using Nesh.WebAPI.Serviecs;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.IdentityModel.Tokens;
using MongoDB.Driver;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Nesh.WebAPI
{
    public class Startup
    {
        public Startup(IConfiguration configuration, IWebHostEnvironment env)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            Environment = env ?? throw new ArgumentNullException(nameof(env));
        }

        public IConfiguration Configuration { get; }

        public IWebHostEnvironment Environment { get; set; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton(Environment);
            services.AddSingleton(Configuration);

            services.AddSingleton<OrleansService>();
            services.AddSingleton<IHostedService>(_ => _.GetService<OrleansService>());
            services.AddSingleton(_ => _.GetService<OrleansService>().Client);

            services.AddHttpClient();

            services.AddControllers().AddNewtonsoftJson();

            services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();

            services.Configure<ConsoleLifetimeOptions>(options =>
            {
                options.SuppressStatusMessages = true;
            });

            services.AddScoped<IJWTService, JWTService>();
            services.Configure<JWTConfig>(Configuration.GetSection("JWT"));
            JWTConfig jwtConfig = new JWTConfig();
            Configuration.GetSection("JWT").Bind(jwtConfig);

            services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();
            services.AddControllers().AddNewtonsoftJson();
            services.AddHttpClient();

            services.AddAuthentication(x =>
            {
                x.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                x.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            })
            .AddJwtBearer(options =>
            {
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    RequireSignedTokens = true,

                    // The signing key must match!
                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = new SymmetricSecurityKey(Encoding.ASCII.GetBytes(jwtConfig.SecurityKey)),

                    // Validate the JWT Issuer (iss) claim
                    ValidateIssuer = false,

                    // Validate the JWT Audience (aud) claim
                    ValidateAudience = false,

                    // Validate the token expiry
                    //ValidateLifetime = true,

                    // If you want to allow a certain amount of clock drift, set that here
                    //ClockSkew = TimeSpan.Zero,
                    /***********************************TokenValidationParameters????????????***********************************/
                    // RequireSignedTokens = true,
                    // SaveSigninToken = false,
                    // ValidateActor = false,
                    // ????????????????????false????????????Issuer??Audience????????????????????
                    // ValidateAudience = true,
                    // ValidateIssuer = true, 
                    // ValidateIssuerSigningKey = false,
                    // ????????Token??Claims??????????Expires
                    // RequireExpirationTime = true,
                    // ??????????????????????
                    // ClockSkew = TimeSpan.FromSeconds(300),
                    // ????????Token??????????????????????Token??Claims????NotBefore??Expires????
                    // ValidateLifetime = true
                };
                options.Events = new JwtBearerEvents
                {
                    OnMessageReceived = (context) => {
                        if (!context.HttpContext.Request.Path.HasValue)
                        {
                            return Task.CompletedTask;
                        }

                        var accessToken = context.HttpContext.Request.Query["access_token"];
                        var path = context.HttpContext.Request.Path;
                        if (!(string.IsNullOrWhiteSpace(accessToken)))
                        {
                            context.Token = accessToken;
                            return Task.CompletedTask;
                        }

                        return Task.CompletedTask;
                    }
                };
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHttpsRedirection();

            app.UseRouting();
            app.UseCors();

            app.UseAuthentication();
            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
