using Nesh.WebAPI.DTOs;
using Nesh.WebAPI.Serviecs;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Storage.Models;
using Newtonsoft.Json;
using Orleans;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace Nesh.WebAPI.Controllers
{
    [Route("api/auth")]
    [ApiController]
    [Authorize]
    public class AuthController : Controller
    {
        private IClusterClient ClusterClient { get; }
        private IHttpClientFactory HttpClients { get; }
        private ILogger Logger { get; }
        private IConfiguration Configuration { get; }
        private IJWTService JWT { get; }

        public AuthController(ILogger<AuthController> logger, IHttpClientFactory http_clients, IConfiguration config, IClusterClient client, IJWTService jwt)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            HttpClients = http_clients ?? throw new ArgumentNullException(nameof(http_clients));
            Configuration = config ?? throw new ArgumentNullException(nameof(config));
            ClusterClient = client ?? throw new ArgumentNullException(nameof(client));
            JWT = jwt ?? throw new ArgumentNullException(nameof(jwt));
        }

        [HttpPost("wx_login")]
        [AllowAnonymous]
        public async Task<ActionResult> WeChatLogin([FromBody] WeChatLoginRequestDTO request)
        {
            LoginResponseDTO res = new LoginResponseDTO();
            try
            {
                string app_id = Configuration.GetSection("WeChat")["AppId"];
                string app_secrect = Configuration.GetSection("WeChat")["AppSecret"];
                string url = $"https://api.weixin.qq.com/sns/jscode2session?appid={app_id}&secret={app_secrect}&js_code={request.code}&grant_type=authorization_code";

                var client = HttpClients.CreateClient();
                var json = await client.GetStringAsync(url);
                dynamic response = JsonConvert.DeserializeObject<dynamic>(json);

                if (response.errcode == 0)
                {
                    string unionid = (string)response.unionid;

                    IPlatformSession session = ClusterClient.GetGrain<IPlatformSession>(unionid);
                    Account account = await session.VerifyAccount(Platform.WeChat, unionid);
                    if (account == null)
                    {
                        throw new Exception($"VerifyAccount cant found {unionid}");
                    }

                    string access_token = JWT.GetAccessToken(account);
                    await session.RefreshToken(access_token);

                    res.data = access_token;
                    res.result = LoginResult.Success;
                }
                else
                {
                    res.result = LoginResult.WeChatLoginFaild;
                    res.data = (string)response.errmsg;
                }
            }
            catch (Exception ex)
            {
                res.result = LoginResult.WeChatLoginFaild;
                res.data = ex.ToString();
                Logger.LogError(ex.ToString());
            }

            return Json(res);
        }

        [HttpPost("sim_login")]
        [AllowAnonymous]
        public async Task<ActionResult> SimLogin([FromBody] SimLoginRequestDTO request)
        {
            LoginResponseDTO res = new LoginResponseDTO();
            try
            {
                IPlatformSession session = ClusterClient.GetGrain<IPlatformSession>(request.user_name);
                Account account = await session.VerifyAccount(Platform.Sim, request.user_name);
                if (account == null)
                {
                    throw new Exception($"VerifyAccount cant found {request.user_name}");
                }

                string access_token = JWT.GetAccessToken(account);
                await session.RefreshToken(access_token);

                res.data = access_token;
                res.result = LoginResult.Success;
            }
            catch (Exception ex)
            {
                res.result = LoginResult.None;
                res.data = ex.ToString();
                Logger.LogError(ex.ToString());
            }

            return Json(res);
        }

        [HttpGet("test_auth")]
        public Task<string> TestAuth()
        {
            return Task.FromResult("hello");
        }
    }
}
