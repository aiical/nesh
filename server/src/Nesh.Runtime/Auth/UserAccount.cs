using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Storage.Models;
using Nesh.Abstractions.Utils;
using Nesh.Runtime.Service;
using Nesh.Runtime.Utils;
using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Auth
{
    public class UserAccount : Grain, IUserAccount
    {
        private Guid _UserId { get; set; }
        private ILogger _Logger { get; set; }
        private IIdGeneratorService IdGenerator { get; set; }
        private IMongoCollection<Role> RoleCollection { get; set; }
        private IMongoCollection<Realm> RealmCollection { get; set; }

        public override Task OnActivateAsync()
        {
            _UserId = this.GetPrimaryKey();
            _Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger($"{nameof(UserAccount)}[" + _UserId + "]");
            IdGenerator = ServiceProvider.GetService<IIdGeneratorService>();
            IMongoClient MongoClient = ServiceProvider.GetService<IMongoClient>();

            IMongoDatabase Database = MongoClient.GetDatabase(PersistUtils.AUTH_DB);
            RoleCollection = Database.GetCollection<Role>(PersistUtils.ROLE_COLLECTION);
            RealmCollection = Database.GetCollection<Realm>(PersistUtils.REALM_COLLECTION);

            return base.OnActivateAsync();
        }

        public async Task<IReadOnlyList<Role>> GetRoles()
        {
            try
            {
                FilterDefinitionBuilder<Role> builder = Builders<Role>.Filter;
                FilterDefinition<Role> filter = builder.Eq(nameof(Role.user_id), _UserId);
                IAsyncCursor<Role> result = await RoleCollection.FindAsync(filter);

                List<Role> roles = await result.ToListAsync();
                if (roles == null)
                {
                    return new List<Role>();
                }

                return roles;
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "GetRoles Failed!");
            }

            return null;
        }

        public async Task<Role> CreateRole(int realm, string resume_json)
        {
            try
            {
                FilterDefinitionBuilder<Realm> builder = Builders<Realm>.Filter;
                FilterDefinition<Realm> filter = builder.Eq(nameof(Realm.id), realm);
                IAsyncCursor<Realm> result = await RealmCollection.FindAsync(filter);
                if (!(await result.AnyAsync()))
                {
                    _Logger.LogError($"realm {realm} is not exist, cant create role");
                    return null;
                }

                Role role = new Role();
                role.origin_id = IdGenerator.NewIdentity();
                role.user_id = _UserId;
                role.realm_id = realm;
                role.create_time = TimeUtils.Now;
                role.resume_json = resume_json;

                await RoleCollection.InsertOneAsync(role);

                return role;
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, $"CreateRole Failed! {realm} {resume_json}");
            }

            return null;
        }

        public async Task<bool> HasRole(long role_id)
        {
            try
            {
                FilterDefinitionBuilder<Role> builder = Builders<Role>.Filter;
                FilterDefinition<Role> filter = builder.Eq(nameof(Role.origin_id), role_id);
                IAsyncCursor<Role> result = await RoleCollection.FindAsync(filter);

                Role role = await result.FirstOrDefaultAsync();

                return role != null;
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, $"HasRole Failed! {role_id}");
            }

            return false;
        }
    }
}