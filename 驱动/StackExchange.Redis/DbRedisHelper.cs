using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using StackExchange.Redis;

namespace Common.DbUtils
{
    public static class DbRedisHelperExt
    {
        public static IEnumerable<RedisKey> GetAllKeys(this IDatabase db, RedisValue pattern = default(RedisValue))
        {
            return DbRedisHelper.GetAllKeys(db.Database, pattern);
        }
    }

    public class DbRedisHelper
    {
        public static string ConnectionString;

        public static string Ip;
        public static int Port;

        private static readonly object _locker = new object();
        private static ConnectionMultiplexer _redis;


        public static ConnectionMultiplexer GetConn()
        {
            if (_redis == null || !_redis.IsConnected)
            {
                lock (_locker)
                {
                    if (_redis != null)
                        return _redis;
                    _redis = ConnectionMultiplexer.Connect(ConnectionString); ;
                    return _redis;
                }
            }

            return _redis;
        }

        public static IEnumerable<RedisKey> GetAllKeys(int db = 0, RedisValue pattern = default(RedisValue))
        {
            var conn = GetConn();
            var endPoints = conn.GetEndPoints();
            var server = conn.GetServer(endPoints[0]);
            return server.Keys(db, pattern);
        }


        public static IDatabase GetDb(int db = 0, object asyncState = null)
        {
            return GetConn().GetDatabase(db, asyncState);
        }


        public static void CheckRedis()
        {
            while (true)
            {
                try
                {
                    GetDb().StringGet("ashdjasdkjasdhka");
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(3000);
                }
            }
        }


    }
}
