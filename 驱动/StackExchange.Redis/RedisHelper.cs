using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Test
{
    public class RedisHelper
    {
        private static object _locker = new object();
        private static ConnectionMultiplexer _redis;
        public static ConnectionMultiplexer GetConn()
        {
            if (_redis == null || !_redis.IsConnected)
            {
                lock (_locker)
                {
                    if (_redis != null) return _redis;

                    _redis = ConnectionMultiplexer.Connect("localhost,resolvedns=1"); ;
                    return _redis;
                }
            }

            return _redis;
        }

        public static IDatabase GetDb(int db = -1, object asyncState = null)
        {
            return GetConn().GetDatabase(db, asyncState);
        }
    }
}