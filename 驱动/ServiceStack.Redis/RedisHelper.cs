using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public class RedisHelper
    {
        public static PooledRedisClientManager prcm = new PooledRedisClientManager("localhost"); 
    }
}



//localhost
//127.0.0.1:6379
//redis://localhost:6379
//password @localhost:6379
//clientid:password @localhost:6379
//redis://clientid:password@localhost:6380?ssl=true&db=1


//using (var conn = RedisHelper.prcm.GetClient())
//{
                
//}