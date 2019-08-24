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

//事务
//using (IRedisClient RClient = prcm.GetClient())
//{
//    RClient.Add("key",1);
//    using (IRedisTransaction IRT = RClient.CreateTransaction())
//    {
//        IRT.QueueCommand(r => r.Set("key", 20));
//        IRT.QueueCommand(r => r.Increment("key",1)); 

//        IRT.Commit(); // 提交事务
//    }
//    Response.Write(RClient.Get<string>("key"));
//}

//并发锁
//using (IRedisClient RClient = prcm.GetClient())
//{
//    RClient.Add("mykey",1);
//    // 支持IRedisTypedClient和IRedisClient
//    using (RClient.AcquireLock("testlock")) 
//    {
//        Response.Write("申请并发锁<br/>");
//        var counter = RClient.Get<int>("mykey");

//        Thread.Sleep(100);

//        RClient.Set("mykey", counter + 1);
//        Response.Write(RClient.Get<int>("mykey"));
//    }
//}


//发布订阅
//using (var conn = RedisHelper.prcm.GetClient())
//{
//    conn.Db = 2;

//    //创建订阅
//    IRedisSubscription subscription = conn.CreateSubscription();
//    //接受到消息时
//    subscription.OnMessage = (channel, msg) =>
//    {
//        Console.WriteLine($"从频道：{channel}上接受到消息：{msg},时间：{DateTime.Now.ToString("yyyyMMdd HH:mm:ss")}");
//        Console.WriteLine($"频道订阅数目：{subscription.SubscriptionCount}");
//        Console.WriteLine("___________________________________________________________________");
//    };
//    //订阅频道时
//    subscription.OnSubscribe = (channel) =>
//    {
//        Console.WriteLine("订阅客户端：开始订阅" + channel);
//    };
//    //取消订阅频道时
//    subscription.OnUnSubscribe = (a) => { Console.WriteLine("订阅客户端：取消订阅"); };

//    //订阅频道
//    subscription.SubscribeToChannels("channel1");

//    //发布
//    conn.PublishMessage("channel1", "你好");

//}