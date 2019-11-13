using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using ProtoBuf;
using StackExchange.Redis;
using MessagePack;

namespace TestRedis
{
    public static class ProtobufHelper
    {

        public static byte[] Serialize(object val)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, val);
                return ms.ToArray();
            }
        }

        public static string SerializeToString(object val, Encoding encoding = null)
        {
            if (encoding == null)
                encoding = Encoding.Default;
            return encoding.GetString(Serialize(val));
        }


        public static T Deserialize<T>(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                return Serializer.Deserialize<T>(ms);
            }
        }



        public static T Deserialize<T>(string data, Encoding encoding = null)
        {
            if (encoding == null)
                encoding = Encoding.Default;
            return Deserialize<T>(encoding.GetBytes(data));
        }


        //public static byte[] ProtobufToBytes(this object val)
        //{
        //    using (var ms = new MemoryStream())
        //    {
        //        Serializer.Serialize(ms, val);
        //        return ms.ToArray();
        //    }
        //}

        //public static T ProtobufToObject<T>(this byte[] bytes)
        //{
        //    using (var ms = new MemoryStream(bytes))
        //    {
        //        return Serializer.Deserialize<T>(ms);
        //    }
        //}
    }

    public static class RedisExtensions
    {

        #region Json

        public static bool JsonSet(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSet(key, JsonConvert.SerializeObject(val), expiry, when, flags);
        }


        public static T JsonGet<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            return JsonConvert.DeserializeObject<T>(db.StringGet(key, flags));
        }

        public static List<T> JsonGet<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = db.StringGet(keys, flags);
            var list = new List<T>();
            foreach (string val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(JsonConvert.DeserializeObject<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static bool HashJsonSet(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSet(key, hashKey, JsonConvert.SerializeObject(val), when, flags);
        }

        public static T HashJsonGet<T>(this IDatabase db, RedisKey key, RedisValue hashKey, CommandFlags flags = CommandFlags.None)
        {
            return JsonConvert.DeserializeObject<T>(db.HashGet(key, hashKey, flags));
        }


        #endregion

        #region Protobuf

        public static bool ProtobufSet(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSet(key, ProtobufHelper.Serialize(val), expiry, when, flags);
        }

        public static T ProtobufGet<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            return ProtobufHelper.Deserialize<T>(db.StringGet(key, flags));
        }

        public static List<T> ProtobufGet<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = db.StringGet(keys, flags);
            var list = new List<T>();
            foreach (byte[] val in values)
            {
                if (val != null)
                    list.Add(ProtobufHelper.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }


        public static bool HashProtobufSet(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSet(key, hashKey, ProtobufHelper.Serialize(val), when, flags);
        }

        public static T HashProtobufGet<T>(this IDatabase db, RedisKey key, RedisValue hashKey, CommandFlags flags = CommandFlags.None)
        {
            return ProtobufHelper.Deserialize<T>(db.HashGet(key, hashKey, flags));
        }

        #endregion

        #region MessagePack

        public static bool MsgPackSet(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSet(key, MessagePackSerializer.Serialize(val), expiry, when, flags);
        }

        public static T MsgPackGet<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            return MessagePackSerializer.Deserialize<T>(db.StringGet(key, flags));
        }

        public static List<T> MsgPackGet<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = db.StringGet(keys, flags);
            var list = new List<T>();
            foreach (byte[] val in values)
            {
                if (val != null)
                    list.Add(MessagePackSerializer.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }


        public static bool HashMsgPackSet(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSet(key, hashKey, MessagePackSerializer.Serialize(val), when, flags);
        }

        public static T HashMsgPackGet<T>(this IDatabase db, RedisKey key, RedisValue hashKey, CommandFlags flags = CommandFlags.None)
        {
            return MessagePackSerializer.Deserialize<T>(db.HashGet(key, hashKey, flags));
        }

        #endregion

    }

    public static class RedisExtensionsAsync
    {

        #region Json

        public static Task<bool> JsonSetAsync(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, JsonConvert.SerializeObject(val), expiry, when, flags);
        }

        public static async Task<T> JsonGetAsync<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }

        public static async Task<List<T>> JsonGetAsync<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(JsonConvert.DeserializeObject<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashJsonSetAsync(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, JsonConvert.SerializeObject(val), when, flags);
        }

        public static async Task<T> HashJsonGetAsync<T>(this IDatabase db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }


        #endregion

        #region Protobuf

        public static Task<bool> ProtobufSetAsync(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, ProtobufHelper.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> ProtobufGetAsync<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        public static async Task<List<T>> ProtobufGetAsync<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(ProtobufHelper.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashProtobufSetAsync(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, ProtobufHelper.Serialize(val), when, flags);
        }

        public static async Task<T> HashProtobufGetAsync<T>(this IDatabase db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        #endregion

        #region MessagePack

        public static Task<bool> MsgPackSetAsync(this IDatabase db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, MessagePackSerializer.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> MsgPackGetAsync<T>(this IDatabase db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        public static async Task<List<T>> MsgPackGetAsync<T>(this IDatabase db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(MessagePackSerializer.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashMsgPackSetAsync(this IDatabase db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, MessagePackSerializer.Serialize(val), when, flags);
        }

        public static async Task<T> HashMsgPackGetAsync<T>(this IDatabase db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        #endregion

    }

    public static class RedisBatchExtensionsAsync
    {

        #region Json

        public static Task<bool> JsonSetAsync(this IBatch db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, JsonConvert.SerializeObject(val), expiry, when, flags);
        }

        public static async Task<T> JsonGetAsync<T>(this IBatch db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }

        public static async Task<List<T>> JsonGetAsync<T>(this IBatch db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(JsonConvert.DeserializeObject<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashJsonSetAsync(this IBatch db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, JsonConvert.SerializeObject(val), when, flags);
        }

        public static async Task<T> HashJsonGetAsync<T>(this IBatch db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }


        #endregion

        #region Protobuf

        public static Task<bool> ProtobufSetAsync(this IBatch db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, ProtobufHelper.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> ProtobufGetAsync<T>(this IBatch db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        public static async Task<List<T>> ProtobufGetAsync<T>(this IBatch db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(ProtobufHelper.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashProtobufSetAsync(this IBatch db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, ProtobufHelper.Serialize(val), when, flags);
        }

        public static async Task<T> HashProtobufGetAsync<T>(this IBatch db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        #endregion

        #region MessagePack

        public static Task<bool> MsgPackSetAsync(this IBatch db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, MessagePackSerializer.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> MsgPackGetAsync<T>(this IBatch db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        public static async Task<List<T>> MsgPackGetAsync<T>(this IBatch db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(MessagePackSerializer.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashMsgPackSetAsync(this IBatch db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, MessagePackSerializer.Serialize(val), when, flags);
        }

        public static async Task<T> HashMsgPackGetAsync<T>(this IBatch db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        #endregion

    }

    public static class RedisTranExtensionsAsync
    {

        #region Json

        public static Task<bool> JsonSetAsync(this ITransaction db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, JsonConvert.SerializeObject(val), expiry, when, flags);
        }

        public static async Task<T> JsonGetAsync<T>(this ITransaction db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }

        public static async Task<List<T>> JsonGetAsync<T>(this ITransaction db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(JsonConvert.DeserializeObject<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashJsonSetAsync(this ITransaction db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, JsonConvert.SerializeObject(val), when, flags);
        }

        public static async Task<T> HashJsonGetAsync<T>(this ITransaction db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(val);
        }


        #endregion

        #region Protobuf

        public static Task<bool> ProtobufSetAsync(this ITransaction db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, ProtobufHelper.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> ProtobufGetAsync<T>(this ITransaction db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        public static async Task<List<T>> ProtobufGetAsync<T>(this ITransaction db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(ProtobufHelper.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashProtobufSetAsync(this ITransaction db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, ProtobufHelper.Serialize(val), when, flags);
        }

        public static async Task<T> HashProtobufGetAsync<T>(this ITransaction db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return ProtobufHelper.Deserialize<T>(val);
        }

        #endregion

        #region MessagePack

        public static Task<bool> MsgPackSetAsync(this ITransaction db, RedisKey key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSetAsync(key, MessagePackSerializer.Serialize(val), expiry, when, flags);
        }

        public static async Task<T> MsgPackGetAsync<T>(this ITransaction db, RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.StringGetAsync(key, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        public static async Task<List<T>> MsgPackGetAsync<T>(this ITransaction db, RedisKey[] keys, CommandFlags flags = CommandFlags.None) where T : class
        {
            var values = await db.StringGetAsync(keys, flags).ConfigureAwait(false);
            var list = new List<T>();
            foreach (var val in values)
            {
                if (!string.IsNullOrEmpty(val))
                    list.Add(MessagePackSerializer.Deserialize<T>(val));
                else
                    list.Add(null);
            }
            return list;
        }

        public static Task<bool> HashMsgPackSetAsync(this ITransaction db, RedisKey key, RedisValue hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSetAsync(key, hashKey, MessagePackSerializer.Serialize(val), when, flags);
        }

        public static async Task<T> HashMsgPackGetAsync<T>(this ITransaction db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            var val = await db.HashGetAsync(key, hashKey, flags).ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<T>(val);
        }

        #endregion

    }

}
