using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using ProtoBuf;
using StackExchange.Redis;

namespace TestRedis
{
    public static class RedisExtensions
    {

        #region Json

        public static bool JsonSet(this IDatabase db, string key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSet(key, JsonConvert.SerializeObject(val), expiry, when, flags);
        }

        public static bool JsonSet(this IDatabase db, KeyValuePair<string, object>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var vs = new KeyValuePair<RedisKey, RedisValue>[values.Length];
            for (var i = 0; i < values.Length; i++)
            {
                var item = values[i];
                vs[i] = new KeyValuePair<RedisKey, RedisValue>(item.Key, JsonConvert.SerializeObject(item.Value));
            }
            return db.StringSet(vs, when, flags);
        }

        public static T JsonGet<T>(this IDatabase db, string key, CommandFlags flags = CommandFlags.None)
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

        public static bool HashJsonSet(this IDatabase db, string key, string hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSet(key, hashKey, JsonConvert.SerializeObject(val), when, flags);
        }

        public static T HashJsonGet<T>(this IDatabase db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            return JsonConvert.DeserializeObject<T>(db.HashGet(key, hashKey, flags));
        }


        #endregion

        #region Protobuf

        public static bool ProtobufSet(this IDatabase db, string key, object val, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.StringSet(key, ProtobufHelper.Serialize(val), expiry, when, flags);
        }

        public static bool ProtobufSet(this IDatabase db, KeyValuePair<string, object>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            var vs = new KeyValuePair<RedisKey, RedisValue>[values.Length];
            for (var i = 0; i < values.Length; i++)
            {
                var item = values[i];
                vs[i] = new KeyValuePair<RedisKey, RedisValue>(item.Key, ProtobufHelper.Serialize(item.Value));
            }
            return db.StringSet(vs, when, flags);
        }

        public static T ProtobufGet<T>(this IDatabase db, string key, CommandFlags flags = CommandFlags.None)
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


        public static bool HashProtobufSet(this IDatabase db, string key, string hashKey, object val, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            return db.HashSet(key, hashKey, ProtobufHelper.Serialize(val), when, flags);
        }

        public static T HashProtobufGet<T>(this IDatabase db, string key, string hashKey, CommandFlags flags = CommandFlags.None)
        {
            return ProtobufHelper.Deserialize<T>(db.HashGet(key, hashKey, flags));
        }

        #endregion

    }

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

}
