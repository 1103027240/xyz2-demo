package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST, Constant.REDIS_PORT, 10000);
    }

    //获取Jedis
    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    //关闭Jedis
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    //获取异步操作Redis
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        return RedisClient.create("redis://127.0.0.1:6379/0").connect();
    }

    //关闭异步操作Redis
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            asyncRedisConn.close();
        }
    }

    //以异步方式从Redis获取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if (StrUtil.isNotBlank(dimJsonStr)) {
                return JSON.parseObject(dimJsonStr);
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //以异步方式向Redis插入数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id, JSONObject dimJsonObj) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        asyncCommands.setex(key, Constant.DIM_REDIS_EXPIRE, dimJsonObj.toJSONString());
    }

    //从Redis获取数据
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String dimJsonStr = jedis.get(key);
        if (StrUtil.isNotBlank(dimJsonStr)) {
            return JSON.parseObject(dimJsonStr);
        }
        return null;
    }

    //向Redis插入数据
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        String key = getKey(tableName, id);
        jedis.setex(key, Constant.DIM_REDIS_EXPIRE, dimJsonObj.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        return String.format("%s:%s", tableName, id);
    }

}
