package com.cn.tz13.bigdata.redis.client;

import com.cn.tz13.bigdata.config.ConfigUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.util.Properties;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-18:32
 */
public class JedisUtil {
    private static final Logger LOG = Logger.getLogger(JedisUtil.class);
    private static Properties redisConf;

    private static final String redisConfPath = "redis/redis.properties";

    static {
        //读取配置文件
        redisConf =ConfigUtil.getInstance().getProperties(redisConfPath);
    }
    public static Jedis getJedis(int db){
        Jedis jedis =JedisUtil.getJedis();
        if (jedis != null){
            jedis.select(db);
        }
        return jedis;
    }
    public static void close(Jedis jedis){
        if(jedis!=null){
            jedis.close();
        }
    }

    /**
     * 并发很高的时候 会出现获取连接失败的情况  导致程序挂掉
     * @return
     */
    public static Jedis getJedis(){
        int timeoutCount = 0;
        // 如果是网络超时则多试几次
        while (true){
            try {
                Jedis jedis = new Jedis(redisConf.get("redis.hostname").toString(),
                        Integer.valueOf(redisConf.get("redis.port").toString()));
                return jedis;
            }catch (Exception e){
                if (e instanceof JedisConnectionException
                        || e instanceof SocketTimeoutException){
                    timeoutCount++;
                    LOG.warn("获取jedis连接超时次数:" +timeoutCount);
                    if (timeoutCount > 4){
                        LOG.error("获取jedis连接超时次数a:" +timeoutCount);
                        LOG.error(null,e);
                        break;
                    }
                }else {
                    LOG.error("getJedis error", e);
                    break;
                }
            }
        }
        return null;
    }
    /**
     * redis  hash结构
     * @param args
     */
    public static void main(String[] args) {
        Jedis jedis = JedisUtil.getJedis(10);
        //jedis.hset("112","name","张三");
        jedis.hset("112","age","6");
        JedisUtil.close(jedis);
    }
}
