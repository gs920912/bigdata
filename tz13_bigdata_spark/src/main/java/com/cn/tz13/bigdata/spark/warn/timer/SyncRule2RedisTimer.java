package com.cn.tz13.bigdata.spark.warn.timer;

import com.cn.tz13.bigdata.redis.client.JedisUtil;
import com.cn.tz13.bigdata.spark.warn.domain.TZ_RuleDomain;
import com.cn.tz13.bigdata.spark.warn.dao.TZ_ruleDao;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-19:46
 */
public class SyncRule2RedisTimer extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRule2RedisTimer.class);
    @Override
    public void run() {
        System.out.println("定时任务执行");
        //将mysql中的规则同步到redis
        //1.获取所有规则
        List<TZ_RuleDomain> ruleList = TZ_ruleDao.getRuleList();
        //2.遍历所有规则，循环放入REDIS
        Jedis jedis = null;
        try {
            for (int i = 0; i < ruleList.size(); i++){
                jedis = JedisUtil.getJedis(15);
                TZ_RuleDomain tz_ruleDomain = ruleList.get(i);
                String id = tz_ruleDomain.getId()+"";
                String publisher = tz_ruleDomain.getPublisher();
                String warn_fieldname = tz_ruleDomain.getWarn_fieldname();
                String warn_fieldvalue = tz_ruleDomain.getWarn_fieldvalue();
                //18688888888
                //18688888888
                String send_mobile = tz_ruleDomain.getSend_mobile();
                String send_type = tz_ruleDomain.getSend_type();
                //使用hash结构存入redis  使用告警字段名+":" + 字段内容作为key
                String redisKey = warn_fieldname +":"+ warn_fieldvalue;
                jedis.hset(redisKey,"id",
                        StringUtils.isNoneBlank(id)?id:"");
                jedis.hset(redisKey,"publisher",
                        StringUtils.isNoneBlank(publisher)?publisher:"");
                jedis.hset(redisKey,"warn_fieldname",
                        StringUtils.isNoneBlank(warn_fieldname)?warn_fieldname:"");
                jedis.hset(redisKey,"warn_fieldvalue",
                        StringUtils.isNoneBlank(warn_fieldvalue)?warn_fieldvalue:"");
                jedis.hset(redisKey,"send_mobile",
                        StringUtils.isNoneBlank(send_mobile)?send_mobile:"");
                jedis.hset(redisKey,"send_type",
                        StringUtils.isNoneBlank(send_type)?send_type:"");

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            JedisUtil.close(jedis);
        }
        LOG.info("===========同步规则成功===========" + ruleList.size());
    }

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new SyncRule2RedisTimer(),0,1*3*1000);
    }
}
