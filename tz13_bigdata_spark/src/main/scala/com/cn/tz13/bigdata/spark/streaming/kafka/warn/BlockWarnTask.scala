package com.cn.tz13.bigdata.spark.streaming.kafka.warn

import java.util

import com.cn.tz13.bigdata.redis.client.JedisUtil
import com.cn.tz13.bigdata.spark.streaming.kafka.warn.WarnStreamingTask.warn
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-21:44 
  */
object BlockWarnTask {


  def beginWarn(DS: DStream[util.Map[String, String]]):Unit = {
    //黑名单告警
    val array = Array("phone")
    DS.foreachRDD(rdd => {
      //序列化问题  RDD外未经序列化的对象在RDD内部无法使用
      rdd.foreachPartition(partion => {
        val jedis = JedisUtil.getJedis(15)
        while (partion.hasNext){
          val map = partion.next()
          //构建比对的key
          array.foreach(field => {
            if (map.containsKey(field)){
              //字段值
              val fieldValue = map.get(field)
              //构建rediskey
              val redisKey = field +":" + fieldValue
              //进行比对  就是判断这个redisKey 是不是存在
              val boolean = jedis.exists(redisKey)
              if (boolean){
                //TODO 说明命中
                //时间控制，相同告警消息，30秒一次 ，可以将命中时间记录到规则中
                val warn_time = jedis.hget(redisKey,"warn_time")
                //TODO 时间是不是超过30秒
                //上一次命中时间
                val warn_time_long = java.lang.Long.valueOf(warn_time)
                //当前时间
                val now_time_long = System.currentTimeMillis()/1000
                if (now_time_long - warn_time_long >30){
                  //如果时间间隔大于30秒。告警
                  warn(redisKey,jedis,map)
                  //刷新时间
                  jedis.hset(redisKey,"warn_time",System
                    .currentTimeMillis()/1000+"")
                }

              }else{
                //之前没有被名中过，现在是第一次命中
                //告警
                warn(redisKey,jedis,map)
                //记录告警时间
                jedis.hset(redisKey,"warn_time",System
                  .currentTimeMillis()/1000+"")
              }
            }else{
              //TODO 没有命中
            }
          })
        }
      })
    })
  }

}
