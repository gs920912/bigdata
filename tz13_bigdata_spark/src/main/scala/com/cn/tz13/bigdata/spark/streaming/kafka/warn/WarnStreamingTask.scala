package com.cn.tz13.bigdata.spark.streaming.kafka.warn

import java.util.Timer

import com.cn.tz13.bigdata.es.client.ESclientUtil
import com.cn.tz13.bigdata.redis.client.JedisUtil
import com.cn.tz13.bigdata.spark.common.SscFactory
import com.cn.tz13.bigdata.spark.streaming.KafkaParamsUtil
import com.cn.tz13.bigdata.spark.streaming.kafka.KafkaParamsUtil
import com.cn.tz13.bigdata.spark.warn.dao.WarningMessageDao
import com.cn.tz13.bigdata.spark.warn.domain.WarningMessage
import com.cn.tz13.bigdata.spark.warn.timer.{DingDingWarnImpl, PhoneWarnImpl, SyncRule2RedisTimer}
import com.cn.tz13.bigdata.time.TimeTranstationUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager
import redis.clients.jedis.Jedis

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-21:29 
  */
object WarnStreamingTask extends Serializable with Logging{

  def main(args: Array[String]): Unit = {
    //TODO 定时任务，同步规则
    val timer: Timer = new Timer
    timer.schedule(new SyncRule2RedisTimer,0,1 * 3 * 1000)
    //TODO 构建DS流
    val kafkaParams = KafkaParamsUtil.getKafkaParams("WarnStreamingTask")
    val ssc = SscFactory.newLocalSSC("WarnStreamingTask",
      4L,2)
    val kafkaManager = new KafkaManager(kafkaParams,false)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaManager.createJson2MapStringDricetStreamWithOffset(ssc,
      Set("chl_test2")).persist(StorageLevel.MEMORY_AND_DISK)
    //流量告警
    FlowWarnTask.beginWarn(DS)
    //黑名单告警
    BlockWarnTask.beginWarn(DS)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 预警
    * @param redisKey
    * @param jedis
    * @param map
    */
  def warn(redisKey:String,jedis:Jedis,map:java.util.Map[String,String]): Unit ={
    //拼装告警消息，发送出去
    val spilt = redisKey.split(":")
    if (spilt.length == 2){
      //构造告警消息
      val message = new WarningMessage()
      //从redis中获取组装信息
      val redisMap = jedis.hgetAll(redisKey)
      message.setAlarmRuleid(redisMap.get("id"))
      message.setSendMobile(redisMap.get("send_mobile"))
      message.setSendType(redisMap.get("send_type"))
      message.setAccountid(redisMap.get("publisher"))
      message.setAlarmType("2")
      //构造告警内容体
      val warn_type = "【黑名单告警】=>"
      //獲取經緯度
      val longitude = map.get("longitude")
      val latitude = map.get("latitude")
      val collect_time = map.get("collect_time")
      val phone = map.get("phone")
      val realDate = TimeTranstationUtils.Date2yyyyMMdd_HHmmss(
        java.lang.Long.valueOf(collect_time+"000"))
      val warn_content = s"${warn_type}【手机号为${phone}】的嫌犯在" +
        s"${realDate}出现在经纬度【${longitude},${latitude}】" +
        s"的地方，具体地址为 XXXX百货大厦 附近"
      //头像 //省份证 //姓名 //邮箱  //QQ  //微博
      message.setSenfInfo(warn_content)
      //TODO 写入数据库，为了前端进行调用
      WarningMessageDao.insertWarningMessageReturnId(message)
      //TODO 告警消息发送
      if(message.getSendType.equals("2")){
        val warnI = new PhoneWarnImpl()
        warnI.warn(message)
      }

      if(message.getSendType.equals("2")){
        val warnI = new DingDingWarnImpl()
        warnI.warn(message)
      }
    }
  }
}
