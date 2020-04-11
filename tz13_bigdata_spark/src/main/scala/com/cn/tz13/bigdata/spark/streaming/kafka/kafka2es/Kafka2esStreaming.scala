package com.cn.tz13.bigdata.spark.streaming.kafka.kafka2es

import com.cn.tz13.bigdata.es.admin.AdminUtil
import com.cn.tz13.bigdata.es.client.ESclientUtil
import com.cn.tz13.bigdata.spark.common.SscFactory
import com.cn.tz13.bigdata.spark.common.convert.DataConvert
import com.cn.tz13.bigdata.spark.streaming.KafkaParamsUtil
import com.cn.tz13.bigdata.spark.streaming.kafka.KafkaParamsUtil
import com.cn.tz13.bigdata.time.TimeTranstationUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-16:03 
  */
object Kafka2esStreaming extends Serializable with Logging{

  def main(args: Array[String]): Unit = {
    //测试，不想记录offset
    val kafkaParams = KafkaParamsUtil.getKafkaParams("Kafka2esStreaming")
    val ssc = SscFactory.newLocalSSC(
      "KafkaManagerTest",4L,2)
    val kafkaManager = new KafkaManager(kafkaParams,false)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaManager.createJson2MapStringDricetStreamWithOffset(ssc,Set("chl_test2"))

    val array = Array("qq","mail","wechat")
//    val DS1 = DS.filter(map => {
//      "wechat".equals(map.get("table"))
//    })
    array.foreach(table=>{
      val tableDS = DS.filter(map =>{
        table.equals(map.get("table"))
      })
      tableDS.foreachRDD(rdd=>{
        EsSpark.saveToEs(rdd,s"${table}/${table}",
          KafkaParamsUtil.getEsParams("id"))
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
