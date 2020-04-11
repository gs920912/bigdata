package com.cn.tz13.bigdata.spark.streaming.kafka.kafka2es

import com.cn.tz13.bigdata.spark.common.SscFactory
import com.cn.tz13.bigdata.spark.streaming.KafkaParamsUtil
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-16:40 
  */
object Kafka2esStreamingTest {
  def main(args: Array[String]): Unit = {
    //测试，不想记录offset
    val kafkaParams = KafkaParamsUtil.getKafkaParams(
      "Kafka2esStreamingTest")
    val ssc = SscFactory.newLocalSSC("KafkaManagerTest",
      4L,2)
    val kafkaManager = new KafkaManager(kafkaParams,false)
    val DS = kafkaManager.createJson2MapStringDricetStreamWithOffset(ssc,Set("chl_test2"))
    DS.foreachRDD(rdd=>{





      //写入ES
      //rdd.take(2).foreach(println(_))
      //MAP JSON
      // EsSpark.saveToEs(rdd, "test/test", )
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
