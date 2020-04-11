package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-14:17 
  */
object KafkaManagerTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaManagerTest").setMaster("local[2]")
    // sparkConf.set("spark.streaming.receiver.maxRate","1")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(2L))

    val kafkaManager = new KafkaManager(KafkaClusterTest.kafkaParams)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaManager.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc,Set("chl_test2"))
    DS.foreachRDD(rdd=>{
      rdd.foreach(println(_))
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
