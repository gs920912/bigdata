package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-14:22 
  */
object KafkaUtilsTest888 {
  //TODO  KafkaUtils是一个直接提供kafka流数据的一个工具类
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.setAppName("KafkaUtilsTest888").setMaster("local[2]")
    // sparkConf.set("spark.streaming.receiver.maxRate","1")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(2L))


    val DS = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, KafkaClusterTest.kafkaParams,Set("chl_test2"))

    DS.foreachRDD(rdd=>{
      val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetList.foreach(x=>{
        println(s"获取kafka中的偏移信息${x}" )
      })
      rdd.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()

    /*   def createDirectStream[
       K: ClassTag,
       V: ClassTag,
       KD <: Decoder[K]: ClassTag,
       VD <: Decoder[V]: ClassTag] (
                                     ssc: StreamingContext,
                                     kafkaParams: Map[String, String],
                                     topics: Set[String]
                                   ): InputDStream[(K, V)] = {
         val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
         val kc = new KafkaCluster(kafkaParams)
         val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
         new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
           ssc, kafkaParams, fromOffsets, messageHandler)*/


  }
}
