package com.cn.tz13.bigdata.spark.streaming

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-15:55 
  */
object KafkaParamsUtil {

  def getKafkaParams(groupId:String):Map[String,String] = {
    val kafkaParams:Map[String,String] = Map[String,String](
      "metadata.broker.list"->"cdh12:9092",
      "group.id"->groupId,
      "auto.offset.reset"->"smallest"
    )
    kafkaParams
  }
  def getEsParams(idField:String):Map[String,String] = {
    Map("es.mapping.id" -> idField,
      "es.nodes"->"cdh12",
      "es.port"->"9200",
      "es.clustername"->"my-application")
  }
}

