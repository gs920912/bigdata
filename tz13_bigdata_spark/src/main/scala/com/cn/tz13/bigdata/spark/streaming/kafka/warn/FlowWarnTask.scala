package com.cn.tz13.bigdata.spark.streaming.kafka.warn

import java.util

import org.apache.spark.streaming.dstream.DStream

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-21:43 
  */
object FlowWarnTask {
  def beginWarn(DS: DStream[util.Map[String, String]]):Unit = {
    val DS_new = DS.map(map => {
      val collect_time = map.get("collect_time")
      java.lang.Long.valueOf(collect_time)
    })
    DS_new.foreachRDD(rdd => {
      //4秒的流量
      val flow = rdd.reduce(_+_)
      println("flow =========" + flow)
      //预警
      if(flow > 1000000){
        println("【流量预警】==》 流量超出阈值" + 1000000)
      }
    })
  }

}
