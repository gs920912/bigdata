package com.cn.tz13.bigdata.spark.common

import org.apache.spark.{Logging, SparkConf}

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-14:34 
  */
object SparkConfFactory extends Serializable with Logging{
  /**
    * 本地批处理
    */
  def newSparkLocalConf(appName:String="default",threads:Int=1): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(appName).setMaster(s"local[${threads}]")
  }
}
