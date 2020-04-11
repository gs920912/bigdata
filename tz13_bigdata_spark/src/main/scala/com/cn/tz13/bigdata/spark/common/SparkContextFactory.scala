package com.cn.tz13.bigdata.spark.common

import org.apache.spark.Logging
import org.apache.spark.SparkContext

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-14:40 
  */
object SparkContextFactory  extends Serializable with Logging{
  /**
    * 構造SparkContext
    * @param appname
    * @param threads
    */
  def newSparkLocalBatchContext(appname:String = "default",
                                threads:Int = 1): SparkContext ={

    val sparkConf = SparkConfFactory.newSparkLocalConf(appname,threads)
    new SparkContext(sparkConf)
  }

}
