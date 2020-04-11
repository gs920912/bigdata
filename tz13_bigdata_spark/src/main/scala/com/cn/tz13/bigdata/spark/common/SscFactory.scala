package com.cn.tz13.bigdata.spark.common


import org.apache.spark.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/3-15:49 
  */
object SscFactory  extends Serializable with Logging{
  def newLocalSSC(appName:String="default",
                  batchInterval:Long=3L,
                  threads:Int=1):StreamingContext = {
    val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    new StreamingContext(sparkConf,Seconds(batchInterval))
  }

}
