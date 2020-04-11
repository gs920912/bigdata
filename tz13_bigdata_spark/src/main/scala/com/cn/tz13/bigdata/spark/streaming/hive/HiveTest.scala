package com.cn.tz13.bigdata.spark.streaming.hive

import com.cn.tz13.bigdata.spark.common.SscFactory
import com.cn.tz13.bigdata.spark.hive.HiveConf
import com.cn.tz13.bigdata.spark.streaming.KafkaParamsUtil
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-16:54 
  */
object HiveTest  extends Serializable with Logging {
  def main(args: Array[String]): Unit = {
    //TODO 构建DS流
    val kafkaParams = KafkaParamsUtil.getKafkaParams("HiveTest")
    val ssc = SscFactory.newLocalSSC("WarnStreamingTask",
      4L,2)
    val kafkaManager = new KafkaManager(kafkaParams,false)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaManager.createJson2MapStringDricetStreamWithOffset(ssc,Set("chl_test2"))
    val sparkContext = ssc.sparkContext
    //TODO 动态创建HIVE表
    //1.构造hiveContext
    val hiveContext = HiveConf.getHiveContext(sparkContext)
    hiveContext.sql("use default")
    //2.创建HIVE表，读取配置文件，遍历构建
    HiveConfig.hiveTableSQL.foreach(map => {
      hiveContext.sql(map._2)
    })
    //TODO 往HDFS写入数据
    HiveConfig.tables.foreach(table => {
      //按表名（类型）进行过滤数据
      val tableDS = DS.filter(map => {
        table.equals(map.get("table"))
      })
      //向HDFS写入  使用DF写入  RDD转DF
      tableDS.foreachRDD(rdd => {
        val tableSchema = HiveConfig.mapSchema.get(table.toString)
        val schemaFields = tableSchema.fieldNames
        //构建rowRDD
        val rowRDD = rdd.map(map => {
          //需要把map转为row
          val listRow = new java.util.ArrayList[Object]()
          //填充listRow
          for (schemaField <- schemaFields){
            listRow.add(map.get(schemaField))
          }
          Row.fromSeq(listRow)
        })
        //构建DF
        val tableDF = hiveContext.createDataFrame(rowRDD,tableSchema)
        tableDF.show(2)
        //写入HDFS
        //1.定义HDFS目录
        val tableHdfsPath = s"hdfs://cdh12:8020${HiveConfig.rootPath}/${table}"
        //2.写入
        tableDF.write.mode(SaveMode.Append).parquet(tableHdfsPath)
        //TODO 建立映射关系
        val sql = s"ALTER TABLE ${table} SET LOCATION '${tableHdfsPath}'"
        println("======================="  + sql)
        hiveContext.sql(sql)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
