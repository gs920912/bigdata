package com.cn.tz13.bigdata.spark.streaming.hive

import com.cn.tz13.bigdata.spark.common.SscFactory
import com.cn.tz13.bigdata.spark.hive.HiveConf
import com.cn.tz13.bigdata.spark.streaming.KafkaParamsUtil
import com.cn.tz13.bigdata.time.TimeTranstationUtils
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._
/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-20:08 
  */
object PartitionHIve extends Serializable with Logging{
  def main(args: Array[String]): Unit = {
    //TODO 构建DS流
    val kafkaParams = KafkaParamsUtil.getKafkaParams("HiveTest")
    val ssc = SscFactory.newLocalSSC("WarnStreamingTask",
      4L,2)
    val kafkaManager = new KafkaManager(kafkaParams,false)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaManager.createJson2MapStringDricetStreamWithOffset(ssc,
      Set("chl_test2")).map(map => {
      //添加日期字段
      val collect_time = map.get("collect_time")
      //日期工具类
      val date = TimeTranstationUtils.Date2yyyyMMdd(java.lang
          .Long.valueOf(collect_time+"000"))
      //构建3个分区字段
      val year = date.substring(0,4)
      val month = date.substring(4,6)
      val day = date.substring(6,8)
      map.put("dayPartion",date)
      map.put("year",date)
      map.put("month",date)
      map.put("day",date)
      map
    })
    val sparkContext = ssc.sparkContext
    //TODO 动态创建HIVE表
    //1.构造hiveContext
    val context = HiveConf.getHiveContext(sparkContext)
    context.sql("use default")
    //2.创建HIVE表，读取配置文件，遍历构建
    HiveConfig.partitionSQL.foreach(map => {
      context.sql(map._2)
    })
    //TODO 往HDFS写入数据
    HiveConfig.tables.foreach(table => {
      //按表名（类型）进行过滤数据
      val tableDS = DS.filter(map => {
        table.equals(map.get("table"))
      })
      //向HDFS写入  使用DF写入  RDD转DF
      tableDS.foreachRDD(rdd => {
        //TODO 按日期分组，拿到RDD中所有的日期
        val arrayDays = rdd.map(x => {x.get("dayPartion")})
          .distinct().collect()
        arrayDays.foreach(date => {
          //构建3个分区字段
          val year = date.substring(0,4)
          val month = date.substring(4,6)
          val day = date.substring(6,8)
          val tableSchema = HiveConfig.mapSchema.get(table.toString)
          val schemaFields = tableSchema.fieldNames
          //构建rowRDD
          val rowRDD = rdd.filter(map => {
            date.equals(map.get("dayPartion"))
          }).map(map =>{
            //需要把map转为row
            val listRow = new java.util.ArrayList[Object]()
            //填充listRow
            for (schemaField <- schemaFields){
              listRow.add(map.get(schemaField))
            }
            Row.fromSeq(listRow)
          })
          //构建DF
          val tableDF = context.createDataFrame(rowRDD,tableSchema)
          tableDF.show(2)
          //写入HDFS
          //1.定义HDFS目录
          val tableHdfsPath = s"hdfs://cdh12:8020${HiveConfig.rootPath}" +
            s"/${table}/${year}/${month}/${day}"
          //2.写入
          tableDF.write.mode(SaveMode.Append).parquet(tableHdfsPath)
          //TODO 建立映射关系
          val sql = s"ALTER TABLE ${table} ADD IF NOT EXISTS PARTITION" +
            s"(year='${year}',month='${month}',day='${day}') LOCATION '" +
            s"${tableHdfsPath}'"
          println("======================="  + sql)
          context.sql(sql)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
