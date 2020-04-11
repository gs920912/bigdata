package com.cn.tz13.bigdata.spark.streaming.hive

import com.cn.tz13.bigdata.spark.common.SparkContextFactory
import com.cn.tz13.bigdata.spark.hive.HdfsAdmin
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.JavaConversions._
/**
  * @Description 小文件合并
  * @author gs
  * @date 2020/2/17-21:08 
  */
object CombineHdfs extends Serializable with Logging{
  def main(args: Array[String]): Unit = {
    val sc = SparkContextFactory.newSparkLocalBatchContext(
      "CombineHdfs",1)
    val sqlContext = new SQLContext(sc)
    //TODO 读取所有小文件
    HiveConfig.tables.foreach(table => {
      println("=============开始合并小文件==============")
      val table_path = s"hdfs://cdh12:8020${HiveConfig.rootPath}/${table}"
      val tableDF = sqlContext.read.load(table_path)
      tableDF.show(2)
      //获取目录下的所有文件名，注意不要获取元数据文件，只要以part开头的文件
      val fileSystem:FileSystem = HdfsAdmin.get().getFs
      val fileStatusArray:Array[FileStatus] = fileSystem
        .globStatus(new Path(table_path+"/part*"))
      //将文件状态转为真实路径
      //获取此部分是为了删除小文件使用
      val paths:Array[Path] = FileUtil.stat2Paths(fileStatusArray)
      //TODO 组成大文件，重新写入
      tableDF.repartition(1).write.mode(SaveMode.Append)
        .parquet(table_path)
      //TODO 删除小文件
      paths.foreach(path => {
        fileSystem.delete(path)
        println(s"删除小文件${path}成功")
      })
    })
  }
}
