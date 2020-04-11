package com.cn.tz13.bigdata.spark.streaming.hive



import org.apache.commons.configuration.{CompositeConfiguration, PropertiesConfiguration}
import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * @Description TODO
  * @author gs
  * @date 2020/2/17-17:13 
  */
object HiveConfig  extends Serializable with Logging{



  //配置文件路径
  val hiveFilePath = "es/mapping/fieldmapping.properties"
  val rootPath = "/user/hive/external"
  //获取的字段可以按序排列
  var config : CompositeConfiguration = null
  var tables: java.util.List[_] = null
  var hiveTableSQL: java.util.HashMap[String, String] = null
  var partitionSQL: java.util.HashMap[String, String] = null
  var mapSchema: java.util.HashMap[String, StructType] = null
  init()







  def init(): Unit = {
    println("=============记载配置文件config=============")
    config = HiveConfig.readCompositeConfiguration(hiveFilePath)
    val keys = config.getKeys
    while (keys.hasNext){
      println(keys.next())
    }
    println("=============获取分区tables=============")
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })
    println("=============获取分区tables=============")
    partitionSQL = createPartitionSQL
    partitionSQL.foreach(sql => {
      println(sql)
    })
    println("=============获取建表语句=============")
    hiveTableSQL = createHiveTables()
    hiveTableSQL.foreach(tableSQL => {
      println(tableSQL)
    })
    println("=============获取表schema=============")
    mapSchema = createSchema
    mapSchema.foreach(x=>{println(x)})

  }

  def createSchema: java.util.HashMap[String, StructType] = {
    val mapStructType = new java.util.HashMap[String, StructType]
    for (table <- tables){
      val arrayStructFields = ArrayBuffer[StructField]()
      //获取某一表的所有字段
      val tableFields = config.getKeys(table.toString)
      //根据字段和类型构造StructField
      while (tableFields.hasNext){
        val key = tableFields.next()
        val field = key.toString.split("\\.")(1)
        val fieldType = config.getProperty(key.toString)
        fieldType match {
          case "string"=> arrayStructFields += StructField(field,StringType,true)
          case "long"=> arrayStructFields += StructField(field,StringType,true)
          case "double"=> arrayStructFields += StructField(field,StringType,true)
          case _ =>
        }
        val schema = StructType(arrayStructFields)
        mapStructType.put(table.toString,schema)
      }
    }
    mapStructType
  }
  /**
    * 创建建表语句
    */
  def createPartitionSQL: java.util.HashMap[String, String] = {
    val hiveSqlMap = new java.util.HashMap[String, String]
    tables.foreach(table => {
      //构建sql建表语句，拼接
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //获取这个表类型的所有字段
      val fields = config.getKeys(table.toString)
      //拼接字段
      fields.foreach(tableField => {
        //获取真实字段，去掉前缀
        val field = tableField.toString.split("\\.")(1)
        //获取字段类型
        val fieldType = config.getProperty(tableField.toString)
        //拼接字段
        sql = sql + field
        //拼接类型
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      //字段循环完成之后，切掉最后多的一个","
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s") partitioned by (year string,month " +
        s"string,day string) STORED AS PARQUET LOCATION '" +
        s"${rootPath}/${table}'"
      hiveSqlMap.put(table.toString,sql)
    })
    hiveSqlMap
  }
  /**
    * 创建建表语句
    */
  def createHiveTables(): java.util.HashMap[String, String] = {
    val hiveSqlMap = new java.util.HashMap[String, String]
    tables.foreach(table => {
      //构建sql建表语句，拼接
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //获取这个表类型的所有字段
      val fields = config.getKeys(table.toString)

      //拼接字段
      fields.foreach(tableField => {
        //获取真实字段，去掉前缀
        val field = tableField.toString.split("\\.")(1)
        //获取字段类型
        val fieldType = config.getProperty(tableField.toString)
        //拼接字段
        sql = sql + field
        //拼接类型
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })

      //字段循环完成之后，切掉最后多的一个","
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s") STORED AS PARQUET LOCATION '${rootPath}/qq'"
      hiveSqlMap.put(table.toString, sql)
    })
    hiveSqlMap
  }

  /**
    * 配置文件读取
    * @param path
    * @return
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    val compositeConfiguration = new CompositeConfiguration
    try {
      val configuration = new PropertiesConfiguration(path)
      compositeConfiguration.addConfiguration(configuration)
    } catch {
      case e =>{
        logError(s"加载配置文件${path}失败", e)
      }
    }
    logInfo(s"加载配置文件${path}成功")
    compositeConfiguration
  }
}
