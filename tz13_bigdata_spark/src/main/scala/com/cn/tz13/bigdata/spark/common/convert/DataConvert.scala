package com.cn.tz13.bigdata.spark.common.convert
import java.util

import com.cn.tz13.bigdata.config.ConfigUtil

import scala.collection.JavaConversions._
/**
  * @Description 数据类型转换
  * @author gs
  * @date 2020/2/3-17:00 
  */
object DataConvert {


  //获取映射关系
  val fieldMappingPath = "es/mapping/fieldmapping.properties"
  val tableFieldMap = DataConvert.getEsFieldtypeMap()
  def strMap2esObjectMap(map: util.Map[String, String]):util.HashMap[String, Object] ={
    //获取这条数据的数据类型
    val table = map.get("table")
    //根据类型获取 映射关系
    val fieldmappingMap = tableFieldMap.get(table)
    val objectMap = new util.HashMap[String, Object]
    //真实数据的所有字段
    val set = map.keySet().iterator()
    while (set.hasNext){
      val key = set.next()
      //通过key获取数据类型
      val dataType = fieldmappingMap
    }

  }
  def getEsFieldtypeMap():util.HashMap[String, util.HashMap[String, String]] = {
    val mapMap = new util.HashMap[String, util.HashMap[String, String]]
    val properties = ConfigUtil.getInstance().getProperties(fieldMappingPath)
    //获取所有的key
    val tableFields = properties.keySet()
    //获取所有的类型
    // wechat,mail,qq
    val tables = properties.get("tables").toString.split(",")
    //通过tables筛选字段
    tables.foreach(table => {
      val map = new util.HashMap[String, String]
      tableFields.foreach(tableField => {
        if (tableField.toString.startsWith(table)){
          val key = tableField.toString.split("\\.")(1)
          val value = properties.get(tableField).toString
          map.put(key, value)
        }
      })
      mapMap.put(table, map)
    })
    mapMap
  }
}
