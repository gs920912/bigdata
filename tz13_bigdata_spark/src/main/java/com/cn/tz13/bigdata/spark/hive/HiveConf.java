package com.cn.tz13.bigdata.spark.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Iterator;
import java.util.Map;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-19:06
 */
public class HiveConf {
    private volatile static HiveContext hiveContext;
    public static HiveContext getHiveContext(SparkContext sparkContext){
        if (hiveContext == null){
            synchronized(HiveConf.class){
                if (hiveContext == null){
                    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\hadoop.dll");
                    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\winutils.exe");
                    hiveContext = new HiveContext(sparkContext);
                    //本机跑需要加载集群的配置参数
                    Configuration conf = new Configuration();
                    conf.addResource("spark/hive/core-site.xml");
                    conf.addResource("spark/hive/hive-site.xml");
                    conf.addResource("spark/hive/hdfs-site.xml");
                    //现在这个参数只是加载conf里面去了，还没有加载到 hiveContext
                    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
                    while (iterator.hasNext()){
                        Map.Entry<String, String> next = iterator.next();
                        hiveContext.setConf(next.getKey(),next.getValue());
                    }
                }
            }
        }
        return hiveContext;
    }
}
