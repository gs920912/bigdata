package com.cn.tz13.bigdata.config;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-15:53
 */
public class ConfigUtil {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);
    /**单例模式*/
    /**定义一个私有静态变量*/
    private static volatile ConfigUtil configUtil;
    /**提供一个私有构造方法*/
    private ConfigUtil(){}
    /**提供一个公用的静态方法获取*/
    public static ConfigUtil getInstance(){
        //双重否定
        if(configUtil == null){
            synchronized (ConfigUtil.class){
                if (configUtil == null){
                    configUtil = new ConfigUtil();
                }
            }
        }
        return configUtil;
    }
    /**定义一个读取配置文件的方法*/
    public Properties getProperties(String path){
        Properties properties = new Properties();
        try{
            LOG.info("开始加载配置文件" + path);
            InputStream insss = this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(insss);
            LOG.info("开始加载配置文件" + path +"成功");
        }catch (Exception e){
            LOG.error("加载配置文件" + path +"失败");
            LOG.error(null,e);
        }
        return properties;
    }


    public static void main(String[] args) {
        String path = "kafka/kafka-server-config.properties";
        Properties properties = ConfigUtil.getInstance().getProperties(path);
        properties.keySet().forEach(key->{
            System.out.println(key);
        });
    }


}
