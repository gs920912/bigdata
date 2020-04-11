package com.cn.tz13.bigdata.kafka.config;

import com.cn.tz13.bigdata.config.ConfigUtil;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-17:46
 */
public class KafkaConfig {
    private static final Logger LOG = Logger.getLogger(KafkaConfig.class);
    /**定义一个kafka配置路径*/
    private static final String DEFAULT_KAFKA_CONFIG_PATH = "kafka/kafka-server-config.properties";
    /**kafka的配置文件*/
    private Properties properties;
    private ProducerConfig producerConfig;
    /**定义一个私有的静态变量*/
    private static volatile KafkaConfig kafkaConfig = null;
    /**私有构造方法*/
    private KafkaConfig(){
        //读取配置文件
        LOG.info("开始实例化producerConfig");
        properties = ConfigUtil.getInstance().getProperties(DEFAULT_KAFKA_CONFIG_PATH);
        producerConfig = new ProducerConfig(properties);
        LOG.info("实例化producerConfig成功");
    }

    /**对外获取对象的一个公共方法*/
    public static KafkaConfig getInstance(){
        //双重否定
        if(kafkaConfig == null){
            synchronized (ConfigUtil.class){
                if(kafkaConfig == null){
                    kafkaConfig = new KafkaConfig();
                }
            }
        }
        return kafkaConfig;
    }

    public ProducerConfig getProducerConfig(){
        return producerConfig;
    }

}
