package com.cn.tz13.bigdata.es.client;
import com.cn.tz13.bigdata.config.ConfigUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/3-17:30
 */
public class ESclientUtil {
    private static final Logger LOG = Logger.getLogger(ESclientUtil.class);
    /**配置文件地址*/
    private static final String ESCOBFIG_PATH = "es/es_cluster.properties";

    private volatile static TransportClient client;
    private ESclientUtil(){};
    /**读取配置文件*/
    private static Properties properties;
    static {
        properties = ConfigUtil.getInstance().getProperties(ESCOBFIG_PATH);
    }
    public static TransportClient getClient(){
        //获取配置
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        String host = properties.get("es.cluster.nodes1").toString();
//        String host2 = properties.get("es.cluster.nodes2").toString();
//        String host3 = properties.get("es.cluster.nodes3").toString();
        int port = Integer.valueOf(properties.get("es.cluster.tcp.port").toString());
        String myClusterName = properties.get("es.cluster.name").toString();
        Settings settings = Settings.builder().put("cluster.name", myClusterName).build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
//                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host2), port))
//                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host3), port));
        }catch (UnknownHostException e){
            e.printStackTrace();
        }
        return client;
    }
}
