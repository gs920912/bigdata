package com.cn.tz13.bigdata.es.admin;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-17:50
 */
public class MappingUtil {
    private static final Logger LOG = Logger.getLogger(MappingUtil.class);

    /**
     * 创建mapping
     * @param client
     * @param index
     * @param type
     * @param jsonString
     * @return
     */
    public static boolean addMapping(TransportClient client,
                                     String index,
                                     String type,
                                     String jsonString) {
        PutMappingResponse putMappingResponse = null;
        try {
            PutMappingRequest putMappingRequest = new PutMappingRequest(index)
                    .type(type).source(JSON.parseObject(jsonString));
            putMappingResponse = client.admin().indices().putMapping(putMappingRequest)
                    .actionGet();

        }catch (Exception e){
            LOG.error("创建maaping错误");
            LOG.error(null, e);
        }
        boolean acknowledged = putMappingResponse.isAcknowledged();
        if (acknowledged){
            LOG.info("创建"+ type + "的mapping成功");
        }
        return acknowledged;
    }
    public static boolean addMapping2(TransportClient client,String index,
                                      String type,String jsonString){
        PutMappingResponse putMappingResponse = null;
        try {
            putMappingResponse = client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .setSource(jsonString)
                    .get();
        } catch (Exception e) {
            LOG.error("创建maaping错误");
            LOG.error(null, e);
        }

        boolean acknowledged = putMappingResponse.isAcknowledged();
        return acknowledged;
    }
}
