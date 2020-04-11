package com.cn.tz13.bigdata.es.admin;
import com.cn.tz13.bigdata.common.file.FileCommon;
import com.cn.tz13.bigdata.es.client.ESclientUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;


/**
 * @author gs
 * @Description TODO
 * @date 2020/2/3-17:29
 */
public class AdminUtil {
    private static final Logger LOG = Logger.getLogger(AdminUtil.class);

    public static void main(String[] args) throws Exception{
        AdminUtil.buildIndexAndType("aaaaa","aaaaa",
                "es/mapping/wechat.json",14,1);
    
    }

    /**
     * 创建索引并且创建mapping
     * @param index
     * @param type
     * @param path
     * @param shard
     * @param replication
     * @return
     */
    public static boolean buildIndexAndType(String index,
                                            String type,
                                            String path,
                                            int shard,
                                            int replication) {

        boolean flag;
        try {
            //获取客户端
            TransportClient client = ESclientUtil.getClient();
            //创建索引
            boolean indices = AdminUtil.createIndices(client, index, shard, replication);
            //创建mapping
            if (indices){
                LOG.info("创建索引"+ index + "成功");
                //读取json配置文件
                String json = FileCommon.getAbstractPath(path);
                flag = MappingUtil.addMapping(client,index,type,json);
            }else {
                flag =false;
                LOG.error("创建索引"+index+"失败");
            }
        }catch (Exception e){
            flag = false;
            LOG.error("创建索引"+index+"失败");
            LOG.error(null,e);
        }
        return flag;
    }

    /**
     * 创建索引
     * @param client
     * @param index
     * @param shard
     * @param replication
     * @return
     */
    private static boolean createIndices(TransportClient client,
                                         String index,
                                         int shard,
                                         int replication) {
        CreateIndexResponse createIndexResponse = null;
        //判断索引是不是存在
        if (!indexExists(client, index)){
            createIndexResponse = client.admin().indices()
                    .prepareCreate(index).setSettings(Settings.builder()
                        .put("index.number_of_shards", shard)
                        .put("index.number_of_replicas", replication))
                    .get();
        }
        LOG.info("索引"+index + "已经存在");

        return createIndexResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     * @param client
     * @param index
     * @return
     */
    private static boolean indexExists(TransportClient client, String index) {
        boolean tag = false;
        try {
            IndicesExistsResponse indicesExistsResponse = client.admin()
                    .indices().prepareExists(index).execute().actionGet();
            tag = indicesExistsResponse.isExists();
        }catch (Exception e){
            LOG.error("判断索引是否存在异常");
            LOG.error(null, e);
        }
        return tag;
    }
}
