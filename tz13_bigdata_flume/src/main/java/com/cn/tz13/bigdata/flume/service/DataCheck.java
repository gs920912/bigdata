package com.cn.tz13.bigdata.flume.service;

import com.cn.tz13.bigdata.config.ConfigUtil;
import com.cn.tz13.bigdata.flume.constant.ConstantsFields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/2-15:56
 */
public class DataCheck {
    static Properties properties;
    static {
        String fieldsPath = "common/datatype.properties";
        properties = ConfigUtil.getInstance().getProperties(fieldsPath);
    }

    /**
     * @param fileName          文件名
     * @param absolute_filename 文件绝对路径名
     * @param line              记录内容
     */
    public static Map txtParseAndValidation(String fileName,
                                            String absolute_filename, String line) {
        //存放正确数据(line)
        Map data = new HashMap<String,String>();
        //存放错误信息
        Map errorData = new HashMap<String,String>();
        //清洗，转换，加工
        //imei imsi longitude latitude phone_mac device_mac device_number collect_time username phone object_username send_message accept_message message_time
        // 324354578998989	324354578998989	24.000000	25.000000	1c-41-cd-ae-4f-4f	1c-41-cd-ae-4f-4f	32109231	1557305985	weixin2	186094322221	judy			1789098763
        //TODO 首先对数据进行转换
        //首先要获取数据字典，根据类型获取
        String table = fileName.split("_")[0].toLowerCase();
        //根据数据类型获取数据字典
        String[] fields = properties.get(table).toString().split(",");
        String[] lines = line.split("\t");
        if (fields.length == lines.length){
            for (int i = 0; i < fields.length; i++) {
                data.put(fields[i],lines[i]);
            }
            //TODO 数据加工  根据业务需求对数据进行调整
            //1.没有唯一ID，可以解决数据重复消费问题
            data.put("id", UUID.randomUUID().toString()
                    .replace("-",""));
            //2.添加数据类型
            data.put("table",table);
            data.put("rksj",(System.currentTimeMillis()/1000)+"");
            data.put(ConstantsFields.FILE_NAME,fileName);
            data.put(ConstantsFields.ABSOLUTE_FILENAME,absolute_filename);
        }else {
            //错误数据
            errorData.put("leng_error","字段和值的长度不匹配");
            errorData.put("leng","字段和值的长度不匹配,需要的长度为"
                    +fields.length + "\t" + "实际长度" + lines.length);
        }
        //如果errorData里面有错误数据
        if(errorData.size() > 0){
            data = null;
            //写入ES  写mysql
        }
        return data;
    }
}
