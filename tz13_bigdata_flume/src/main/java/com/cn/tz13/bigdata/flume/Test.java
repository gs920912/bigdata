package com.cn.tz13.bigdata.flume;

import com.cn.tz13.bigdata.config.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/2-16:34
 */
public class Test {
    public static void main(String[] args) {
        Properties properties;

        String fieldsPath = "common/datatype.properties";
        properties = ConfigUtil.getInstance().getProperties(fieldsPath);



        String line = "324354578998989\t324354578998989\t24.000000\t25.000000\t1c-41-cd-ae-4f-4f\t1c-41-cd-ae-4f-4f\t32109231\t1557305985\tweixin2\t186094322221\tjudy\t\t\t1789098763";
        String field = properties.get("wechat").toString();


        String[] lines = line.split("\t");
        String[] fields = field.split(",");

        Map data = new HashMap<String,String>();
        for (int i = 0; i < fields.length; i++) {
            data.put(fields[i],lines[i]);
        }
        System.out.println(data);

    }
}
