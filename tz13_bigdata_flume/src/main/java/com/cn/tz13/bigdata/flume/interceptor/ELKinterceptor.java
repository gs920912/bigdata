package com.cn.tz13.bigdata.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.tz13.bigdata.flume.constant.ConstantsFields;
import com.cn.tz13.bigdata.flume.service.DataCheck;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/2-15:49
 */
public class ELKinterceptor implements Interceptor {

    /**拦截方法*/
    @Override
    public Event intercept(Event event) {
        if(event == null){
            return null;
        }
        //TODO 获取事件的信息
        //消息头
        Map<String, String> headers = event.getHeaders();
        String fileName = headers.get(ConstantsFields.FILE_NAME);
        String absolute_filename = headers.get(ConstantsFields.ABSOLUTE_FILENAME);
        //消息
        String line = new String(event.getBody(), Charsets.UTF_8);
        //TODO 对数据进行处理
        //清洗，转换，加工
        Map map = DataCheck.txtParseAndValidation(fileName, absolute_filename, line);
        if(map == null){
            return null;
        }
        String json = JSON.toJSONString(map);
        Event eventNew = new SimpleEvent();
        eventNew.setBody(json.getBytes());
        //业务代码
        return eventNew;
    }
    /**批量处理*/
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> listEvents = new ArrayList<>();
        events.forEach(event -> {
            Event eventNew = intercept(event);
            if (eventNew != null){
                listEvents.add(eventNew);
            }
        });
        return listEvents;
    }
    /**内部类构造拦截器*/
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ELKinterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
    /**初始化*/
    @Override
    public void initialize() {

    }
    @Override
    public void close() {

    }
}
