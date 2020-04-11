package com.cn.tz13.bigdata.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-16:15
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String myProp;
    /**读取配置文件 flume.conf*/
    @Override
    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");
        this.myProp = myProp;
    }
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data
            Event e = new SimpleEvent();

            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {

        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
