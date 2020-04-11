package com.cn.tz13.bigdata.flume.sink;

import com.cn.tz13.bigdata.kafka.producer.StringProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-16:05
 */
public class MySink extends AbstractSink implements Configurable {
    private String myProp;

    private static final Logger LOG = Logger.getLogger(MySink.class);

    //kafka topic
    private String topic;

    @Override
    public void configure(Context context) {
        topic = context.getString("topic");
    }

    @Override
    public void start() {
    }

    @Override
    public void stop () {
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        LOG.error("===" + "");
        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do
            Event event = ch.take();
            if(event == null){
                txn.rollback();
                return Status.BACKOFF;
            }
            String line = new String(event.getBody());
            //TODO 数据已经拿到，直接往kafka 里面写入就可以了，缺一个写kakfa的API
            StringProducer.producer(topic,line);
            LOG.error("===" + line);
            // Sen  d the Event to the external repository.
            // storeSomeData(e);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }
}
