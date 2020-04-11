package com.cn.tz13.bigdata.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.io.File.separator;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-16:40
 */
public class FolderSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger LOG = Logger.getLogger(FolderSource.class);
    /**文件监控目录*/
    String folderDir ;
    /**文件处理成功存放的根目录*/
    String succDir ;
    /**每批最多处理多少文件*/
    private int fileNum;
    /**存放每批需要处理的文件集合*/
    private List<File> listFile;
    private List<Event> eventList;
    /**读取配置文件*/
    @Override
    public void configure(Context context) {
        eventList = new ArrayList<>();
        folderDir = context.getString("folderDir");
        succDir = context.getString("succDir");
        fileNum = context.getInteger("fileNum");
        LOG.info("初始化参数==========================" +
                "folderDir:" + folderDir+
                "succDir:" + succDir+
                "fileNum:" + fileNum);
    }
    /**业务代码*/
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        //睡2秒，方便看日志
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            LOG.error("===========Source被执行");
            //读取文件
            List<File> files = (List<File>)FileUtils.listFiles(new File(folderDir),
                    new String[]{"txt"},true);
            //文件截取
            int fileCount = files.size();
            if (fileCount > fileNum){
                listFile = files.subList(0, fileNum);
            }else {
                listFile = files;
            }
            if (listFile.size() > 0){
                //遍历读取文件
                for (int i = 0; i < listFile.size(); i++){
                    File file = listFile.get(i);
                    //原始文件名
                    String fileName = file.getName();
                    //定义新的文件目录
                    //文件处理成功存放的据对目录
                    String fileDirNew = succDir + separator + "2020-02-01";
                    //新文件的绝对路径文件名 = 新目录 + 原来的文件名
                    String fileNameNew = fileDirNew + fileName;
                    //如果文件存在
                    if (new File(fileNameNew).exists()){
                        //如果同名文件存在，不处理
                    }else {
                        //如果同名文件不存在，处理当前文件
                        List<String> lines = FileUtils.readLines(file);
                        lines.forEach(line ->{
                            Event e = new SimpleEvent();
                            e.setBody(line.getBytes());
                            //附加元数据到消息头
                            Map headers = new HashMap<String, String>();
                            headers.put("filename",fileName);
                            headers.put("absolute_filename",fileNameNew);
                            e.setHeaders(headers);
                            eventList.add(e);
                        });
                        //文件处理完成，将解析完成的文件移动到成功目录下
                        FileUtils.moveToDirectory(file, new File(fileDirNew), true);
                    }

                }
                getChannelProcessor().processEventBatch(eventList);
                LOG.info("批量推送数据到channel" +eventList.size() + "成功" );
                eventList.clear();
            }
            status = Status.READY;
        }catch (Exception e){
            status = Status.BACKOFF;
            // re-throw all Errors
            LOG.error(null,e);
        }finally {

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
