package com.cn.tz13.bigdata.flume.source;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.io.File.separator;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/1-16:39
 */
public class FolderSourceTest {
    public static void main(String[] args) {
        String folderDir = "E:\\BaiduNetdiskDownload\\测试数据\\search";
        //读取文件
        Collection<File> files = FileUtils.listFiles(new File(folderDir),
                new String[]{"txt"}, true);
        if (files.size()>0){
            //遍历读取文件
            files.forEach(file -> {
                String fileName = file.getName();
                //解析文件，解析过程中要把文件进行备份
                String succPath = "E:\\BaiduNetdiskDownload\\测试数据\\search";
                //定义新的文件目录
                String fileDirNew = succPath + separator + "2020-02-01";
                System.out.println("==" + fileDirNew);
                //新文件的绝对路径文件名 = 新目录 + 原来的文件名
                String fileNameNew = fileDirNew + fileName;
                try {
                    //如果文件存在
                    if(new File(fileNameNew).exists()){
                        //如果同名文件存在，不处理
                    }else{
                        //如果同名文件不存在，处理当前文件
                        List<String> lines = FileUtils.readLines(file);
                        lines.forEach(line->{
                            System.out.println(line);
                        });
                        //文件处理完成，将解析完成的文件移动到成功目录下
                        FileUtils.moveToDirectory(file,new File(fileDirNew),true);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

    }
}
