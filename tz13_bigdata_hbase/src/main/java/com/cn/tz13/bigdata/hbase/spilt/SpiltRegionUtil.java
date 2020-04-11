package com.cn.tz13.bigdata.hbase.spilt;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;
/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-20:25
 */
public class SpiltRegionUtil {
    /**
     * 定义分区
     * @return
     */
    public static byte[][] getSplitKeysBydinct() {

        String[] keys = new String[]{"1","2", "3","4", "5","6", "7","8", "9","a","b", "c","d","e","f"};
        //String[] keys = new String[]{"10|", "20|", "30|", "40|", "50|", "60|", "70|", "80|", "90|"};
        byte[][] splitKeys = new byte[keys.length][];

        /**通过treeset排序  升序排序*/
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }



    /**
     * 定义分区
     * @return
     */
    public static byte[][] getSplitKeysByNumber() {

        String[] keys = new String[]{"10|", "11|", "12|", "13|", "14|", "15|", "16|", "17|", "18|", "19|"};
        byte[][] splitKeys = new byte[keys.length][];
        /**通过treeset排序  升序排序*/
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

}
