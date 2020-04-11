package com.cn.tz13.bigdata.spark.warn.timer;

import com.cn.tz13.bigdata.spark.warn.domain.WarningMessage;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-19:45
 */
public interface WarnI {
    boolean warn(WarningMessage warningMessage);
}
