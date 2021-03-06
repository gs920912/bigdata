package com.cn.tz13.bigdata.spark.warn.timer;

import com.cn.tz13.bigdata.spark.warn.domain.WarningMessage;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-20:18
 */
public class PhoneWarnImpl implements WarnI {
    @Override
    public boolean warn(WarningMessage warningMessage) {
        //对接第三方的接口，对接短信接口。 API接口
        //获取手机号
        String[] phones = warningMessage.getSendMobile().split(",");
        //获取告警内容
        String senfInfo = warningMessage.getSenfInfo();
        for (int i = 0; i < phones.length; i++) {
            String phone = phones[i];
            MessageSend.sendMessage(phone,senfInfo);
        }
        return false;
    }
}
