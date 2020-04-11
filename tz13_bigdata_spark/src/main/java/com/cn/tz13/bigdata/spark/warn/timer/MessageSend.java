package com.cn.tz13.bigdata.spark.warn.timer;

/**
 * @author gs
 * @Description 模拟第三方接口
 * @date 2020/2/4-20:13
 */
public class MessageSend {
    /**
     * 模拟短信接口
     * @param phone         手机号
     * @param sendInfo      内容
     */
    public static void sendMessage(String phone, String sendInfo) {
        System.out.println("发送短信到手机号为" + phone);
        System.out.println("发送内容为=======》" + sendInfo);
    }
    /**
     * 模拟钉钉
     * @param didi        手机号
     * @param sendInfo      内容
     */
    public static void sendDingDingMessage(String didi,String sendInfo){
        System.out.println("发送消息到钉钉号为" + didi);
        System.out.println("发送内容为=======》" + sendInfo);
    }
}
