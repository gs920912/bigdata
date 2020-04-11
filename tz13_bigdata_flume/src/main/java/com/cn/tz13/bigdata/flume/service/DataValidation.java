package com.cn.tz13.bigdata.flume.service;
import com.cn.tz13.bigdata.flume.constant.ErrorMapFields;
import com.cn.tz13.bigdata.flume.sink.MySink;
import com.cn.tz13.bigdata.regex.Validation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cn.tz13.bigdata.flume.constant.ErrorMapFields.*;
/**
 * @author gs
 * @Description TODO
 * @date 2020/2/2-16:29
 */
public class DataValidation {
    private static final Logger LOG = Logger.getLogger(DataValidation.class);
    private static Map<String,String>  dataTypeMap;
    private static List<String> listAuthType;
    private static String isErrorES;
    private static final String USERNAME=ErrorMapFields.USERNAME;

    private static final String SJHM=ErrorMapFields.SJHM;
    private static final String SJHM_ERROR=ErrorMapFields.SJHM_ERROR;
    private static final String SJHM_ERRORCODE=ErrorMapFields.SJHM_ERRORCODE;

    private static final String QQ=ErrorMapFields.QQ;
    private static final String QQ_ERROR=ErrorMapFields.QQ_ERROR;
    private static final String QQ_ERRORCODE=ErrorMapFields.QQ_ERRORCODE;

    private static final String IMSI=ErrorMapFields.IMSI;
    private static final String IMSI_ERROR=ErrorMapFields.IMSI_ERROR;
    private static final String IMSI_ERRORCODE=ErrorMapFields.IMSI_ERRORCODE;

    private static final String IMEI=ErrorMapFields.IMEI;
    private static final String IMEI_ERROR=ErrorMapFields.IMEI_ERROR;
    private static final String IMEI_ERRORCODE=ErrorMapFields.IMEI_ERRORCODE;

    private static final String MAC=ErrorMapFields.MAC;
    private static final String CLIENTMAC=ErrorMapFields.CLIENTMAC;
    private static final String STATIONMAC=ErrorMapFields.STATIONMAC;
    private static final String BSSID=ErrorMapFields.BSSID;
    private static final String MAC_ERROR=ErrorMapFields.MAC_ERROR;
    private static final String MAC_ERRORCODE=ErrorMapFields.MAC_ERRORCODE;

    private static final String DEVICENUM=ErrorMapFields.DEVICENUM;
    private static final String DEVICENUM_ERROR=ErrorMapFields.DEVICENUM_ERROR;
    private static final String DEVICENUM_ERRORCODE=ErrorMapFields.DEVICENUM_ERRORCODE;

    private static final String CAPTURETIME=ErrorMapFields.CAPTURETIME;
    private static final String CAPTURETIME_ERROR=ErrorMapFields.CAPTURETIME_ERROR;
    private static final String CAPTURETIME_ERRORCODE=ErrorMapFields.CAPTURETIME_ERRORCODE;


    private static final String EMAIL=ErrorMapFields.EMAIL;
    private static final String EMAIL_ERROR=ErrorMapFields.EMAIL_ERROR;
    private static final String EMAIL_ERRORCODE=ErrorMapFields.EMAIL_ERRORCODE;

    private static final String AUTH_TYPE=ErrorMapFields.AUTH_TYPE;
    private static final String AUTH_TYPE_ERROR=ErrorMapFields.AUTH_TYPE_ERROR;
    private static final String AUTH_TYPE_ERRORCODE=ErrorMapFields.AUTH_TYPE_ERRORCODE;

    private static final String FIRM_CODE=ErrorMapFields.FIRM_CODE;
    private static final String FIRM_CODE_ERROR=ErrorMapFields.FIRM_CODE_ERROR;
    private static final String FIRM_CODE_ERRORCODE=ErrorMapFields.FIRM_CODE_ERRORCODE;

    private static final String STARTTIME=ErrorMapFields.STARTTIME;
    private static final String STARTTIME_ERROR=ErrorMapFields.STARTTIME_ERROR;
    private static final String STARTTIME_ERRORCODE=ErrorMapFields.STARTTIME_ERRORCODE;
    private static final String ENDTIME=ErrorMapFields.ENDTIME;
    private static final String ENDTIME_ERROR=ErrorMapFields.ENDTIME_ERROR;
    private static final String ENDTIME_ERRORCODE=ErrorMapFields.ENDTIME_ERRORCODE;


    private static final String LOGINTIME=ErrorMapFields.LOGINTIME;
    private static final String LOGINTIME_ERROR=ErrorMapFields.LOGINTIME_ERROR;
    private static final String LOGINTIME_ERRORCODE=ErrorMapFields.LOGINTIME_ERRORCODE;
    private static final String LOGOUTTIME=ErrorMapFields.LOGOUTTIME;
    private static final String LOGOUTTIME_ERROR=ErrorMapFields.LOGOUTTIME_ERROR;
    private static final String LOGOUTTIME_ERRORCODE=ErrorMapFields.LOGOUTTIME_ERRORCODE;

    /**
     * 数据清洗 校验
     * @param map
     * @return
     */
    public static Map<String,Object> dataValidation(Map<String,String> map){

        if(map == null){
            return null;
        }

        //存放错误信息
        Map<String,Object> errorMap = new HashMap<>();
        //一个字段值 一个字段值的 校验。  如果有2个字段相关联的，需要做关联校验
        //校验手机号码
        sjhmValidation(map,errorMap);
        //校验mac
        macValidation(map,errorMap);
        //校验经纬度
        longlaitValidation(map,errorMap);
        //定义自己的清洗规则
        //TODO 大小写统一
        //TODO 时间类型统一
        //TODO 数据字段统一
        //TODO 业务字段转换
        //TODO 数据矫正
        //TODO 验证MAC不能为空
        //TODO 验证IMSI不能为空
        //TODO 验证 QQ IMSI IMEI
        //TODO 验证DEVICENUM是否为空 为空返回错误
        //devicenumValidation(map,errorMap);
        //TODO 验证CAPTURETIME是否为空 为空过滤   不为10，14位数字过滤
        //capturetimeValidation(map,errorMap);
        //TODO 验证EMAIL
        //emailValidation(map,errorMap);
        //TODO 验证STARTTIME ENDTIME LOGINTIME LOGOUTTIME
        //timeValidation(map,errorMap);
        return errorMap;
    }


    /**
     * 手机号码校验
     * @param map
     * @param errorMap
     */
    public static void sjhmValidation(Map<String,String> map, Map<String,Object> errorMap){
        if(map.containsKey("phone")){
            String sjhm = map.get("phone");
            //正则验证
            boolean isMobile = Validation.isMobile(sjhm);
            if(!isMobile){
                LOG.error("==手机号码格式不对，格式为" + sjhm);
                errorMap.put("SJHM",sjhm);
                errorMap.put("SJHM_ERROR","手机号码格式不对");
            }

        }
    }

    //MAC验证  10003
    public static void macValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey("phone_mac")){
            String mac=map.get("phone_mac");
            if(StringUtils.isNotBlank(mac)){
                boolean bool = Validation.isMac(mac);
                if(!bool){
                    errorMap.put(MAC,mac);
                    errorMap.put(MAC_ERROR,MAC_ERRORCODE);
                }
            }else{
                LOG.info("MAC验证失败");
                errorMap.put(MAC,mac);
                errorMap.put(MAC_ERROR,MAC_ERRORCODE);
            }
        }
    }


    /**
     * 经纬度验证  错误过滤  10012  10013
     * @param map
     * @param errorMap
     */
    public static void longlaitValidation( Map<String, String> map,Map<String,Object> errorMap){
        if(map == null){
            return ;
        }
        if(map.containsKey(LONGITUDE)&&map.containsKey(LATITUDE)){
            String longitude=map.get(LONGITUDE);
            String latitude=map.get(LATITUDE);

            boolean bool1 = Validation.isLONGITUDE(longitude);
            boolean bool2 = Validation.isLATITUDE(latitude);

            if(!bool1){
                errorMap.put(LONGITUDE,longitude);
                errorMap.put(LONGITUDE_ERROR,LONGITUDE_ERRORCODE);
            }
            if(!bool2){
                errorMap.put(LATITUDE,latitude);
                errorMap.put(LATITUDE_ERROR,LATITUDE_ERRORCODE);
            }
        }
    }
}
