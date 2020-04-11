package com.cn.tz13.bigdata.spark.warn.dao;

import com.cn.tz13.bigdata.db.DBCommon;
import com.cn.tz13.bigdata.spark.common.convert.BaseDataConvert;
import com.cn.tz13.bigdata.spark.warn.domain.TZ_RuleDomain;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author gs
 * @Description TODO
 * @date 2020/2/4-19:18
 */
public class TZ_ruleDao {
    private static final Logger LOG = LoggerFactory.getLogger(TZ_ruleDao.class);
    public static List<TZ_RuleDomain> getRuleList(){
        List<TZ_RuleDomain> listRules = null;
        //获取mysql连接
        Connection conn =DBCommon.getConn("test");
        QueryRunner query = new QueryRunner();
        String sql = "select * from tz_rule";
        try {
            listRules = query.query(conn,sql,new BeanListHandler<>(TZ_RuleDomain.class));
        }catch (SQLException e){
            LOG.error(null,e);
        }finally {
            DBCommon.close(conn);

        }
        return listRules;
    }

    public static void main(String[] args) {
        List<TZ_RuleDomain> ruleList = TZ_ruleDao.getRuleList();
        ruleList.forEach(rule->{
            System.out.println(rule.getId());
            System.out.println(rule.getPublisher());
        });
    }
}
