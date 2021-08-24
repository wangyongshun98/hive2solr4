package com.wys.hive.store;

import com.wys.hive.conf.Conf;
import com.wys.hive.format.SolrHiveInputFormat;
import com.wys.hive.format.SolrHiveOutputFormat;
import com.wys.hive.serde2.SolrSerde;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;

/**
 * StorageHandler封装了整个与自定义 外部表打交道
 * 的一切信息，便于管理所有的流程
 * @author IMZHANGJIE.CN
 */

public class SolrStorageHandler extends DefaultStorageHandler {

    final static Logger log= LoggerFactory.getLogger(SolrStorageHandler.class);

    @Override
    public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
        return SolrHiveInputFormat.class;
    }


    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {

        return SolrHiveOutputFormat.class;
}

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return SolrSerde.class;
    }




    @Override
    public void configureInputJobProperties(TableDesc tbl, Map<String, String> jobProperties) {
        final Properties properties = tbl.getProperties();
        //设置属性到运行时的jobconf里面
        Conf.copyProperties(properties,jobProperties);
    }


    @Override
    public void configureOutputJobProperties(TableDesc tbl, Map<String, String> jobProperties) {
        final Properties properties = tbl.getProperties();

        //设置属性到运行时的jobconf里面
        Conf.copyProperties(properties,jobProperties);

    }

    public void printMapInfo(Map<String,String> map){
        for(Map.Entry<String,String> kv:map.entrySet()){
            log.info(" 表meta： key:{}  ==>  value:{} ",kv.getKey(),kv.getValue());
        }
    }




}
