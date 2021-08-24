package com.wys.hive.reader;

import com.wys.hive.conf.Conf;
import com.wys.hive.util.ZipUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * hive写数据到指定的solr中
 * @author IMZHANGJIE.CN
 */
public class SolrHiveWriter implements FileSinkOperator.RecordWriter {
    final static Logger log= LoggerFactory.getLogger(SolrHiveWriter.class);
    SolrServer sc =null;
    int batchSize;
    SystemDefaultHttpClient httpClient = new SystemDefaultHttpClient();
    //批量插入
    List<SolrInputDocument> datas=new ArrayList<SolrInputDocument>();

//    AtomicInteger count=new AtomicInteger();
    LBHttpSolrServer lbHttpSolrClient=new LBHttpSolrServer(httpClient);

    public SolrHiveWriter(JobConf conf){

        //初始化solrclient// 必须在这里初始化，否则，提取到一个公用类里面初始化会报错，因为MR的进程JVM和客户端的类是独立的
        if(conf.get(Conf.IS_SOLRCLOUD).equals("1")) {
            //solrcloud模式
            sc=new CloudSolrServer(conf.get(Conf.SOLR_URL).trim(),lbHttpSolrClient);//设置Cloud的client
            ((CloudSolrServer)sc).setDefaultCollection(conf.get(Conf.COLLECTION_NAME));//设置集合名
//            ((CloudSolrClient)sc).setParallelUpdates(false);//取消并行更新
        }else{
            //普通模式
            sc = new HttpSolrServer(conf.get(Conf.SOLR_URL),httpClient);
        }
        this.batchSize=Integer.parseInt(conf.get(Conf.SOLR_CURSOR_BATCH_SIZE));
        log.info("批处理提交数量：{}",batchSize);
    }

    @Override
    public void write(Writable w) throws IOException {
        MapWritable map = (MapWritable) w;
        SolrInputDocument doc = new SolrInputDocument();
        for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
            String key = entry.getKey().toString();//得到key
            String value = entry.getValue().toString().trim();// null值会转成空字符串 得到value
            
            if(key.startsWith("content") || key.startsWith("text")){//如果是富文本字段，去掉html标签再创建索引
            	
            	//如果需要限制长度，可以在内容前面加上___LIMIT___250___LIMIT___
                boolean limit = false;
                int limitValue = 0;
                if(value.startsWith("___LIMIT___")){
                	limit = true;
                	limitValue = Integer.parseInt(value.substring("___LIMIT___".length(), value.lastIndexOf("___LIMIT___")));
                	value = value.replace("___LIMIT___"+limitValue+"___LIMIT___", "");
                }            	
            	
            	if(value.startsWith("gzip:")){//如果是gzip开头的说明文本已经压缩了
            		value = value.substring(5,value.length());
            		value = ZipUtils.gunzip(value);
            	}
            	//去掉html中的标签和代码标签
            	value = value.replaceAll("<i>([\\s\\S]*?)</i>|<pre>([\\s\\S]*?)</pre>", "").replaceAll("</?[^>]+>", "").replace("&nbsp;", " ").replace("nbsp;", " ");
            	if(limit){
            		value = value.substring(0, value.length() > limitValue ? limitValue : value.length());
            	}
            }
            
            //只有value有值的数据，我们才推送到solr里面，无值数据，不再发送到solr里面
            if(value.startsWith("_array_")){//如果是_array_开头的数据，则是multivalued数据
	             value = value.replace("_array_", "");
	             if(StringUtils.isEmpty(value)){
	            	 continue;
	             }
	        	 String[] sl = value.split(",");  //即把hive输入的数据通过空格分隔，切成数组（hive的sql只要concact即可）      
	             List<String> valuesl = java.util.Arrays.asList(sl);
	             //log.info("add entry value lists:" + valuesl);
	             for(String vl :valuesl){
	                doc.addField(key,vl); //改为调用addFiled的方法，防止覆盖
	             }                	
            }else{
            		doc.setField(key,value);
            }
        }
        
        datas.add(doc);

        //批量处理，大于等于一定量提交
        if(datas.size()==batchSize){
            try {
                sc.add(datas);
                sc.commit();    //不提交，等待flush,如果batchSize大于默认值则每次执行这个操作相当于commit
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                //清空集合数据
                datas.clear();
            }
        }

    }

    @Override
    public void close(boolean abort) throws IOException {
        try {
            //关闭资源再次提交索引
//            if(sc==null||datas==null){
//                log.info("sc 在close 是null");
//                System.out.println("sc 在close 是null");
//            }
            //关闭前，再次追加索引
        	if(datas != null && datas.size() != 0){
        		sc.add(datas);
        	}
            sc.commit();

//            log.info("Map结束，提交完毕，总共计数：{}",count.get());
//            sc.commit();
            sc.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
