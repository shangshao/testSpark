package com.testsolr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;
/**
 * Created by shangyongqiang on 2016/9/12.
 */

public class TestSolr extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if(args.length==0 || args.length>3){
            usage();
        }
        return TestJob1.test(args);
    }
    private static void usage(){
        System.out.println("输入参数:<配置文件路径><起始行><结束行>");
        System.exit(1);
    }
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();

        try {
            ToolRunner.run(conf, new TestSolr(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 测试job
     */

    static class TestJob1 {

        private static final Logger LOGGER = LogManager.getLogger(TestJob1.class);
        private static Configuration conf;
        private static void createHBaseConfiguration(String loaction) {
            ConfigProperties pro = new ConfigProperties(loaction);
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",pro.getHBASE_ZOOKEEPER_QUORUM());
            conf.set("hbase.zookeeper.property.clientProt",pro.getHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT());
            conf.set("hbase.rootdir",pro.getHBASE_ROOTDIR());
            conf.set("hbase.master",pro.getHBASE_MASTER());
            conf.set("solr.server",pro.getSOLR_SERVER());
        }

        public static int test(String [] args)
                throws IOException,
                ClassNotFoundException, InterruptedException {
            createHBaseConfiguration(args[0]);
            ConfigProperties pro = new ConfigProperties(args[0]);
            String tname = pro.getHBASE_TABLE_NAME();
            String cf = pro.getHBASE_TABLE_FAMILY();
            Job job = new Job(conf,"SolrHbaseIndex");
            job.setJarByClass(TestSolr.class);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(args[1]));
            scan.setStopRow(Bytes.toBytes(args[2]));
            scan.addFamily(Bytes.toBytes(cf));
            scan.setCaching(500);
            scan.setCacheBlocks(false);
            //创建map任务
          TableMapReduceUtil.initTableMapperJob(tname, scan, SolrMapper.class, Text.class, Text.class, job);
            //不需要输出
            job.setOutputFormatClass(NullOutputFormat.class);
            return job.waitForCompletion(true) ? 0 : 1;

        }
    }

    /**
     * map任务
     */
    static class SolrMapper extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key,Result value,Context context)
                throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
            HttpSolrServer solrServer = new HttpSolrServer(conf.get("solr.server"));
            solrServer.setDefaultMaxConnectionsPerHost(100);
            solrServer.setMaxTotalConnections(1000);
            solrServer.setSoTimeout(20000);
            solrServer.setConnectionTimeout(20000);
            SolrInputDocument sol = new SolrInputDocument();
            sol.addField("rowkey",new String(value.getRow()));
            for (KeyValue keyValue : value.list()) {
                String fieldName = new String(keyValue.getQualifier());
                String fieldvalue = new String (keyValue.getValue());
                //需要建立索引的列
                if (fieldName.equalsIgnoreCase("account_name")
                        || fieldName.equalsIgnoreCase("article_title")
                        || fieldName.equalsIgnoreCase("article_content")) {
                    sol.addField(fieldName, fieldvalue);
                }
            }
            try {
                solrServer.add(sol);
                solrServer.commit(true,true,true);
            } catch (SolrServerException e) {
                System.err.println("更新Solr索引异常:" + new String(value.getRow()));
            }


        }
    }
}















