package com.week2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/5/25.
 * 汽车编码 的信息结构化
 */
public class Step2 extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        List<String> list = new ArrayList<String>();
        String outputPath= "/user/shangyongqiang/hbase/auto/";
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try {
            ToolRunner.run(conf, new Step2(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 测试job
     */

    static class TestJob1 {

        private static final Logger LOGGER = LogManager.getLogger(TestJob1.class);

        public static int test(Configuration conf, List<String> list, String output)
                throws IOException,
                ClassNotFoundException, InterruptedException {

//            conf.setBoolean(Job.USED_GENERIC_PARSER, true);

            String tablename = "t_auto_autoinfo2";
            conf.set("hbase.master","dmp06:16000");
            conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05");
            Job job = new Job(conf, "WordCountHbaseReader");
            job.setJarByClass(TestMapper1.class);
            //设置任务数据的输出路径；
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setReducerClass(Myreduce1.class);
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob(tablename, scan, TestMapper1.class, Text.class, Text.class, job);
            return job.waitForCompletion(true) ? 0 : 1;

        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends
            TableMapper<Text,Text> {

        @Override
        protected void map(ImmutableBytesWritable key,Result value,Context context)
                throws IOException, InterruptedException {

//anto_info2  00030104_10069_30480   网站编号_品牌编号(车ID)_具体编号(配置ID)
//dmp_info 0000006EEF56B7226842310AFBC09021_02_00010002_2016012809_HB   用户唯一标识_数据来源_url编号_访问时间(yyyyMMddHH)_省份
            /**
             *     String ip = Bytes.toString(row.get()).split("-")[0];
             String url = new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("url")));
             text.set(ip+"&"+url);
             context.write(text, ONE);
             */
            String[] uid_sce_url_time_pro= Bytes.toString(key.get()).split("\\_");
            String web=uid_sce_url_time_pro[0];
            String type=uid_sce_url_time_pro[1];
            String code=uid_sce_url_time_pro[2];
            k1.set(web+"_"+code);//具体到配置
            String w = web+"_"+type;//查看这一系列的

            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }
            String  text= map.get("auto_info:auto_name")+"\t"+map.get("auto_info:gearbox")+"\t"+map.get("auto_info:pqa")+"\t"+map.get("auto_info:manu_price")+"\t"+map.get("auto_info:size")+"\t"+map.get("auto_info:com_wear")+"\t"+map.get("auto_info:struct")+"\t"+map.get("auto_info:source")+"\t"+map.get("auto_info:oil_wear")+"\t"+map.get("auto_info:engine");

            if(web.startsWith("00030005")){
                //汽车之家
                String v=map.get("auto_info:auto_name").split("\\#")[0];
                context.write(new Text(w),new Text(v));
            }else if(web.startsWith("00030007")){
                //惠买车
                String v=map.get("auto_info:auto_name").split(" ")[0];
                context.write(new Text(w),new Text(v));
            }

            if(!"".equals(text)){
                context.write(k1, new Text(text));
            }

        }
    }

    /**
     * reduce任务
     */
    static class Myreduce1 extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //汽车的类别去重
            String v="";
            for(Text value :values){
               v=value.toString();
            }
            context.write(key,new Text(v));

        }
    }
}
