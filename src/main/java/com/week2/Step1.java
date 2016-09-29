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
 * Created by shangyongqiang on 2016/5/26.
 * 将dmp_info中的信息结构化
 */
public class Step1 extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        List<String> list = new ArrayList<String>();
       String outputPath= "/user/shangyongqiang/hbase/info/";
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try {
//            if(ToolRunner.run(conf, new Info_Code(), args)==0){
                ToolRunner.run(conf, new Step1(), args);
//            }

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

            String tablename = "dmp_info";
            conf.set("hbase.master","dmp06:16000");
            conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05");
            Job job = new Job(conf, "HbaseReader");
            job.setJarByClass(TestMapper1.class);
            //设置任务数据的输出路径；
            FileOutputFormat.setOutputPath(job, new Path(output));
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

//dmp_info 0000006EEF56B7226842310AFBC09021_02_00010002_2016012809_HB   用户唯一标识_数据来源_url编号_访问时间(yyyyMMddHH)_省份
//anto_info2  00030005_1006_14665   网站编号_品牌编号(车ID)_具体编号(配置ID)
            String[] uid_sce_url_time_pro= Bytes.toString(key.get()).split("\\_");
            String uid=uid_sce_url_time_pro[0];
            String sce=uid_sce_url_time_pro[1];
            String url=uid_sce_url_time_pro[2];
            String time=uid_sce_url_time_pro[3];
            String pro=uid_sce_url_time_pro[4];
            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            String text="";
            String k2="";
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }
            //汽车00030006000
            if(url.startsWith("0003")){
                //auto city  bsid   count 可能有多值以^隔开
                k2=Bytes.toString(key.get());
                if(map.get("info:auto")!=null ){
                    String[] autos = map.get("info:auto").split("\\^");
                   Map<Integer,String> countMap = new HashMap();
                   Map<Integer,String> cityMap = new HashMap();
                   Map<Integer,String> bsidMap = new HashMap();
                    Map<String,Integer> disMap = new HashMap();
                    if( map.get("info:count")!=null){
                        String[] count =  map.get("info:count").split("\\^");
                        for (int i = 0; i < count.length; i++) {
                            countMap.put(i,count[i]);
                        }
                    }
                    if( map.get("info:city")!=null){
                        String[] count =  map.get("info:city").split("\\^");
                        for (int i = 0; i < count.length; i++) {
                            cityMap.put(i,count[i]);
                        }
                    }
                    if( map.get("info:bsid")!=null){
                        String[] count =  map.get("info:bsid").split("\\^");
                        for (int i = 0; i < count.length; i++) {
                            cityMap.put(i,count[i]);
                        }
                    }

                        for (int i = 0; i < autos.length; i++) {
                            String autoCode,cityCode;
                            if(url.startsWith("00030005")){
                                 autoCode =  url.substring(0, 8)+"_"+autos[i];
                                 cityCode = cityMap.get(i)==null?null:(url.substring(0, 8)+"_"+cityMap.get(i));
                            }else{
                                 autoCode =  "00030007_"+autos[i];
                                 cityCode = cityMap.get(i)==null?null:("00030006_"+cityMap.get(i));
                            }
                            String count = countMap.get(i);
                            String bsid =bsidMap.get(i);
                            text = uid + "\t" + url + "\t"+sce + "\t" + time + "\t" + pro + "\t" + autoCode + "\t" + count + "\t" + cityCode + "\t" + bsid;
                            //用map去重
                            disMap.put(text,1);
                        }
                    for(String t:disMap.keySet()){
                        k1.set(k2);
                            if (!"".equals(text)) {
                                context.write(k1, new Text(t));
                            }
                    }

                }else{
                        text = uid + "\t" + url + "\t"+ sce + "\t" + time + "\t" + pro + "\t" + map.get("info:auto") + "\t" + map.get("info:count") + "\t" + map.get("info:city") + "\t" + map.get("info:bsid");
                        k1.set(k2);
                        if (!"".equals(text)) {
                            context.write(k1, new Text(text));
                        }
                }
            }

        }
    }

}
