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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/5/25.
 * 汽车编码
 */
public class Car_Code1 extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        List<String> list = new ArrayList<String>();
        String outputPath= "/user/shangyongqiang/hbase/auto2/";
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try {
            ToolRunner.run(conf, new Car_Code1(), args);
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

            String tablename = "t_auto_autoinfo";
            conf.set("hbase.master","dmp06:16000");
            conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05");
            Job job = new Job(conf, "CarHbaseReader");
            //需要分区,不同类型的格式不一样分区到不同分区中
//            job.setNumReduceTasks(5);
//            job.setPartitionerClass(Pt.class);
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
            String ke=Bytes.toString(key.get());
            String[] uid_sce_url_time_pro= ke.split("\\_");
            String web=uid_sce_url_time_pro[0];
            String type=uid_sce_url_time_pro[1];
            String code=uid_sce_url_time_pro[2];
            k1.set(web+"_"+code);//具体到配置
            String w = web+"_"+type;//查看这一系列的
/**
 *
 auto_info:fuel
 auto_info:gearbox
 auto_info:name
 auto_info:pqa
 auto_info:price
 auto_info:size
 auto_info:speed
 auto_info:style
 auto_info:engine
 auto_info:speed
 auto_info:struct
 *
 */
            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v=new String(r.getValue());
//                if(k.equals("auto_info:manu_price") || k.equals("auto_info:source") ){
//                    DataInputStream dis=new DataInputStream(new ByteArrayInputStream(r.getValue()));
//                    float f=dis.readFloat();
//                    dis.close();
//                     v=String.valueOf(f);
//                }else{
//                    v= new String(r.getValue());
//                }
                map.put(k,v);
            }
            String  text= map.get("auto_info:fuel")+"\t"+map.get("auto_info:gearbox")+"\t"+map.get("auto_info:name")+"\t"+map.get("auto_info:pqa")+"\t"+map.get("auto_info:price")+"\t"+map.get("auto_info:size")+"\t"+map.get("auto_info:level")+"\t"+map.get("auto_info:style")+"\t"+map.get("auto_info:engine")+"\t"+map.get("auto_info:speed")+"\t"+map.get("auto_info:struct");
            text+="\t"+ke;
//            if(web.startsWith("00030005")){
//                //汽车之家
//
//                context.write(new Text(w),new Text(v));
//            }else if(web.startsWith("00030007")){
//                //惠买车
//                String v=map.get("auto_info:auto_name");
//                int a=v.lastIndexOf("#");
//                v= v.substring(0,a);
//                context.write(new Text(w),new Text(v));
//            }
            context.write(new Text(w),new Text(text));
            if(!"".equals(text)){
                context.write(k1, new Text(text));
            }

//StringBuilder sb = new StringBuilder();
//            for(Map.Entry<byte[],byte[]> entry:value.getFamilyMap("content".getBytes()).entrySet()){
//                String str =  new String(entry.getValue());
//                //将字节数组转换为String类型
//                if(str != null){
//                    sb.append(new String(entry.getKey()));
//                    sb.append(":");
//                    sb.append(str);
//                }
//                context.write(new Text(key.get()), new Text(new String(sb)));
//            }
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
