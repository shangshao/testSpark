package com.job1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shangyongqiang on 2016/6/29.
 */
public class User_Car_Code extends Configured implements Tool {
    public static Text k1 = new Text();
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //开始时间args[0],结束时间args[1]
        String starttime=args[1];
        String endtime=args[2];
        //把时间转化为时间戳
        SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
        long st = s.parse(starttime).getTime();
        long et = s.parse(endtime).getTime();
       String outputPath= args[0];
        return TestJob1.test(conf, st, et, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        if(args.length!=3){
            System.out.print("参数0输出路径,参数1是开始时间,参数2是结束时间");
            System.exit(0);
        }

        try {
//            if(ToolRunner.run(conf, new Info_Code(), args)==0){
                ToolRunner.run(conf, new User_Car_Code(), args);
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 读取hbase的job
     */

    static class TestJob1 {

        public static int test(Configuration conf,Long st,Long et, String output)
                throws IOException,
                ClassNotFoundException, InterruptedException {
            conf.set("hbase.master","dmp06:16000");
            conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05");
            Job job = new Job(conf, "carcode");
            job.setJarByClass(TestJob1.class);
            job.setMapperClass(TestMapper1.class);
            //设置任务数据的输出路径；
            FileOutputFormat.setOutputPath(job, new Path(output));
//            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            List<Scan> scans = new ArrayList();
//            //读取t_channel_baseinfo来获取子频道id
//            Scan scan1 =new Scan();
//            scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("t_channel_baseinfo"));
//            scans.add(scan1);

            //读取dmp_info中的关于新闻的上网行为来进行表连接
            Scan scan = new Scan();
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("dmp_info"));
            //指定时间区间来取数
            scan.setTimeRange(st,et);
//            RowFilter r = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("0003"));
//            QualifierFilter ff = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("auto")));
//            scan.setFilter(ff);
            scans.add(scan);
            TableMapReduceUtil.initTableMapperJob(scans, TestMapper1.class, Text.class, Text.class, job);
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
            String k2=Bytes.toString(key.get());
            String[] uid_sce_url_time_pro= k2.split("\\_");
            String uid=uid_sce_url_time_pro[0];
            String sce=uid_sce_url_time_pro[1];
            String url=uid_sce_url_time_pro[2];
            String time=uid_sce_url_time_pro[3];
            String pro=uid_sce_url_time_pro[4];
            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }
            String text="";
            //auto city  bsid   count 可能有多值以^隔开
            if(url.startsWith("0003")){
            if(map.get("info:auto")!=null ){
                String[] autos = map.get("info:auto").split("\\^");
                Map<Integer,String> countMap = new HashMap();
                Map<Integer,String> cityMap = new HashMap();
                Map<Integer,String> bsidMap = new HashMap();
                if( map.get("info:count")!=null){
                    String[] count =  map.get("info:count").split("\\^");
                    for (int i = 0; i < count.length; i++) {
                        countMap.put(i, count[i]==null?"1":count[i]);
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
                        bsidMap.put(i,count[i]);
                    }
                }
                for (int i = 0; i < autos.length; i++) {
//汽车之家的汽车code是hbase中的~~~~~易车,汽车报价大全,惠买车的汽车code是一样的
//                    String autoCode,cityCode;
//                    if(url.startsWith("00030005")){
//                        autoCode =  url.substring(0, 8)+"_"+autos[i];
//                        cityCode = cityMap.get(i)==null?null:(url.substring(0, 8)+"_"+cityMap.get(i));
//                    }else{
//                        autoCode =  "00030007_"+autos[i];
//                        cityCode = cityMap.get(i)==null?null:("00030006_"+cityMap.get(i));
//                    }
//



                    String autoCode =  url.substring(0, 8)+"_"+autos[i];
                    String cityCode = cityMap.get(i)==null?null:(url.substring(0, 8)+"_"+cityMap.get(i));
                    String count = countMap.get(i);
                    String bsid = bsidMap.get(i);
                    text = uid + "\t" + url + "\t"+sce + "\t" + time + "\t" + pro + "\t" + autoCode + "\t" + count + "\t" + cityCode + "\t" +bsid;

                    k1.set(k2);
                    if (!"".equals(text)) {
                        context.write(k1, new Text(text));
                    }
                }
            }else{
                text = uid + "\t" + url + "\t"+ sce + "\t" + time + "\t" + pro + "\t" + map.get("info:auto") + "\t" + map.get("info:count") + "\t" + map.get("info:city") + "\t" + map.get("info:bsid");
                k1.set(k2);
                if (!"".equals(text)) {
                    context.write(k1, new Text(text));
                }
            }

        }}
    }



}
