package com.sexAge;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
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
 *
 *
 *
 * 新闻 dmp_info
 列名  含义
 channel 频道代码（同一个人同一时间同一url有多个channel时用^隔开）
 bsid  电信基站标识
 count 浏览该频道次数（与频道标签一一对应，多个时用^隔开）


 新闻频道信息表
 （1） rowkey结构：新闻网站编号_频道id
 （2） t_channel_baseinfo：
 CLOUMN FAMILY 字段信息
 列族名称  备注  字段名称  备注
 channel_info  频道信息  channel_name  频道名称
 *
 *
 *
 */
public class News_Code extends Configured implements Tool {
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
                ToolRunner.run(conf, new News_Code(), args);
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
            Job job = new Job(conf, "newscode");
            job.setJarByClass(TestJob1.class);
            job.setMapperClass(TestMapper1.class);
            job.setReducerClass(Myreduce1.class);
            //设置任务数据的输出路径；
            FileOutputFormat.setOutputPath(job, new Path(output));
//            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            List<Scan> scans = new ArrayList();
            //读取t_channel_baseinfo来获取子频道id
            Scan scan1 =new Scan();
            scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("t_channel_baseinfo"));
            scans.add(scan1);

            //读取dmp_info中的关于新闻的上网行为来进行表连接
            Scan scan = new Scan();
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("dmp_info"));
            //指定时间区间来取数
            scan.setTimeRange(st,et);
            QualifierFilter ff = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("channel")));
            scan.setFilter(ff);
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
//        private static final Logger LOGGER = LogManager.getLogger(TestMapper1.class);
//
//        private String flag;
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            FileSplit split = (FileSplit) context.getInputSplit();
//            flag = split.getPath().getName();// 判断读的数据集
//            LOGGER.info(flag);
//        }

        @Override
        protected void map(ImmutableBytesWritable key,Result value,Context context)
                throws IOException, InterruptedException {
            String[] uid_sce_url_time_pro= Bytes.toString(key.get()).split("\\_");
            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }
            String v="";
            if(uid_sce_url_time_pro.length==5){
                //属于用户新闻上网的数据
//000098E13752F246259BFEB95BEDD227_01_00130047001_2016050702_SH  *******info:channel******info:count
                String uid=uid_sce_url_time_pro[0];
                String sce=uid_sce_url_time_pro[1];
                String url=uid_sce_url_time_pro[2];
                String time=uid_sce_url_time_pro[3];
                String pro=uid_sce_url_time_pro[4];
                List<String> ch = new ArrayList();
                List<String> co = new ArrayList();
                List<String> bs = new ArrayList();
                Map<String,String> map1  = new HashMap();
                //由于频道存在多个用^隔开.所以分开多个来操作
                if(map.get("info:channel")!=null){
                    String[] chans = map.get("info:channel").split("\\^");
                    ch= Arrays.asList(chans);
                }
                if(map.get("info:count")!=null){
                    String[] cons = map.get("info:count").split("\\^");
                    co= Arrays.asList(cons);
                }
                if(map.get("info:bsid")!=null){
                    String[] cons = map.get("info:bsid").split("\\^");
                    bs= Arrays.asList(cons);
                }
                for (int i = 0; i <ch.size() ; i++) {
                    map1.put(ch.get(i), co.size()==0? "1":co.get(i));
                }
                for(String a :map1.keySet()){
                    String pindao=url+"_"+a;
                    String count= map1.get(a);
                    k1.set(pindao);
                    v="0"+Bytes.toString(key.get())+"\t"+count;
                    context.write(k1, new Text(v));
                }

            }else {
                //属于频道信息的数据  00130092001_3395190664   ******* channel_info:channel_name
                k1.set(key.get());
                v = "1"+map.get("channel_info:channel_name");
                context.write(k1, new Text(v));
            }

        }
    }

    /**
     * reduce任务
     */

    static class Myreduce1 extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //进行表连接
            String name="";
            String k="";
            String v1="";
            String lei =key.toString().substring(0,8);
            switch (lei){
                case "00130087": v1="网易新闻";break;
                case "00130047": v1="搜狐新闻";break;
                case "00130088": v1="腾讯新闻";break;
                case "00130089": v1="凤凰新闻";break;
                case "00130090": v1="新浪新闻";break;
                case "00130091": v1="人民新闻";break;
                case "00130048": v1="今日头条";break;
                case "00130092": v1="一点资讯";break;
                case "00130093": v1="天天快报";break;
                case "00130049": v1="Zaker";break;
            }
            List<String> list =new ArrayList<String>();
            List<String> list1 =new ArrayList<String>();
            for(Text value :values){
                String v =value.toString();
               if(v.startsWith("0")){
                   list.add(v.substring(1));
               }else{
                   list1.add(v.substring(1));
               }
            }
            for(String a :list){
                for(String b:list1){
                    //判断新闻app
                  context.write(new Text(b),new Text(key.toString()+"\t"+a+"\t"+v1));
                }
            }

        }
    }

}
