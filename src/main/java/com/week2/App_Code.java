package com.week2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
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
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shangyongqiang on 2016/5/26.
 */
public class App_Code extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
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
        return TestJob1.test(conf, st,et, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        if(args.length!=3){
            System.out.print("参数0输出路径,参数1是开始时间,参数2是结束时间");
            System.exit(0);
        }

        try {
//            if(ToolRunner.run(conf, new Info_Code(), args)==0){
                ToolRunner.run(conf, new App_Code(), args);
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

        public static int test(Configuration conf,Long st,Long et, String output)
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
            //指定时间区间来取数据
            scan.setTimeRange(st,et);
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
            String[] uid_sce_url_time_pro= Bytes.toString(key.get()).split("\\_");
            String uid=uid_sce_url_time_pro[0];
            String sce=uid_sce_url_time_pro[1];
            String url=uid_sce_url_time_pro[2];
            String time=uid_sce_url_time_pro[3];
            String pro=uid_sce_url_time_pro[4];
            Map<String,String> map = new HashMap<String,String>();
//            NavigableMap<byte[], byte[]> info = value.getFamilyMap(Bytes.toBytes("info"));
//
//            for (Map.Entry e:info.entrySet()){
//               e.getKey();
//                e.getValue();
//            }
            KeyValue[] kv = value.raw();
            //app编码
//            String text=url.substring(0, 8);

            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }
            String count=map.get("info:count");
            String k2=uid+"\t"+sce+"\t"+pro+"\t"+url+"\t"+time+"\t"+count;
            context.write(new Text(key.get()), new Text(k2));

        }
    }

}
