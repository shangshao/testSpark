package com.week2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by shangyongqiang on 2016/5/30.
 *
 * hive表中user_property中取得
 */
public class StaticticsApp extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String path1 = args[0];//起始日期
        String path2 = args[1];//结束日期
        String outpath =args[2]+"type";//输出目录
        List<String> list= new ArrayList();
        //本地测试目录是hdfs上的目录
//        String inputPathsStr = "hdfs://dmp01:9000/user/hive/basedata/liantong/2015121411";
//        List<String> list = DateUtil.getlist(ding, path1, path2);
        return TestJob1.test(conf, list, outpath);
    }

    public static void main(String[] args) {
        Configuration conf= new Configuration();
        try {

                ToolRunner.run(conf, new StaticticsApp(), args) ;

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

            //conf.set("mapreduce.job.queuename", "ven51");
            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            Job job = Job.getInstance(conf, "Statistics_Moblie");

            //本地测试
//            conf.set("fs.defaultFS", "hdfs://dmp01:9000");
//            conf.set("yarn.resourcemanager.hostname", "dmp01");

            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置map的输出key和value的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapperClass(TestMapper1.class);
            //job.setInputFormatClass(SequenceFileInputFormat.class);
            //  FileInputFormat.setInputPathFilter(job, MapReducePathFilter.class);
            FileInputFormat.setInputDirRecursive(job, true);

            FileSystem fileSystem = FileSystem.get(conf);
            for (int i=0;i<list.size();i++) {
                if(fileSystem.exists(new Path(list.get(i)))) {
                    FileInputFormat.addInputPath(job, new Path(list.get(i)));
                }
            }
            if (fileSystem.exists(new Path(output))) {
                fileSystem.delete(new Path(output), true);
            }
            job.setReducerClass(Myreduce1.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Map<String, String> types = new HashMap<String, String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //读入手机型号做匹配
            InputStream inputStream = StaticticsApp.class.getResourceAsStream("/moblietype.txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String linedata;
            while ((linedata = bufferedReader.readLine()) != null) {
                String[] a= linedata.split("\\t");
                types.put(a[0], a[1]);
            }
            bufferedReader.close();
            inputStream.close();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String attrs = value.toString();
            String[] Uid = attrs.split("\\u0001");
            String phone_type=Uid[3];
            String imei = Uid[9];
            if(imei!="null" && phone_type!="null" ) {
//                if(types.containsKey(Uid[3])){
//                    String k = Uid[9] + "\t" + Uid[3];
//                    k1.set(k);
//                    context.write(k1, a);
//                }else{
//                    String k = Uid[9] +"\t"+ "其他";
//                    k1.set(k);
//                    context.write(k1, a);
//                }
                String k="";
                for (String s : types.keySet()) {
                    if (phone_type.contains(s)) {
                        k = Uid[9] + "\t" + s;
                        k1.set(k);
                        context.write(k1, a);
                    }
                }
                if(k==""){
                    k = imei +"\t"+ "其他";
                    k1.set(k);
                    context.write(k1, a);
                }
            }


        }
    }

    /**
     * reduce任务
     */
    static class Myreduce1 extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int b = 0 ;
            for(IntWritable value :values){
                b=b+value.get();
            }
            context.write(key,new IntWritable(b));

        }
    }
}
