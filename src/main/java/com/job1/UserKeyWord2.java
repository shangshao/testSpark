package com.job1;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by shangyongqiang on 2016/7/18.
 */
public class UserKeyWord2 extends Configured implements Tool {

        //把用户一个月内搜索关键词进行整合,进行tfidf处理,算出权重最高的20个词
    public static Text k1 = new Text();
    public static String ding="hdfs://mycluster/user/shangyongqiang/hb/keyword/step1";
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputPath = "hdfs://mycluster/user/shangyongqiang/hb/keyword/step3";
        List<String> list = new ArrayList<String>();
        //读入库存数据作对比来找出新增的数据
        list.add(ding);
//        list.add(args[2]+"macidfaandroid");
        //String[] inputPaths = inputPathsStr.split(",");
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf= new Configuration();
        try {
            ToolRunner.run(conf, new UserKeyWord2(), args);
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

            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            Job job = Job.getInstance(conf, "TfIdf1");
            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置map的输出key和value的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(TestMapper1.class);
            job.setReducerClass(Myreduce1.class);
            job.addCacheFile(new Path("hdfs://mycluster/user/shangyongqiang/hb/keyword/step1/part-r-00003").toUri());
            job.addCacheFile(new Path("hdfs://mycluster/user/shangyongqiang/hb/keyword/step2/part-r-00000").toUri());
//            job.setNumReduceTasks(4);
//            job.setPartitionerClass(MyPt.class);
            FileInputFormat.setInputDirRecursive(job, true);

            FileSystem fileSystem = FileSystem.get(conf);
            for (int i=0;i<list.size();i++) {
                if(fileSystem.exists(new Path(list.get(i)))){
                    FileInputFormat.addInputPath(job, new Path(list.get(i)));
                }


            }
            if (fileSystem.exists(new Path(output))) {
                fileSystem.delete(new Path(output), true);
            }
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private static final Logger LOGGER = LogManager.getLogger(TestMapper1.class);
        public static Map<String,Integer> cmap = new HashMap<>();
        public static Map<String,Integer> df = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if(cmap.size()==0 || df.size()==0) {
                URI[] uri = context.getCacheFiles();
                if (uri != null) {
                    for (int i = 0; i < uri.length; i++) {
                        if (uri[i].getPath().endsWith("part-r-00003")) {
                            Path path = new Path(uri[i].getPath());
                            BufferedReader bf = new BufferedReader(new FileReader(path.getName()));
                            String line = bf.readLine();
                            if (line.startsWith("count")) {
                                String[] ls = line.split("\\t");
                                cmap.put(ls[0], Integer.valueOf(ls[1]));
                            }
                            bf.close();
                        } else if(uri[i].getPath().endsWith("part-r-00000")){
                            Path path = new Path(uri[i].getPath());
                            BufferedReader bf = new BufferedReader(new FileReader(path.getName()));
                            String line;
                            while ((line = bf.readLine()) != null) {
                                String[] ls = line.split("\\t");
                                df.put(ls[0], Integer.valueOf(ls[1]));
                            }
                            bf.close();
                        }
                    }
                }

            }
            LOGGER.info("shangyongqiang");
            LOGGER.info(cmap.toString());
            LOGGER.info(df.size());
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            FileSplit fs = (FileSplit)context.getInputSplit();
            if(!fs.getPath().getName().contains("part-r-00003")){
                String [] v = value.toString().trim().split("\t");
                if(v.length>=2 ){
                    int tf = Integer.valueOf(v[1].trim());
                    String [] b = v[0].split("_");
                    if(b.length>=2){
                        String w=b[0];
                        String id=b[1];
                        double s =tf*Math.log(cmap.get("count")/df.get(w));
                        NumberFormat nf = NumberFormat.getInstance();
                        nf.setMaximumFractionDigits(5);
                        context.write(new Text(id),new Text(nf.format(s)+":"+w));
                    }
                }
            }

            }

        }



    /**
     * reduce任务
     */
    static class Myreduce1 extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//           StringBuffer sb = new StringBuffer();
//
//            for( Text i :values ){
//               sb.append(i.toString()+"\\t");
//            }
//            context.write(key,new Text(sb.toString()));
            //对value进行排序
            List<String> list = new ArrayList();
         for( Text i :values ){
              list.add(i.toString());
            }
            Collections.sort(list);
            Collections.reverse(list);
            int num=0;
            StringBuilder sb =new StringBuilder();
            //取前10个搜索关键词
            for (String a:list){
                if(num>10)break;
               String[] b= a.split(":");
                if(b[1].length()>1){
                    sb.append("|"+b[1]+":"+b[0]);
                    num++;
                }
            }
            if(sb.length()>0){
                sb.deleteCharAt(0);
                context.write(key,new Text(sb.toString()));
            }

        }

        }
}


