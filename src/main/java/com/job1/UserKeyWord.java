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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/7/18.
 */
public class UserKeyWord extends Configured implements Tool {

        //把用户一个月内搜索关键词进行整合,进行tfidf处理,算出权重最高的20个词
    public static Text k1 = new Text();
    public static String ding="hdfs://mycluster/user/shangyongqiang/hb/keyword";
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputPath = ding+"/step1";
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
            if(ToolRunner.run(conf, new UserKeyWord(), args)==0){
                if(ToolRunner.run(conf, new UserKeyWord1(), args)==0){
                   ToolRunner.run(conf, new UserKeyWord2(), args);
                }
            }
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
            Job job = Job.getInstance(conf, "TfIdf");

            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置map的输出key和value的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapperClass(TestMapper1.class);
            job.setReducerClass(Myreduce1.class);
            job.setNumReduceTasks(4);
            job.setPartitionerClass(MyPt.class);
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
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] v = value.toString().split("\\u0001");
            if(v.length>=2 && !"\\N".equals(v[1])){
                String id=v[0].trim();
                String content =v[1].trim();

                StringReader sr =new StringReader(content);
                IKSegmenter ikSegmenter =new IKSegmenter(sr, true);
                Lexeme word=null;
                while( (word=ikSegmenter.next()) !=null ){
                    String w= word.getLexemeText();
                    context.write(new Text(w+"_"+id), new IntWritable(1));
                }
                context.write(new Text("count"), new IntWritable(1));
            }else{
                System.out.println(value.toString()+"-------------");
            }
            }

        }



    /**
     * reduce任务
     */
    static class Myreduce1 extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for( IntWritable i :values ){
                sum= sum+i.get();
            }
            if(values.equals(new Text("count"))){
                System.out.println(values.toString() +"___________"+sum);
            }
            context.write(key, new IntWritable(sum));
        }

        }
}


