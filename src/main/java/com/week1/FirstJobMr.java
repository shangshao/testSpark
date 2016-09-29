package com.week1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/5/23.
 */
public class FirstJobMr extends Configured implements Tool {
    public static Text k1 = new Text();
    public static Analyzer analyzer = new IKAnalyzer(false);
    public static String ding="hdfs://mycluster/user/hive/warehouse/getdatas.db/s_keys";
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //可以是输入参数args
//         String path1 = args[0];//起始日期
//         String path2 = args[1];//结束日期
//        Date d1 = DFS.parse(path1);
//        Date d2 = DFS.parse(path2);
//        Calendar calendar=Calendar.getInstance();
//        calendar.setTime(d1);
//        Calendar calendar1=Calendar.getInstance();
//        calendar1.setTime(d2);
//        int ge = calendar1.get(Calendar.DAY_OF_MONTH)-calendar.get(Calendar.DAY_OF_MONTH);
        //本地测试目录是hdfs上的目录
//        String inputPathsStr = "hdfs://dmp01:9000/user/hive/basedata/liantong/2015121411";
        String outputPath = ding+"/result";
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
            ToolRunner.run(conf, new FirstJobMr(), args);
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
            Job job = Job.getInstance(conf, "TestJob");

            //本地测试
//            conf.set("fs.defaultFS", "hdfs://dmp01:9000");
//            conf.set("yarn.resourcemanager.hostname", "dmp01");

            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置map的输出key和value的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(TestMapper1.class);
            //自定义分区一个区就是一天的数据
            //job.setPartitionerClass(MyPartitioner.class);
            // job.setNumReduceTasks(6);
            //job.setInputFormatClass(SequenceFileInputFormat.class);
            //  FileInputFormat.setInputPathFilter(job, MapReducePathFilter.class);
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
//            job.setReducerClass(Myreduce1.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(output));

//                if(job.waitForCompletion(true)){
//                    FSDataOutputStream os = fileSystem.create(new Path("/tmp/data"));
//                    FSDataInputStream is = fileSystem.open(new Path(output));
//                    FileStatus stat = fileSystem.getFileStatus(new Path(output));
//                    // create the buffer
//                    byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
//                    is.readFully(0, buffer);
//                    os.write(buffer);
//                    os.close();
//                    is.close();
//                }

            //需要将新生成的数据放到指定目录
//            if(job.waitForCompletion(true)){
//                FileSystem fs = FileSystem.get(URI.create("/shang/macdata/part-r-00000"), conf);
//                FSDataInputStream is = fileSystem.open(new Path("hdfs://dmp01:9000"+output+"/part-r-00000"));
//                FSDataOutputStream out = fs.append(new
//                        Path("/shang/macdata/part-r-00000"));
//                IOUtils.copyBytes(is, out, 4096, true);
//
//            }

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] attrs = value.toString().split("\\u0001");
            if(attrs.length==5){
                String userid = attrs[0];
                String province = attrs[1];
                String keywords = attrs[2];
                String time = attrs[3];
                String source =attrs[4];
                if(attrs[2]!=null&&attrs[2]!=""){
                    StringReader reader = new StringReader(keywords);
                    TokenStream ts = analyzer.tokenStream("", reader);
                    CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
                    ts.reset();
                    String s ="";
                    while(ts.incrementToken()){
//                        term.toString()
                        s+= "|"+term.toString();
                    }
                    if(s!="" &&s !=null){
                        s.substring(1);
                    }
                    k1.set(userid);
                    s = s+","+province+","+source+","+time;
                    context.write(k1,new Text(s));
                    reader.close();
                    ts.close();
                }
            }
//            String[] Uid = attrs.split("\\u0001");
//            String idfa=Uid[5];//明文
//            String imeim =Uid[8];//明文
//            String androidid=Uid[11];//明文
//            if(!imeim.equals("null") ){
//                if(!androidid.equals("null")){
//                   String k = imeim.split("\\u0003")[0]+","+androidid.split("\\u0003")[0];
//                    k1.set(k);
//                    context.write(k1,a);
//                }else if(!idfa.equals("null")){
//                    String k = imeim.split("\\u0003")[0]+","+idfa.split("\\u0003")[0];
//                    k1.set(k);
//                    context.write(k1,a);
//                }
//            }
//            k1.set(attrs[0]);
//            context.write(k1,a);
        }
    }

    /**
     * reduce任务
     */
//    static class Myreduce1 extends Reducer<Text, Text,Text, Text> {
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            int b = 0 ;
//            for(Text value :values){
//                b=b+value.get();
//            }
//            if(b==1){
//                context.write(key,a);
//            }
//        }
//    }
}
