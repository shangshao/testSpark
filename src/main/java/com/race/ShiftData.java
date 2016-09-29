package com.race;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/4/22.
 */
public class ShiftData extends Configured implements Tool {

        //前三个月的用户数据来预测未来三个月的用户是否会换机
        //先找出未来三个月的用户是不是会换机

        //根文件得出1.txt(1-3),需要4.txt(4-6)
//        for (int i = 1; i < 11; i++) {
//
//        }
        public static Text k1 = new Text();

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        String inputPathsStr = "/user/shangyongqiang/bisai/data/";
        String outputPath = "/user/shangyongqiang/bisai/out2/";
        String[] inputPaths = inputPathsStr.split(",");
        return TestJosms.test(conf, inputPaths, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
//           ToolRunner.run(conf, new CleanDataStep1(), args)==0){
            if( ToolRunner.run(conf, new ShiftData(), args)==0){
            ToolRunner.run(conf, new CleanDataStep2(), args);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 测试job
     */

    static class TestJosms {

        private static final Logger LOGGER = LogManager.getLogger(TestJosms.class);

        public static int test(Configuration conf, String[] inputs, String output)
                throws IOException,
                ClassNotFoundException, InterruptedException {

            conf.set("mapreduce.job.queuename", "dmp2");
            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            Job job = Job.getInstance(conf, "bisai");

            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置map的输出key和value的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(TestMapper1.class);
            job.setNumReduceTasks(10);
            job.setPartitionerClass(Pt.class);
            //job.setInputFormatClass(SequenceFileInputFormat.class);
            //  FileInputFormat.setInputPathFilter(job, MapReducePathFilter.class);
            FileInputFormat.setInputDirRecursive(job, true);

            FileSystem fileSystem = FileSystem.get(conf);
            for (int i = 0; i < inputs.length; i++) {
                if (fileSystem.exists(new Path(inputs[i]))) {
                    FileInputFormat.addInputPath(job, new Path(inputs[i]));
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
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private static final Logger LOGGER = LogManager.getLogger(TestMapper1.class);
        Map map1 = new HashMap();
        //正读
        Map map2 = new HashMap();
        //读入手机型号的权重
        Map map3 = new HashMap();
        //判断文件的路径
        String flag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String attrs = value.toString();
            String[] Uid = attrs.split(",");
            if (Uid.length > 10) {
                String imsi, model=null,modelx;
                //连续的三个月为一组来计算
                modelx=(Uid[7]==null?"weizhi":Uid[7]);
                imsi = Uid[1];
//                Double phone, sms, data, sex, net, age, arpu, version = 0.0, os;
//                data = Uid[8] == null ? 0.0 : map1.keySet().contains(Uid[8]) ? (double) map1.get(Uid[8]) : 0.0;
//                age = Uid[4] == null ? 0.0 : map1.keySet().contains(Uid[4]) ? (double) map1.get(Uid[4]) : 0.0;
//                arpu = Uid[5] == null ? 0.0 : map1.keySet().contains(Uid[5]) ? (double) map1.get(Uid[5]) : 0.0;
//                sex = Uid[3] == null ? 0.0 : map1.keySet().contains(Uid[3]) ? (double) map1.get(Uid[3]) : 0.0;
//                net = Uid[2] == null ? 0.0 : map1.keySet().contains(Uid[2]) ? (double) map1.get(Uid[2]) : 0.0;
//
//                if (Uid[9] != null) {
//                    //判断话费区间
//                    int a = Integer.parseInt(Uid[9]);
//                    if (a < 50) {
//                        phone = Double.parseDouble(String.format("%.2f", 1 / 6.0));
//                    } else if (a < 200 && a > 49) {
//                        phone = Double.parseDouble(String.format("%.2f", 2 / 6.0));
//                    } else if (a < 300 && a > 199) {
//                        phone = Double.parseDouble(String.format("%.2f", 3 / 6.0));
//                    } else if (a < 500 && a > 299) {
//                        phone = Double.parseDouble(String.format("%.2f", 4 / 6.0));
//                    } else if (a < 1000 && a > 499) {
//                        phone = Double.parseDouble(String.format("%.2f", 5 / 6.0));
//                    } else {
//                        phone = Double.parseDouble(String.format("%.2f", 6 / 6.0));
//                    }
//                } else {
//                    phone = 0.0;
//                }
//                if (Uid[10] != null) {
//                    int b = Integer.parseInt(Uid[10]);
//                    //判断短信区间通过统计数据发现最大的是999,99
//                    if (b < 10) {
//                        sms = 0.10;
//                    } else if (b < 20 && b > 9) {
//                        sms = 0.20;
//                    } else if (b < 30 && b > 19) {
//                        sms = 0.30;
//                    } else if (b < 40 && b > 29) {
//                        sms = 0.40;
//                    } else if (b < 50 && b > 39) {
//                        sms = 0.50;
//                    } else if (b < 60 && b > 49) {
//                        sms = 0.60;
//                    } else if (b < 70 && b > 59) {
//                        sms = 0.70;
//                    } else if (b < 80 && b > 69) {
//                        sms = 0.80;
//                    } else if (b < 90 && b > 79) {
//                        sms = 0.90;
//                    } else {
//                        sms = 1.0;
//                    }
//                } else {
//                    sms = 0.0;
//                }


//
//                //1.判断手机的操作系统是不是安卓,ios或者其他
//                //2.把手机型号归一化 取top20得手机,其他的归位0.1,null的话是0.0
//                String mobile = (Uid[6]==null?"weizhi":Uid[6]).toLowerCase();
//                if (map2.containsKey(mobile)) {
//                    model = mobile;
//
//                    if (map3.containsKey(model)) {
//                        version = (double) map3.get(model);
//                    } else {
//                        version = 0.1;
//                    }
//
//                    if (mobile.contains("apple")) {
//                        os = 0.5;
//                    } else {
//                        os = 1.0;
//                    }
//                } else if (map2.containsValue(mobile)) {
//                    for (Object s : map2.keySet()) {
//                        if (mobile.equals((String) map2.get(s))) {
//                            model = (String) s;
//                        }
//                    }

//                    if (map3.containsKey(model)) {
//                        version = (double) map3.get(model);
//                    } else {
//                        version = 0.1;
//                    }
//
//                    if (mobile.contains("apple")) {
//                        os = 0.5;
//                    } else {
//                        os = 1.0;
//                    }
//                } else {
//                    version=0.0;
//                    os = 0.0;
//                    model = "other";
//                }

                //最后所有的double拼在一起0.0,0.0,0.0,0.375,0.14,0.09,0.17,0.1,0.0,
                String zong = modelx;
                for (int i = 0; i <12 ; i++) {
                    if(flag.endsWith((i+1)+".txt")){
                        k1.set(i+","+imsi);
                        context.write(k1, new Text(zong));
                    }
                }

            } else {
                LOGGER.info(attrs);
            }
        }
    }

    /**
     * reduce任务
     */
    static class Myreduce1 extends Reducer<Text, Text,Text, Text> {
        private static final Logger LOGGER = LogManager.getLogger(Myreduce1.class);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String  label="0.0",
                    modelx="";
            for(Text value :values){
                modelx+=("|"+value.toString());
            }
                modelx=modelx.substring(1);
                String[] type = modelx.split("\\|");
                for (int i = 0; i < type.length; i++) {
                    for (int j = i + 1; j < type.length; j++) {
                        if (!type[i].toLowerCase().trim().contains(type[j].toLowerCase().trim()) || !type[j].toLowerCase().trim().contains(type[i].toLowerCase().trim())) {
                            //换机,只要是有一对互相不包含就表示换机
                            label = "1.0";
                        }
                    }
            }
            String []keys=key.toString().split(",");
            int a =Integer.parseInt(keys[0]);
            if(a>2){
                String newKey=(a-3)+","+keys[1];
                k1.set(newKey);
                context.write(k1,new Text(label));
            }
        }
    }


}

