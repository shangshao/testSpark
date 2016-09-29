package com.race;

/**
 * Created by shangyongqiang on 2016/4/19.
 */

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class CleanData extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        String inputPathsStr = "/user/shangyongqiang/bisai/out/";
        String outputPath = "/user/shangyongqiang/bisai/out1/";
        String[] inputPaths = inputPathsStr.split(",");
        return TestJosms.test(conf, inputPaths, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new CleanData(), args);
//            if(ToolRunner.run(conf, new CleanData(), args)==0){
//                ToolRunner.run(conf, new ActiveUserStatistics1(), args);
//            }
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
            //性别
            map1.put("男", 0.0);
            map1.put("女", 1.0);
            map1.put("不详", 0.5);
            //网段
            map1.put("2G", 0.0);
            map1.put("3G", 1.0);
            //年龄
            map1.put("17岁以下", 1 / 8.0);
            map1.put("23-25", 2 / 8.0);
            map1.put("30-39", 3 / 8.0);
            map1.put("未知", 0.0);
            map1.put("18-22", 4 / 8.0);
            map1.put("26-29", 5 / 8.0);
            map1.put("50-59", 6 / 8.0);
            map1.put("40-49", 7 / 8.0);
            map1.put("60以上", 1.0);
            //流量
            map1.put("0-499", Double.parseDouble(String.format("%.2f", 1 / 11.0)));
            map1.put("500-999", Double.parseDouble(String.format("%.2f", 2 / 11.0)));
            map1.put("1500-1999", Double.parseDouble(String.format("%.2f", 3 / 11.0)));
            map1.put("2500-2999", Double.parseDouble(String.format("%.2f", 4 / 11.0)));
            map1.put("3500-3999", Double.parseDouble(String.format("%.2f", 5 / 11.0)));
            map1.put("4500-4999", Double.parseDouble(String.format("%.2f", 6 / 11.0)));
            map1.put("1000-1499", Double.parseDouble(String.format("%.2f", 7 / 11.0)));
            map1.put("2000-2499", Double.parseDouble(String.format("%.2f", 8 / 11.0)));
            map1.put("3000-3499", Double.parseDouble(String.format("%.2f", 9 / 11.0)));
            map1.put("4000-4499", Double.parseDouble(String.format("%.2f", 10 / 11.0)));
            map1.put("5000以上", Double.parseDouble(String.format("%.2f", 1.0)));
            //arpu  注意先判断为空
            map1.put("0-49", Double.parseDouble(String.format("%.2f", 1 / 7.0)));
            map1.put("50-99", Double.parseDouble(String.format("%.2f", 2 / 7.0)));
            map1.put("100-149", Double.parseDouble(String.format("%.2f", 3 / 7.0)));
            map1.put("150-199", Double.parseDouble(String.format("%.2f", 4 / 7.0)));
            map1.put("200-249", Double.parseDouble(String.format("%.2f", 5 / 7.0)));
            map1.put("250-299", Double.parseDouble(String.format("%.2f", 6 / 7.0)));
            map1.put("300及以上", Double.parseDouble(String.format("%.2f", 1.0)));

            //读入手机型号作为判断
            InputStream inputStream = CleanData.class.getResourceAsStream("/moblietype.txt");
            InputStream inputStream1 = CleanData.class.getResourceAsStream("/moblietype1.txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(inputStream1));
            String linedata;
            while ((linedata = bufferedReader.readLine()) != null) {
                //全部是小写
                String[] a = linedata.split("\\t");
                map2.put(a[0], a[1]);
            }
            while ((linedata = bufferedReader1.readLine()) != null) {
                //全部是小写
                String[] a = linedata.split("\\t");
                map3.put(a[0], Double.parseDouble(a[1]));
            }
            bufferedReader.close();
            inputStream.close();
            bufferedReader1.close();
            inputStream1.close();

            //判断读的文件是哪个文件然后分区合并
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String attrs = value.toString();
            String[] Uid = attrs.split(",");
            if (Uid.length > 11) {
                String imsi, model = null;
                //连续的三个月为一组来计算

                imsi = Uid[1];
                Double phone, sms, data, sex, net, age, arpu, mtype = 0.0, version = 0.0, os;
                data = Uid[9] == null ? 0.0 : map1.keySet().contains(Uid[9]) ? (double) map1.get(Uid[9]) : 0;
                age = Uid[4] == null ? 0.0 : map1.keySet().contains(Uid[4]) ? (double) map1.get(Uid[4]) : 0;
                arpu = Uid[5] == null ? 0.0 : map1.keySet().contains(Uid[5]) ? (double) map1.get(Uid[5]) : 0;
                sex = Uid[3] == null ? 0.0 : map1.keySet().contains(Uid[3]) ? (double) map1.get(Uid[3]) : 0;
                net = Uid[2] == null ? 0.0 : map1.keySet().contains(Uid[2]) ? (double) map1.get(Uid[2]) : 0;

            /*
            time
            imsi
            net
            sex
            age
            arpu
            mtype
            model
            data
            phone
            sms
             */
                if (Uid[9] != null) {
                    //判断话费区间
                    int a = Integer.parseInt(Uid[9]);
                    if (a < 50) {
                        phone = Double.parseDouble(String.format("%.2f", 1 / 6.0));
                    } else if (a < 200 && a > 49) {
                        phone = Double.parseDouble(String.format("%.2f", 2 / 6.0));
                    } else if (a < 300 && a > 199) {
                        phone = Double.parseDouble(String.format("%.2f", 3 / 6.0));
                    } else if (a < 500 && a > 299) {
                        phone = Double.parseDouble(String.format("%.2f", 4 / 6.0));
                    } else if (a < 1000 && a > 499) {
                        phone = Double.parseDouble(String.format("%.2f", 5 / 6.0));
                    } else {
                        phone = Double.parseDouble(String.format("%.2f", 6 / 6.0));
                    }
                } else {
                    phone = 0.0;
                }
                if (Uid[10] != null) {
                    int b = Integer.parseInt(Uid[10]);
                    //判断短信区间通过统计数据发现最大的是999,99
                    if (b < 10) {
                        sms = 0.10;
                    } else if (b < 20 && b > 9) {
                        sms = 0.20;
                    } else if (b < 30 && b > 19) {
                        sms = 0.30;
                    } else if (b < 40 && b > 29) {
                        sms = 0.40;
                    } else if (b < 50 && b > 39) {
                        sms = 0.50;
                    } else if (b < 60 && b > 49) {
                        sms = 0.60;
                    } else if (b < 70 && b > 59) {
                        sms = 0.70;
                    } else if (b < 80 && b > 69) {
                        sms = 0.80;
                    } else if (b < 90 && b > 79) {
                        sms = 0.90;
                    } else {
                        sms = 1.0;
                    }
                } else {
                    sms = 0.0;
                }

                //判断是否换机
                int ji = 0;
                String[] type = Uid[11].split("\\|");
                if (type.length == 1) {
                    mtype = 0.0;
                } else {
                    for (int i = 0; i < type.length; i++) {
                        for (int j = i + 1; j < type.length; j++) {
                            if (!type[i].toLowerCase().trim().contains(type[j].toLowerCase().trim()) || !type[j].toLowerCase().trim().contains(type[i].toLowerCase().trim())) {
                                //换机,只要是有一对互相不包含就表示换机
                                mtype = 1.0;
                            }
                        }
                    }
                }


                //1.判断手机的操作系统是不是安卓,ios或者其他
                //2.把手机型号归一化 取top20得手机,其他的归位0.1,null的话是0.0
                String mobile = Uid[6].toLowerCase();
                if (map2.containsKey(mobile)) {
                    model = mobile;

                    if (map3.containsKey(model)) {
                        version = (double) map3.get(model);
                    } else {
                        version = 0.1;
                    }

                    if (mobile.contains("apple")) {
                        os = 0.5;
                    } else {
                        os = 1.0;
                    }
                } else if (map2.containsValue(mobile)) {
                    for (Object s : map2.keySet()) {
                        if (mobile.equals((String) map2.get(s))) {
                            model = (String) s;
                        }
                    }

                    if (map3.containsKey(model)) {
                        version = (double) map3.get(model);
                    } else {
                        version = 0.1;
                    }

                    if (mobile.contains("apple")) {
                        os = 0.5;
                    } else {
                        os = 1.0;
                    }
                } else {
                    os = 0.0;
                    model = "other";
                }

                //最后所有的double拼在一起
                String zong = mtype + "," + version + "," + net + "," + sex + "," + age + "," + arpu + "," + data + "," + phone + "," + sms + "," + os;
                for (int i = 0; i <13 ; i++) {


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
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String  net=null,
            sex,
                    age,
            arpu=null,
                    mtype,
            os=null,
                    data=null,
            phone=null,
                    sms=null;
            for(Text value :values){
                String [] records =value.toString().split(",");
                net+="|"+records[0];
                sex=records[1];
                age=records[2];
                arpu="|"+records[3];
                mtype=records[4];
                os="|"+records[5];
                data="|"+records[6];
                phone="|"+records[7];
                sms="|"+records[8];
            }
//            b=b.substring(1);
//            context.write(key,new Text(b));

        }
    }


}

