package com.job1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by shangyongqiang on 2016/8/10.
 */
public class UserApp extends Configured implements Tool {

    public static Text k1 = new Text();
    public static Text value1 = new Text();
    public static Text value2 = new Text();
    public static final Logger LOGGER = LogManager.getLogger(UserAppsOrderJob.class);
    public static String catalog01 = "/user/linan/dmpUserApps/";

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputh = args[0];
        List<String> list = new ArrayList<String>();
        list.add(catalog01);
        return UserAppsOrderJob.test(conf, list, outputh);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new UserApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static class UserAppsOrderJob {
        public static int test(Configuration conf, List<String> list, String outputh) throws IOException,
                ClassNotFoundException, InterruptedException {
            conf.setBoolean(Job.USED_GENERIC_PARSER, true);
            conf.setStrings(Job.QUEUE_NAME, "dmp2");
            Job job = Job.getInstance(conf, "TestJob1");
            job.setJarByClass(UserAppsOrderJob.class);
            job.setMapperClass(UserAppsOrderMapper.class);
            job.setReducerClass(UserAppsOrderReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputDirRecursive(job, true);
            FileSystem fileSystem = FileSystem.get(conf);
            job.setOutputFormatClass(TextOutputFormat.class);
            for (int i = 0; i < list.size(); i++) {
                if (fileSystem.exists(new Path(list.get(i)))) {
                    FileInputFormat.addInputPath(job, new Path(list.get(i)));
                }
            }
            if (fileSystem.exists(new Path(outputh))) {
                fileSystem.delete(new Path(outputh), true);
            }
            FileOutputFormat.setOutputPath(job, new Path(outputh));

            return job.waitForCompletion(true) ? 0 : 1;
        }

    }

    static class UserAppsOrderMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] arrts = value.toString().split("\\t");
            String uid = arrts[0];
            String appCode = arrts[1];
            String count = arrts[2];
            k1.set(uid);
            value1.set(appCode + "_" + count);
            context.write(k1, value1);
        }

    }

    static class UserAppsOrderReducer extends Reducer<Text, Text, Text, Text> {
        private static LinkedHashMap<String, String> linkMap = new LinkedHashMap<String, String>();
        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Properties prop = new Properties();
            try {
                InputStream in = UserApp.class.getResourceAsStream("/appCode.properties");
                if(in != null){
                    //加载属性列表
                    prop.load(in);
                    Enumeration<Object> keys = prop.keys();
                    while(keys.hasMoreElements()){
                        String key = keys.nextElement().toString();
                        String value = prop.getProperty(key);
                        linkMap.put(key, value);
                    }
//					LOGGER.info("setup map is :"+linkMap.size());
                }
                in.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
//            LinkedHashMap<String, String> linkMap = new LinkedHashMap<String, String>();
//            Properties prop = new Properties();
//            try {
//                InputStream in = UserApp.class.getResourceAsStream("/appCode.properties");
//                if (in != null) {
//                    //加载属性列表
//                    prop.load(in);
//                    Enumeration<Object> keys = prop.keys();
//                    while (keys.hasMoreElements()) {
//                        String key1 = keys.nextElement().toString();
//                        String value = prop.getProperty(key1);
//                        linkMap.put(key1, value);
//                    }
//                }
//                in.close();
//            }catch (Exception e){
//                e.printStackTrace();
//            }

            if (linkMap.size() > 0) {
                for (Text t : values) {
                    String[] value = t.toString().split("_");
                    String appCode = value[0];
                    String num = value[1];
                    if (linkMap.containsKey(appCode)) {
                        linkMap.put(appCode, num);
                    }
                }
                StringBuilder sbuilder = new StringBuilder();
                for (Map.Entry<String, String> entry : linkMap.entrySet()) {
                    sbuilder.append(entry.getValue()).append(",");
                }
                String info = sbuilder.toString();
                value2.set(info.substring(0, info.length() - 1));
                context.write(key, value2);
                for (Map.Entry<String, String> entry : linkMap.entrySet()) {
                    linkMap.put(entry.getKey(),"0");
                }
            }


        }
    }
}
