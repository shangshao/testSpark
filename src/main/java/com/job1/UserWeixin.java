package com.job1;



import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/7/18.
 */
public class UserWeixin extends Configured implements Tool {

    public static Text k1 = new Text();
    public static String ding="hdfs://mycluster/user/hive/weixin_biz/ee_weixin_biz.txt";
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputPath = "hdfs://mycluster/user/shangyongqiang/weixin/result";
        List<String> list = new ArrayList<String>();
        list.add(ding);
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf= new Configuration();
        try {
            ToolRunner.run(conf, new UserWeixin(), args);
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

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
        //MTIxMDE3NjU2MQ==961之醉想听你唱fm961961内涵节目你懂得
            //biz                 	string
           // name                	string
           // code                	string
           // description         	string
            String[] attrs = value.toString().split("\\u0001");
            List<String> list = new ArrayList();
            String description=null;
            String code =null;
            if(attrs.length>2){
                if(attrs.length==4){
                    description =attrs[3];
                    code =attrs[2];
                }
                String biz =attrs[0];
                String name =attrs[1];
                list.add(name);
                if(code!=null){
                    list.add(code);
                }


                //对描述信息进行关键词提取与自定义词典的分词
                StringBuilder sb = new StringBuilder();
                if(description!=null){
                    //提取出8个关键词
                    String keyword= StringUtils.join(HanLP.extractKeyword(description, 8), "|");
                    list.add(keyword);
                    for (Term t : HanLP.segment(description)) {
                        if (t.nature.startsWith("nsyq")) {
                            sb.append("|" + t.toString());
                        }
                    }
                    if(sb.length()>0){
                        sb.deleteCharAt(0);
                    }
                    list.add(sb.toString());
                }
                context.write(new Text(biz),new Text(StringUtils.join(list,"\t")));
            }

        }
    }

}
