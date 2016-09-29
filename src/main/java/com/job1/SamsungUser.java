package com.job1;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SamsungUser extends Configured implements Tool {

    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outputPath = args[1];
        List<String> list = new ArrayList<String>();
        list.add(args[0]);
        return TestJob1.test(conf, list, outputPath);
    }

    public static void main(String[] args) {
        Configuration conf= new Configuration();
        try {
            ToolRunner.run(conf, new SamsungUser(), args);
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
            Job job = Job.getInstance(conf, "sanxing");
            job.setJarByClass(TestMapper1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapperClass(TestMapper1.class);
            job.setReducerClass(TestRreduce.class);
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
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Logger LOGGER1 = LogManager.getLogger(TestMapper1.class);
        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] attrs = value.toString().split("\\|");
            if(attrs.length>4){
            String pro = attrs[attrs.length-2];
            String os=attrs[attrs.length-1];
            String goods="";

            if(attrs[2].startsWith("00040008011")){
                //商品加入购物车（商品编号）
                /*
                17ED9EAABB97BF7CEE99CCB8FC66A763|361400024151|00040008011|10419884108|2016083022|GD|1
                 */
                goods=attrs[3];
                String ke = attrs[0]+"\t"+goods+"\t"+pro+"\t"+os;
                k1.set(ke);
                context.write(k1,a);

            }else if(attrs[2].startsWith("00040008012")){
                //查看购物车商品（所有购物车商品编号，数量，用户账户）
                /**
                 * 83CB90289755EDFC46365303915730CB|JKLMNOPQRSTU|00040008012|3199172,1;1032728,1;1745479,1;1075160215,1;1173580208,1;1445766990,1|13482288411_p|2016083100|SH|2
                 */
                String []b=attrs[3].split("\\;");
                for (String s : b) {
                   goods= s.split("\\,")[0];
                    String ke = attrs[0]+"\t"+goods+"\t"+pro+"\t"+os;
                    k1.set(ke);
                    context.write(k1,a);
                }
            }else if(attrs[2].startsWith("00040008013")){
                //订单结算页（结算的商品编号以及数据量，用户账户）
                /**
                 * 83CB90289755EDFC46365303915730CB|JKLMNOPQRSTU|00040008013|3199172,2;1075160215,2|13482288411_p|2016083023|SH|2
                 */
                String []b=attrs[3].split("\\;");
                for (String s : b) {
                    goods= s.split("\\,")[0];
                    String ke = attrs[0]+"\t"+goods+"\t"+pro+"\t"+os;
                    k1.set(ke);
                    context.write(k1,a);
                }
            }else if(attrs[2].startsWith("00040008014")){
                //订单支付页面行为
            }else if(attrs[2].startsWith("00040008015")){
                //购物车商品的编号以及商品单价(需要解码)
                /**
                 * AF9E1C7E0645CA1F751D5D4CECD3411F|JKLMNOPQRSTU|00040008015|[{"skuid":"1637897360","price":"499.00"},{"skuid":"11986492","price":"60.00"},{"skuid":"11794224","price":"37.60"},{"skuid":"117942
                 29","price":"59.10"},{"skuid":"11808389","price":"44.80"},{"skuid":"11829772","price":"107.00"},{"skuid":"11895923","price":"63.20"},{"skuid":"11909384","price":"71.20"},{"skuid":"11896488"
                 ,"price":"78.40"}]|2016083100|SH|2
                 */

//                Gson gson  = new Gson();
//                List<Map<String, String>> o =(List<Map<String, String>>) gson.fromJson(attrs[3].trim().replaceAll(" ",""), new TypeToken<List<Map<String, String>>>() {}.getType());
//                for (Map<String, String> stringStringMap : o) {
//                    goods= stringStringMap.get("skuid");
//                    String ke = attrs[0]+"\t"+goods+"\t"+pro+"\t"+os;
//                    k1.set(ke);
//                    context.write(k1,a);
//                }
                for (String s : attrs[3].replaceAll("\\{|\\}|\\[|\\]|\\\"", "").split("\\,")) {
                    if (s.contains("skuid")) {
                        if (s.split("\\:").length==2) {
                            goods=s.split("\\:")[1];
                            String ke = attrs[0]+"\t"+goods+"\t"+pro+"\t"+os;
                            k1.set(ke);
                            context.write(k1,a);
                        }
                    }
                }


            }else if(attrs[2].startsWith("00040008016")){
                //查看我的订单行为
            }else if(attrs[2].startsWith("00040008017")){
                //订单支付金额（订单编号以及订单总价格，以及支付时间）
            }else if(attrs[2].startsWith("00040008018")){
                //用户对商品进行评价的行为（订单编号，商品编号，评价都在body里）
            }else if(attrs[2].startsWith("00040008001")){
                //浏览商品
            }
            }else{
                LOGGER1.info("syq"+value.toString());
            }
        }
    }
    static class TestRreduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int co=0;
            for(IntWritable a:values){
                co++;
            }
            context.write(key,new IntWritable(co));
        }
    }

}
