package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by shangyongqiang on 2016/4/6.
 * 1.生成hfile文件
 * 2.将生成的hfile文件导入到hbase
 */
public class TestHfile1 {
    public static  class HFileMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",",-1);
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(line[0].getBytes());
            KeyValue kv = new KeyValue(line[0].getBytes(),
                    line[1].getBytes(),line[2].getBytes(),
                    System.currentTimeMillis(),line[3].getBytes()
                    );
            if(null!=kv){
                context.write(rowkey,kv);
            }
        }
    }
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] dfsArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        Job job = new Job (conf,"HFile bulk load test");
        job.setJarByClass(TestHfile1.class);
        job.setMapperClass(HFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
        FileInputFormat.addInputPath(job, new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));
//        HFileOutputFormat.configureIncrementalLoad(job,new Htable());
        System.exit(job.waitForCompletion(true)? 0: 1);

    }
//    public static class HfileLoder{
//        public static void main(String[] args) throws Exception {
//            String[] dfsArgs = new GenericOptionsParser(
//                    ConnectionUtil.getConfiguration(), args).getRemainingArgs();
//            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
//                    ConnectionUtil.getConfiguration());
//            loader.doBulkLoad(new Path(dfsArgs[0]), ConnectionUtil.getTable());
//        }
//    }
}
