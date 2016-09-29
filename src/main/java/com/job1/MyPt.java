package com.job1;

import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by shangyongqiang on 2016/7/19.
 */
public class MyPt extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
                if(text.equals(new Text("count"))) return 3;
        else return 0;
    }
    }



