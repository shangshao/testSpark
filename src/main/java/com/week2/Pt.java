package com.week2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by shangyongqiang on 2016/4/21.
 */
public class Pt extends Partitioner<Text,Text> {
        @Override
        public int getPartition(Text text, Text text2, int i) {
            String[] o1= text.toString().split(",");
            return  Integer.parseInt(o1[0]);

    }
}
