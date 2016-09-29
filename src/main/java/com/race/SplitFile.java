package com.race;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by shangyongqiang on 2016/4/20.
 */
public class SplitFile {
    public static void main(String [] args)  {
        Configuration conf= new Configuration();
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
        conf.set("fs.defaultFS", "hdfs://192.168.3.151:9000");
        FileSystem fs = null;
        FSDataInputStream sf=null;
        try {
            fs = FileSystem. get(URI.create("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/1.txt"), conf);
            for (int i =1; i <11 ; i++) {
//                if(fs.exists(new Path("/user/shangyongqiang/bisai/data/"+i+"-"+(i+2)))){
//                   fs.delete(new Path("/user/shangyongqiang/bisai/data/"+i+"-"+(i+2)),true);
//                }
                FSDataOutputStream outputStream = fs.create(new Path("/user/shangyongqiang/bisai/data/" + i + "-" + (i + 2)));
                IOUtils.closeStream(outputStream);
            }

         sf = fs.open(new Path("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/1.txt"));
        String linedata;
            int a1=0;//计数
        while ((linedata = sf.readLine()) != null) {
            //全部是小写
            linedata=linedata+"\n";
            String[] a = linedata.split(",");
            int time = Integer.parseInt(a[0].substring(4));
            System.out.println("第"+a1+"次");
            for (int i = 1; i <= time-2; i++) {
                int b=i+1;
                int c =i+2;
                int d =i+3;
                if(time==b ||time==c || time==d){
                    FSDataOutputStream out = fs.append(new
                            Path("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/data/"+i+"-"+(i+2)));
                    IOUtils.copyBytes(new ByteArrayInputStream(linedata.getBytes()), out, 4096, true);
                }
            }
            a1++;
        }} catch (IOException e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(sf);
        }

    }
}
