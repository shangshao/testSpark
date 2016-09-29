package com.week2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shangyongqiang on 2016/5/25.
 */
public class HBaseCommon extends Configured implements Tool {
    public static Text k1 = new Text();
    public static IntWritable a = new IntWritable(1);
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if(args.length!=7){
            System.err.println("参数0是输出路径,参数1为表名,参数2为列祖:列名以,号隔开,参数3为行业编码,参数4位开始时间,参数5位结束时间,参数6位hive的表明*******如果不是dm_info中的数据参数3为0");
            System.exit(2);
        }
        String outputPath=args[0];
        String tableName=args[1];
        String lie=args[2];
        String hangye=args[3];
        String starttime=args[4];
        String endtime=args[5];
        //把时间转化为时间戳
        SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
        long st = s.parse(starttime).getTime();
        long et = s.parse(endtime).getTime();
        return TestJob1.test(conf, tableName, outputPath,lie,hangye,st,et);
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try {
            if(ToolRunner.run(conf, new HBaseCommon(), args)==0){

                //建立hive表并把生成的数据导入hive中
                //如果存在date字段的时候建表的时候会报错,经达特字段替换成其他
                 args[2]=args[2].replaceAll("date","dat");
                String[] lie=args[2].split("\\,");
                StringBuilder sb = new StringBuilder();
                String starttime=args[4];
                String outputPath=args[0];
                String tn=args[6];
                sb.append("create table if not exists syq.");
                //rowkey一定要加上
                sb.append(tn+" (rowkey string,");
                for (String l:lie){
                    sb.append(l.split("\\:")[1]+" string,");
                }
                sb.deleteCharAt(sb.lastIndexOf(","));
                sb.append(")  partitioned by (dt string) row format delimited fields terminated by '\\t' stored as textfile");
                String lo ="load data inpath '"+outputPath+"' into table syq."+tn+" partition (dt="+starttime+")";
                Connection conn =null;
                try {
                    Class.forName("org.apache.hive.jdbc.HiveDriver");
                    conn = DriverManager.getConnection("jdbc:hive2://192.168.3.171:10000/syq", "shangyongqiang", "");
                    String sql = sb.toString();
                    PreparedStatement ps = conn.prepareStatement(sql);
                    PreparedStatement ps1 = conn.prepareStatement(lo);
                    ps.execute();
                    ps1.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if(conn!=null)
                            conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 测试job
     */

    static class TestJob1 {

        private static final Logger LOGGER = LogManager.getLogger(TestJob1.class);

        public static int test(Configuration conf, String tablename, String output,String lie,String hangye,long st,long et)
                throws IOException,
                ClassNotFoundException, InterruptedException {

//            conf.setBoolean(Job.USED_GENERIC_PARSER, true);

            conf.set("hbase.master","dmp06:16000");
            conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05");
            conf.set("lie",lie);
            conf.set("hangye",hangye);
            Job job = new Job(conf, "HbaseCommon");
            //需要分区,不同类型的格式不一样分区到不同分区中
//            job.setNumReduceTasks(5);
//            job.setPartitionerClass(Pt.class);
            job.setJarByClass(TestMapper1.class);
            job.setMapperClass(TestMapper1.class);
            //清除已经存在的目录
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(output))){
               fs.delete(new Path(output),true);
            }
            //设置任务数据的输出路径；
            FileOutputFormat.setOutputPath(job, new Path(output));
//            job.setReducerClass(Myreduce1.class);
            Scan scan = new Scan();

            scan.setTimeRange(st,et);
            //  RowFilter r = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("0003"));
//    QualifierFilter ff = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("auto")));
//     scan.setFilter(ff);
            TableMapReduceUtil.initTableMapperJob(tablename, scan, TestMapper1.class, Text.class, Text.class, job);
            return job.waitForCompletion(true) ? 0 : 1;

        }
    }

    /**
     * map任务
     */
    static class TestMapper1 extends
            TableMapper<Text,Text> {


        @Override
        protected void map(ImmutableBytesWritable key,Result value,Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String[] args =conf.get("lie").split(",");
            String hangeye =conf.get("hangye");

            Map<String,String> map = new HashMap<String,String>();
            KeyValue[] kv = value.raw();
            List<Map<Integer,String>> list =new ArrayList();
            for(String a:args){
                Map map1 =new HashMap();
                list.add(map1);
            }
            for(KeyValue r:kv){
                String k=new String(r.getFamily())+":"+new String(r.getQualifier());
                String v= new String(r.getValue());
                map.put(k,v);
            }


//火车票与飞机票是以0开头不过滤url 只过滤字段名,(由于这里含有两个类别)
            if("0".equals(hangeye)){
                for (int i = 0; i < args.length; i++) {
                    if(map.get(args[i])!=null){
                        String[] canshu1 = map.get(args[i]).split("\\^");
                        for (int j = 0; j <canshu1.length ; j++) {
                            list.get(i).put(j,canshu1[j]);
                        }
                    }
                }
//                //map的size最大的为准
//                List<Integer> li = new ArrayList();
//                for(Map m :list){
//                    li.add(m.size());
//                }
//                Collections.sort(li);
                for (int i = 0; i < list.get(0).size(); i++) {
                    StringBuilder sb =new StringBuilder();
                    for (Map ma:list){
                        sb.append("|"+ma.get(i));
                    }
                    String te=sb.toString().substring(1).replaceAll("\\|", "\t");
                    context.write(new Text(key.get()),new Text(te));
                }
            }else{
                String url= Bytes.toString(key.get()).split("\\_")[2];
                if(url.startsWith(hangeye)){
                    for (int i = 0; i < args.length; i++) {
                        if(map.get(args[i])!=null){
                            String[] canshu1 = map.get(args[i]).split("\\^");
                            for (int j = 0; j <canshu1.length ; j++) {
                                list.get(i).put(j,canshu1[j]);
                            }
                        }
                    }
                    //map的size最大的为准
                    List<Integer> li = new ArrayList();
                    for(Map m :list){
                        li.add(m.size());
                    }
                    Collections.sort(li);
                    for (int i = 0; i < li.get(li.size()-1); i++) {
                        StringBuilder sb =new StringBuilder();
                        for (Map ma:list){
                            sb.append("|"+ma.get(i));
                        }
                        String te=sb.toString().substring(1).replaceAll("\\|", "\t");
                        context.write(new Text(key.get()),new Text(te));
                    }
                }
            }
        }
    }
}
