package com.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{ TableOutputFormat}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/16.
 *    用saprk操作hbase
 */
object SparkHbase {
  def main(args: Array[String]) {
    def convert(triple:(Int,String,Int)) ={
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
      p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable,p)
    }

    //定义hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.prooerty.clientPort","2181")
    conf.set("hbase.zookeeper.quorum","master")
    //指定输出格式和输出表明
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")
    //读取RDD并转换
    val sconf = new SparkConf().setMaster("local").setAppName("testSparkHbase")
    val sc = new SparkContext(sconf)
    val rawData = List((1,"lilie",14),(2,"hanmei",18),(1,"someone",20))
    val localData=sc.parallelize(rawData).map(convert)
    localData.saveAsHadoopDataset(jobConf)
    val confs = HBaseConfiguration.create()
    confs.set(TableInputFormat.INPUT_TABLE,"user")
    val userRDDs = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val count=userRDDs.count()
    println("User RDD Count:"+count)
    userRDDs.cache()
    //遍历输出
    userRDDs.foreach{
      case (_,result)=>
      val key = Bytes.toInt(result.getRow)
        val name = Bytes.toString(result.getValue("basic".getBytes(),"name".getBytes()))
        val age = Bytes.toInt(result.getValue("basic".getBytes(),"age".getBytes()))
        println("hhhhhhhhhhhhhhh")
    }
  }
}
