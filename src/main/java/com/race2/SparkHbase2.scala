package com.race2

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.log4j.{Level, Logger}

import org.apache.spark._

/**
 * Created by shangyongqiang on 2016/5/19.
 */
object SparkHbase2 {
  def main(args: Array[String]) {
    //屏蔽不必呀的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext("spark://dmp04:7077","HbaseTest",System.getenv("SPARK_HOME"))
    val conf = HBaseConfiguration.create()
    conf.set("hbase.master","dmp06:16000")
    conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05")
    conf.set(TableInputFormat.INPUT_TABLE,"dmp_info")
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    println("读取的数据的条数是>>>>>>>>>>>>>.")
    println( hbaseRDD.count())
    System.exit(0)
  }
}
