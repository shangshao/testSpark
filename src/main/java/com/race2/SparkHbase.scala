package com.race2

import java.util

import org.apache.hadoop.hbase.{KeyValue, Cell, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

/**
 * Created by shangyongqiang on 2016/5/18.
 * 经过打包之后可以得出结果可以查出value
 */
object SparkHbase {
  //spark需要设置序列化
  def main(args: Array[String]) {
    //屏蔽不必呀的日志显示在终端上
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if(args.length==0){
      println("程序用法:参数0是表名,参数1是rowkey,参数2是列族,参数3是列名")
    }
//  val conf = new SparkConf().set("spark.serialize","org.apach.spark.serialize.KryoSerializer")
  val hbaseConf = HBaseConfiguration.create()
  val tableName = args(0)
  hbaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
  hbaseConf.set("hbase.master","dmp06:16000")
  hbaseConf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05")
  val table = new HTable(hbaseConf,tableName)
  //根据rowkey来获取行值
    //<value>dmp01:2181,dmp02:2181,dmp03:2181,dmp04:2181,dmp05:2181</value>
  val list = List(new Get(Bytes.toBytes("0000008857C2720593BDEA764BB87434_02_00050012001_2015123017_JS ")))
  val rows = table.get(new Get(Bytes.toBytes(args(1))))
   val kv: Array[KeyValue] = rows.raw()
    println("----------------------------------------------------")
    for(r<-kv){
      print("|")
      printf("%20s",new String(r.getFamily)+":"+new String(r.getQualifier))
      print("|")
      println(new String(r.getValue))
      println("----------------------------------------------------")
    }

//  val name = rows.getValue(Bytes.toBytes(args(2)),Bytes.toBytes(args(3)))
//
//  if(name!=null){
//    val nameString = new String(name)
//    println(nameString)
//  }
}
}