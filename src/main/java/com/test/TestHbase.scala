package com.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{TableName, KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner

/**
 * Created by shangyongqiang on 2016/3/26.
 * 连接hbase查询数据
 */
object TestHbase {
  def main(args: Array[String]) {
    val conf= HBaseConfiguration.create();
//    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    conf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05")
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.master","dmp01")
    val conn = ConnectionFactory.createConnection(conf)
    val userTable = TableName.valueOf("ee_telecom_phone")
    val table= conn.getTable(userTable)
    val get = new Get(Bytes.toBytes("0000000C85B8FE92E7B5F0FE24E62AB3"))
    val result:Result = table.get(get)
//    for ( rowKV :KeyValue<- result.raw()) {
//      System.out.print("行名:" + new String(rowKV.getRow()) + " ");
//      System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
//      System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
//      System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
//      System.out.println("值:" + new String(rowKV.getValue()));
//    }
   println( result.getColumnCells("info".getBytes(), "area".getBytes()).get(0).toString)
  }
}
