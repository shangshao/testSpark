package com.week2

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}

/**
 * Created by shangyongqiang on 2016/5/25.
 */
object Url_Code {
  //spark需要设置序列化
  //用spark读取hbase的数据需要读取两个表dmp_info与auto_info
  //dmp_info 0000006EEF56B7226842310AFBC09021_02_00010002_2016012809_HB    用户唯一标识_数据来源_url编号_访问时间(yyyyMMddHH)_省份
  //anto_info2  00030005_1006_14665   网站编号_品牌编号(车ID)_具体编号(配置ID)
  def main(args: Array[String]) {
    //屏蔽不必呀的日志显示在终端上
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    //  val conf = new SparkConf().set("spark.serialize","org.apach.spark.serialize.KryoSerializer")
    val hbaseConf = HBaseConfiguration.create()
    val tableName = args(0)
    hbaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    hbaseConf.set("hbase.master","dmp06:16000")
    hbaseConf.set("hbase.zookeeper.quorum","dmp01,dmp02,dmp03,dmp04,dmp05")
    val table = new HTable(hbaseConf,tableName)
    val list = List(new Get(Bytes.toBytes("0000008857C2720593BDEA764BB87434_02_00050012001_2015123017_JS ")))
    val rows = table.get(new Get(Bytes.toBytes(args(1))))
    val name = rows.getValue(Bytes.toBytes(args(2)),Bytes.toBytes(args(3)))

    if(name!=null){
      val nameString = new String(name)
      println(nameString)
    }
  }
}
