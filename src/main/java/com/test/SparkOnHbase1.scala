package com.test


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan, Put}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{ TableOutputFormat}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/4/5.
 * 用spark读取和hbase的数据过滤行键提取一个用户的信息 rowKey:用户唯一标识_数据来源_url编号_访问时间
 */
object SparkOnHbase1 {

  def convertToString(scan: Scan): String ={
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local","SparkOnHbase")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort","2181")
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")
    def convert(tripe:(Int,String,Int))={
      val p=new Put(Bytes.toBytes(tripe._1))
      p.addColumn("basic".getBytes,"name".getBytes,Bytes.toBytes(tripe._2))
      p.addColumn("badic".getBytes,"age".getBytes,Bytes.toBytes(tripe._3))
      (new ImmutableBytesWritable,p)
    }
    val rawData = List((1,"lilei",2),(1,"hah",3),(3,"meiyou",5))
    val localData = sc.parallelize(rawData).map(convert(_))
    localData.saveAsHadoopDataset(jobConf)

    //从habse中读取数据并转化成spark能操作的RDD
    conf.set(TableInputFormat.INPUT_TABLE,"user")
    //添加过滤条件过滤出同一个用户
    val scan = new Scan()
    scan.setRowPrefixFilter("".getBytes)
    conf.set(TableInputFormat.SCAN,convertToString(scan))
    val result =sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    result.foreach{
      case(_,result)=>
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("name".getBytes,"basic".getBytes))
        val age=Bytes.toInt(result.getValue("basic".getBytes,"age".getBytes))
    }

  }
}
