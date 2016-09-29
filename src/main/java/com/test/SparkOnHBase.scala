package com.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Put, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableOutputFormat}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext

/**
 * Created by shangyongqiang on 2016/3/17.
 * spark读取和写入HBase
 */
object SparkOnHBase {
  def convertScanToString(scan:Scan)={
    val proto=ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  def main(args: Array[String]) {
    val sc = new SparkContext("local","SparkOnHbase")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort","2181")
    //===========Save RDD to HBase
    //step1: jobConf setup
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")

    //step2:rdd mapping to table
    //定义convert函数做这个转换工作
    def convert(triple:(Int,String,Int))={
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn("basic".getBytes(),"name".getBytes(),triple._2.getBytes)
      p.addColumn("basic".getBytes(),"age".getBytes(),Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable,p)
    }
    //step3: read RDD data from somewhere and convert
    val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
    val localData = sc.parallelize(rawData).map(convert(_))
    //用这个方法保存RDD到hbase中
    localData.saveAsHadoopDataset(jobConf)

    //从hbase中读取数据并转化成spark能直接操作的RDD
    conf.set(TableInputFormat.INPUT_TABLE, "user")
    //添加过滤条件,年龄大于18岁
    val scan = new Scan()
    scan.setFilter(new SingleColumnValueFilter("basic".getBytes(),"age".getBytes(),CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(18)))
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    val userRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val count= userRDD.count()
    //遍历输出
    userRDD.foreach{
      case(_,result)=>
        val key =Bytes.toInt(result.getRow)
        val name = Bytes.toString(result.getValue("basic".getBytes(),"name".getBytes()))
        val age = Bytes.toInt(result.getValue("basic".getBytes(),"age".getBytes()))
        println("ROW key :" + key + "name  :" +name +"age:" + age)
    }
  }
}
