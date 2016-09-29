package com.test

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._

/**
 * Created by shangyongqiang on 2016/3/16.
 */
object dataFromHbase {
  def main(args: Array[String]) {
//    val sc = new SparkContext(args(0),"dataFromHbase",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
//    val conf =HBaseConfiguration.create()
//    conf.set(TableInputFormat.INPUT_TABLE,args(1))
//    val hBaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
//    hBaseRDD.count()
//    System.exit(0)
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort","2181")
    conf.set("hbase.zookeeper.quorum","master")
    //这个方法是在hbase的1.0.0之后才出现的,Connnection的创建是个重量级的工作,线程安全,是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)
    //创建user表
    val userTable = TableName.valueOf("user")
    val userDescr = new HTableDescriptor(userTable)
    userDescr.addFamily(new HColumnDescriptor("basic".getBytes()))
    println("Creating table 'user'")
    val admin = conn.getAdmin
    if(admin.tableExists(userTable)){
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(userDescr)
    println("done")
    //插入,查询,扫描,删除操作
    val table= conn.getTable(userTable)
    //准备插入一条key为id001的数据
    val p= new Put("id001".getBytes())
    //为put操作指定column和value(以前的put.add的方法弃用了)
    p.addColumn("basic".getBytes(),"name".getBytes(),"wuchong".getBytes())
    //提价
    table.put(p)
    //查询某条数据
    val g = new Get("id001".getBytes())
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("basic".getBytes(),"name".getBytes()))
    println("GET id001:"+value)
    //扫描数据
    val s= new Scan()
    s.addColumn("basic".getBytes(),"name".getBytes())
    val scanner = table.getScanner(s)
    val it=scanner.iterator()
    try{
    while(it.hasNext()){
      val r = it.next()
      println("Found row"+r)
      println("Found value:"+Bytes.toString(r.getValue("basic".getBytes,"name".getBytes)))

    //删除某条数据,操作方式与put类似
    val d = new Delete("id001".getBytes())
    d.addColumn("basic".getBytes(),"name".getBytes())
    table.delete(d)
    }}finally {
      //确保scanner关闭
      scanner.close()
      if(table!=null) table.close()
      conn.close()
    }



  }
}
