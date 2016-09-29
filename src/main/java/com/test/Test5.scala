package com.test

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext

/**
 * Created by shangyongqiang on 2016/3/17.
 */
object Test5 {
  def main(args: Array[String]) {
    val sc= new SparkContext("local","SparkHBase")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort","2081")
    conf.set("hbase.zookeeper,quorum","master")
    //Connection的创建是个重量级的工作,线程安全,是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)
    //从Connection获得Admin对象(相当于以前的HAdmin)
    val admin = conn.getAdmin
    //本例将操作的表明
    val userTable = TableName.valueOf("user")
    //创建user表
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("basic".getBytes()))
    println("Create table 'user'")
    if(admin.tableExists(userTable)){
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    println("Done!")
    try{
      //获取user表
      val table = conn.getTable(userTable)
      try{
        //准备一条key为"Id001"的数据
        val p = new Put("Id0001".getBytes())
        //为put操作指定column个value(以前的put.add方法被弃用)
        p.addColumn("basic".getBytes(),"name".getBytes(),"wushaung".getBytes())
        //提交
        table.put(p)

        //查询某条数据
        val g = new Get("ID0001".getBytes())
        val result = table.get(g)
        val value = Bytes.toString(result.getValue("basic".getBytes(),"name".getBytes()))
        println("GET ID001:"+ value)


        //扫描数据
        val s = new Scan()
        s.addColumn("basic".getBytes(),"name".getBytes())
        val scanner = table.getScanner(s)
        val it = scanner.iterator()
        try{
          while(it.hasNext){
            val r = it.next()
            println("Found row :" +r)
            println("Found value :" + Bytes.toString(r.getValue("basic".getBytes(),"name".getBytes())))
          }
        }finally{
          scanner.close()
        }

        //删除某条数据.操作方式与put类似
        val  d = new Delete("id0001".getBytes())
        d.addColumn("basic".getBytes(),"name".getBytes())
        table.delete(d)
      } finally {
        if(table!=null) table.close()
      }
    }finally {
      conn.close()
    }
  }
}
