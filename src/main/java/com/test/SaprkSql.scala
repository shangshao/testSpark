package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/30.
 */
object SaprkSql {
  case class Record(code:String,id:String,num:String,city:String,other:String)
  def main(args: Array[String]) {
    //屏蔽不必呀的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val records =sc.textFile("hdfs://192.168.3.151:9000/user/shangyongqiang/test/test.txt").map(_.split("\\|")).map(r=>Record(r(0),r(1),r(2),r(3),r(4))).toDF()
    records.registerTempTable("record")
    val aa: DataFrame =sqlContext.sql("select id ,a.nn from (select id,count(code) as nn from record where num='1' group by id) a where a.nn>2")
    println("输出结果:")
    aa.foreach(println)
    sc.stop()
  }
}
