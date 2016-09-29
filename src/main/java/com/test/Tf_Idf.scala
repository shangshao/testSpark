package com.test

import java.io.ByteArrayInputStream
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.util.Progressable
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/21.
 * 怎么使一行文本就是一个文档来处理
 * 实验了把文本分开跟在一起读取输出的结果一样
 */
object Tf_Idf {


  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }


  def main(args: Array[String]) {

    //加入本地测试的hadoop路径,防止报错
//    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
//    val conf1:Configuration = new Configuration();
//      conf1.set("fs.defaultFS", "hdfs://192.168.3.151:9000");
//      val path = "/user/shangyongqiang/tmp0/english.txt"
//      val fileSystem = FileSystem.get(conf1)
//      val input:FSDataInputStream =fileSystem.open(new Path(path))
//      var i = 0
//      while(input.readLine()!=null){
//        var hh ="/user/shangyongqiang/tmp11/"+i+".txt"
//        val out  =fileSystem.create(new Path(hh))
////        val fs = FileSystem.get(URI.create(hh),conf1)
////        val out:FSDataOutputStream = fs.append(new Path(hh))
//        val a = input.readLine()
//        val in =  new ByteArrayInputStream(a.getBytes())
//        IOUtils.copyBytes(in, out, 4096, true);
//        i+=1
//    }
    val conf = new SparkConf().setAppName("tf-idf")
    val sc = new SparkContext(conf)
    val document = sc.textFile("hdfs://mycluster/user/shangyongqiang/tmp0/english.txt").map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf= hashingTF.transform(document).cache()
    document.foreach(w=>{
      for(v <- w){
        print(v)
      }

    })
    //HashingTF方法只需要一次数据交互,而IDF需要两次数据交互:第一次计算IDF向量,第二次需要个词频次相乘
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    //spark.mllib支持忽略频词低于文档最小数,需要minDocFrep 这个数传给IDF架构函数,在此情况下的IDF设置为0
    val idf1 = new IDF(minDocFreq = 2).fit(tf)
    val tfidf1 :RDD[Vector]=idf1.transform(tf)
    tfidf.foreach(v=>{
//      v.
//      println(v.toString)
      println(v.toString)
    }
    )
//    println("one".hashCode)余弦相似性
  }
}

