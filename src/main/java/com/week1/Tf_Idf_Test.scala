package com.week1

import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/5/24.
 */
object Tf_Idf_Test {
  case class Recode(userid:String,words:Seq[String],province:String,source:String,time:String)
  val keywordMap =scala.collection.mutable.Map(1->"name")
  def nonNegativeMod(x:Int,mod:Int): Int ={
    val rawMod = x%mod
    rawMod+(if(rawMod<0)rawMod else 0)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
//    val data1: RDD[((String, String, String, String), Seq[String])] = sc.textFile("hdfs://mycluster/user/hive/warehouse/getdatas.db/s_keys/result").map(x=> {
//  val words: Array[String] = x.split("\t")(1).split(",")
//      if(!"|".equals(x(0))&&x(0).toString.length>1 &&x.length>4){
//
//      }
//    ((x.split("\t")(0), words(1), words(2), words(3)), words(0).substring(1).split("|").toSeq)
//})

//
    val data: RDD[Seq[String]] = sc.textFile("hdfs://mycluster/user/hive/warehouse/getdatas.db/s_keys/result").map(_.split("\t")(1)).map(_.split(",")(0)).filter(x=>x.length>1&& !x.equals("|")).map(_.substring(1).split("\\|").toSeq)


//    val data = data1.map(a=>a._2)
    val hashingTF = new HashingTF()
    data.foreach(w=>{
      for(v<-w){
        keywordMap+=(nonNegativeMod(v.##,1<<20)->v)
//        keywordMap.foreach(println)
      }
    })
    val tf = hashingTF.transform(data).cache()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
//    val tfidfOfName = tfidf.partitions
    val mapFromRdd = tfidf.map(line=>line.toSparse.indices->line.toSparse.values)
//    mapFromRdd.foreach(
//    n=>println(n._1.toList+"\t"+n._2.toList)
//    )
    val customerMapRdd = mapFromRdd.map(line=>{
      val changeName = (x:Int)=>keywordMap.getOrElse(x,0)
      val KList = line._1.map(changeName)
      val nameMap =KList.toList.zip(line._2.toList)
      nameMap
    })

    println("-------------------------")
    customerMapRdd.saveAsTextFile("/user/shangyongqiang/bisai/iout/1")
    sc.stop()
  }
}
