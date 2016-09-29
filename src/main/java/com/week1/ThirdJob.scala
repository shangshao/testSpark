package com.week1

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by shangyongqiang on 2016/5/24.
 */
object ThirdJob {

  case class Recode(userid:String,words:Seq[String],province:String,source:String,time:String)
  val keywordMap =scala.collection.mutable.Map(1->"name")
  def nonNegativeMod(x:Int,mod:Int): Int ={
    val rawMod = x%mod
    rawMod+(if(rawMod<0)rawMod else 0)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ThirdJob")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val data1 = sc.textFile("hdfs://mycluster/user/hive/warehouse/getdatas.db/s_keys/result").map(x=>{
      val words = x.split("\t")(1).split(",").filter(x=> ! "|".equals(x(0))&&x(0).toString.length>1)
      Recode(x.split("\t")(0),words(0).substring(1).split("|").toSeq,words(1),words(2),words(3))
    }).toDF()
//    val data = data1.map(a=>a._2)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeature")
    data1.select($"words").map{
      case Row(words:Seq[String])=>
      for(v<-words){
        keywordMap+=(nonNegativeMod(v.##,1<<20)->v)
        //        keywordMap.foreach(println)
      }
    }
    val tf = hashingTF.transform(data1).cache()
    val idf = new IDF().setInputCol("rawFeature").setOutputCol("rawFeature1").fit(tf)
    val tfidf: DataFrame = idf.transform(tf)
//    val tfidfOfName = tfidf.partitions
    val mapFromRdd = data1.select($"userid",$"rawFeature1",$"province",$"source",$"time").map{
  case Row(userid:String,words:Vector,province:String,source:String,time:String)=>
    (userid,words.toSparse.indices,words.toSparse.values,province,source,time)
}
//
//    (line=>line.toSparse.indices->line.toSparse.values)
    //    mapFromRdd.foreach(
    //    n=>println(n._1.toList+"\t"+n._2.toList)
    //    )
    val customerMapRdd = mapFromRdd.map(line=>{
      val changeName = (x:Int)=>keywordMap.getOrElse(x,0)
      val KList = line._2.map(changeName)
      val nameMap =KList.toList.zip(line._3.toList)
  (line._1,nameMap,line._4,line._5,line._6)
    }).toJavaRDD()

    println("-------------------------")
    customerMapRdd.saveAsTextFile("/user/shangyongqiang/bisai/iout/1")
    sc.stop()
  }

  }

