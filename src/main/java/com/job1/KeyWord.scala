package com.job1

//import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by shangyongqiang on 2016/7/18.
 */
object KeyWord {
    def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }


    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("tf-idf")
      val sc = new SparkContext(conf)
      val document = sc.textFile("hdfs://mycluster/user/shangyongqiang/hb/keyword/result").map(_.split(" ").toSeq)
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
